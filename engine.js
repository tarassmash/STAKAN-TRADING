// Sell Grid — Short: серверный движок (перенос логики страницы sell-grid-shukin.html, работает 24/7).
// Мульти-монеты: каждый цикл подтягивает ВСЕ открытые позиции и ордера с биржи и ведёт их одновременно:
//   защита от спайка, фильтр ATR, TP / SL по средней цене шорта, maker-тейк лимитом, сетка SELL-лимитов выше цены.
// Режим Щукина: шаг сетки от 1%, тейк от 0.7%, аварийный SL 10%, добор выше при открытом шорте, фильтр OI/CVD.
"use strict";

const DEFAULTS = {
  symbol: "BTCUSDT", interval: "5", category: "linear",
  gridStepPct: 0.25, gridLevels: 4, tpPct: 0.30, slPct: 1.0, orderUsdt: 20, maxDailyLoss: 15, intervalSec: 3,
  liveEnabled: false, makerExit: true, superSpikeProtect: true, maxSpikePct: 0.40, volFilter: true, minAtrPct: 0.15, maxAtrPct: 4,
  shukinMode: false, running: false,
  autopilot: true, autoScanMin: 5, autoScanMode: "pump",
  strategy: "sell",        // "sell" = Sell Grid (short from a pump) | "follow" = autopilot after the price (long or short where it goes)
  followBand: 0.08,
  bandPct: 2,              // autopilot after the price: SELL limit this % above, BUY limit this % below (they follow the price behind it)
  bandTp: 1.0,             // take (limit, maker) from the average entry, %
  bandSl: 3.0,             // stop on the exchange from the average entry, %
  bandAdds: 1,             // extra adding limits in the position's direction, each bandPct further
  bandRef: 5,              // the limits follow the average of this many last candles (a spike fills them instead of pushing them away)   // 24/7: re-scan and move the grid to the best target by itself
};
const SHUKIN = { gridStepPct: 1.00, gridLevels: 5, tpPct: 0.70, slPct: 10, orderUsdt: 12, intervalSec: 3, maxSpikePct: 0.50, minAtrPct: 0.20, maxAtrPct: 5, makerExit: true, superSpikeProtect: true, volFilter: true };
const NUMS = ["bandPct", "bandTp", "bandSl", "bandAdds", "bandRef", "followBand", "autoScanMin", "gridStepPct", "gridLevels", "tpPct", "slPct", "orderUsdt", "maxDailyLoss", "intervalSec", "maxSpikePct", "minAtrPct", "maxAtrPct"];
const BOOLS = ["autopilot", "liveEnabled", "makerExit", "superSpikeProtect", "volFilter"];

function create(host) {
  const { bx, lsGet, lsSet, keys } = host;
  const P = Object.assign({}, DEFAULTS, lsGet("sg.p", {}));
  const save = () => lsSet("sg.p", P);
  const S = Object.assign({ lastAuto: 0, cycles: 0, trades: 0, wins: 0, losses: 0, dayPnl: 0, dayKey: "", targets: [], history: [] }, lsGet("sg.s", {}));
  const saveS = () => lsSet("sg.s", { lastAuto: S.lastAuto, cycles: S.cycles, trades: S.trades, wins: S.wins, losses: S.losses, dayPnl: S.dayPnl, dayKey: S.dayKey, targets: S.targets, history: S.history.slice(-300) });
  const W = {};                               // watches: symbol -> state
  const LOG = lsGet("sg.log", []);
  let busy = false, timer = null, status = "ВЫКЛ", conn = { ok: null, text: "—" }, shukinSig = null, lastSave = 0;
  const now = () => Date.now();

  function log(msg, type) { LOG.unshift([now(), String(msg), type || "info"]); LOG.length = Math.min(LOG.length, 400); if (now() - lastSave > 2000) { lastSave = now(); lsSet("sg.log", LOG); } host.log && host.log(msg); }
  const fmt = (n) => { if (n == null || isNaN(n)) return "—"; const a = Math.abs(n); return a >= 1000 ? n.toFixed(2) : a >= 1 ? n.toFixed(4) : a >= 0.01 ? n.toFixed(6) : n.toFixed(8); };
  function roundToStep(v, step) { const st = String(step); const d = st.includes(".") ? st.split(".")[1].replace(/0+$/, "").length : 0; const s = parseFloat(step) || 0.01; return +(Math.round(v / s) * s).toFixed(Math.max(d, 0)); }
  function floorToStep(v, step) { const st = String(step); const d = st.includes(".") ? st.split(".")[1].replace(/0+$/, "").length : 0; const s = parseFloat(step) || 0.001; return +(Math.floor(v / s + 1e-9) * s).toFixed(Math.max(d, 0)); }

  function ensureDayKey() {
    const k = new Date().toISOString().slice(0, 10);
    if (S.dayKey !== k) { S.dayKey = k; S.dayPnl = 0; S.trades = 0; S.wins = 0; S.losses = 0; saveS(); }
  }
  function recordTrade(pnl, sym, how) {
    ensureDayKey();
    S.trades++; S.dayPnl += pnl; if (pnl >= 0) S.wins++; else S.losses++;
    S.history.push({ at: now(), sym, pnl: +pnl.toFixed(5), how });
    saveS();
    log(`Сделка ${sym || ""} ${how || ""} PnL ${pnl >= 0 ? "+" : ""}${pnl.toFixed(3)}`, pnl >= 0 ? "ok" : "err");
  }

  // ---------- Bybit ----------
  const pub = (path, q) => bx(path, { query: q });
  const priv = (method, path, params) => method === "GET" ? bx(path, { query: params, auth: true }) : bx(path, { method: "POST", body: params, auth: true });
  const INST = {};
  async function getInstrument(symbol, category) {
    if (INST[symbol]) return INST[symbol];
    const r = await pub("/v5/market/instruments-info", { category, symbol });
    const info = (r.list || [])[0]; if (!info) throw new Error("Инструмент не найден");
    return (INST[symbol] = { tickSize: info.priceFilter?.tickSize || "0.01", qtyStep: info.lotSizeFilter?.qtyStep || "0.001", minQty: info.lotSizeFilter?.minOrderQty || "0.001", minNotional: Number(info.lotSizeFilter?.minNotionalValue || 5) });
  }
  async function getOrderBook(symbol, category) {
    const r = await pub("/v5/market/orderbook", { category, symbol, limit: 5 });
    return { bids: (r.b || []).map(([p]) => parseFloat(p)), asks: (r.a || []).map(([p]) => parseFloat(p)) };
  }
  async function getKlines(symbol, category, interval, limit) {
    const r = await pub("/v5/market/kline", { category, symbol, interval, limit: limit || 200 });
    return (r.list || []).map(x => ({ time: Math.floor(parseInt(x[0], 10) / 1000), open: +x[1], high: +x[2], low: +x[3], close: +x[4], volume: +(x[5] || 0) })).reverse();
  }
  const mapOiInterval = (tf) => ({ 1: "5min", 3: "5min", 5: "5min", 15: "15min", 30: "30min", 60: "1h", 240: "4h", D: "1d" }[String(tf)] || "5min");
  async function getOpenInterest(symbol, category, interval, limit) {
    const r = await pub("/v5/market/open-interest", { category, symbol, intervalTime: mapOiInterval(interval), limit: limit || 200 });
    return (r.list || []).map(x => ({ time: Math.floor(parseInt(x.timestamp, 10) / 1000), value: parseFloat(x.openInterest || 0) })).reverse();
  }
  async function getPosition(symbol, category) {
    const r = await priv("GET", "/v5/position/list", { category, symbol });
    const row = (r.list || []).find(p => parseFloat(p.size) > 0);
    if (!row) return { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0, positionIdx: 0 };
    return { side: row.side, size: parseFloat(row.size), avgPrice: parseFloat(row.avgPrice), unrealisedPnl: parseFloat(row.unrealisedPnl || 0), positionIdx: Number(row.positionIdx || 0) };
  }
  const getOpenOrders = async (symbol, category) => (await priv("GET", "/v5/order/realtime", { category, symbol, limit: 50 })).list || [];
  const getAllPositions = async (category) => ((await priv("GET", "/v5/position/list", { category, settleCoin: "USDT", limit: 200 })).list || []).filter(p => Math.abs(parseFloat(p.size || 0)) > 0);
  const getAllOpenOrders = async (category) => (await priv("GET", "/v5/order/realtime", { category, settleCoin: "USDT", limit: 50 })).list || [];
  // one-way (0) or hedge (short = 2)
  const HEDGE = {};
  async function shortIdx(symbol, category) {
    if (HEDGE[symbol] == null) { try { const r = await priv("GET", "/v5/position/list", { category, symbol }); HEDGE[symbol] = (r.list || []).some(x => Number(x.positionIdx) > 0); } catch (e) { HEDGE[symbol] = false; } }
    return HEDGE[symbol] ? 2 : 0;
  }

  // ---------- orders ----------
  async function cancelOurOrders(symbol, category) {
    try {
      const ours = (await getOpenOrders(symbol, category)).filter(o => (o.orderLinkId || "").startsWith("sg_"));
      for (const o of ours) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } }
      if (ours.length) log(`${symbol}: снято ордеров ${ours.length}`, "info");
      return ours.length;
    } catch (e) { return 0; }
  }
  async function closeMarket(symbol, category) {
    const pos = await getPosition(symbol, category);
    if (!pos.size) return;
    await priv("POST", "/v5/order/create", { category, symbol, side: pos.side === "Buy" ? "Sell" : "Buy", orderType: "Market", qty: String(pos.size), reduceOnly: true, positionIdx: pos.positionIdx, orderLinkId: "sg_close_" + now() });
    log(`${symbol}: закрыта market`, "ok");
  }
  async function placeMakerExit(symbol, category, pos, tpPrice, inst) {
    if (!pos.size) return;
    const price = roundToStep(tpPrice, inst.tickSize);
    const body = { category, symbol, side: pos.side === "Buy" ? "Sell" : "Buy", orderType: "Limit", qty: String(pos.size), price: String(price), timeInForce: "PostOnly", reduceOnly: true, positionIdx: pos.positionIdx, orderLinkId: "sg_tp_" + now() };
    try { await priv("POST", "/v5/order/create", body); log(`${symbol}: Maker TP @ ${price}`, "ok"); }
    catch (e) { body.timeInForce = "GTC"; body.orderLinkId = "sg_tp_" + now() + "_g"; try { await priv("POST", "/v5/order/create", body); log(`${symbol}: TP @ ${price} (GTC)`, "ok"); } catch (e2) { log(`${symbol}: Maker TP fail: ${e2.message}`, "err"); } }
  }
  async function placeSellLimit(symbol, category, qty, price, tp, sl, inst) {
    const limitPrice = roundToStep(price, inst.tickSize);
    const body = { category, symbol, side: "Sell", orderType: "Limit", qty: String(qty), price: String(limitPrice), timeInForce: "GTC", positionIdx: await shortIdx(symbol, category), orderLinkId: "sg_s_" + now() + "_" + Math.floor(Math.random() * 999) };
    if (sl) { body.stopLoss = String(sl); body.slTriggerBy = "LastPrice"; }
    if (tp && !P.makerExit) { body.takeProfit = String(tp); body.tpTriggerBy = "LastPrice"; }
    log(`${symbol}: → Sell limit ${qty} @ ${limitPrice}`, "info");
    try { const r = await priv("POST", "/v5/order/create", body); log(`${symbol}: ✅ Sell #${r.orderId}`, "ok"); }
    catch (e) { log(`${symbol}: ❌ Sell: ${e.message}`, "err"); }
  }

  // ---------- indicators ----------
  function estimateAtrPct(c, period) {
    if (!c || c.length < period + 1) return null; let sum = 0;
    for (let i = c.length - period; i < c.length; i++) { const x = c[i], p = c[i - 1]; sum += Math.max(x.high - x.low, Math.abs(x.high - p.close), Math.abs(x.low - p.close)); }
    const last = c[c.length - 1].close; return last ? sum / period / last : null;
  }
  function calculateRSI(c, period) {
    if (!c || c.length < period + 1) return null; let g = 0, l = 0;
    for (let i = c.length - period; i < c.length; i++) { const d = c[i].close - c[i - 1].close; if (d >= 0) g += d; else l -= d; }
    if (l === 0) return 100; return 100 - 100 / (1 + (g / period) / (l / period));
  }
  function buildCvd(c) { let cum = 0; return c.map(x => { cum += x.close >= x.open ? (x.volume || 0) : -(x.volume || 0); return { time: x.time, value: cum }; }); }
  // the Shukin filter (exactly as on the page): OI + CVD -> may we short
  function analyzeShukinContext(candles, oiData) {
    const res = { allowShort: true, reason: "OK", oiTrend: "—", cvdTrend: "—", oiChangePct: 0, cvdSlope: 0 };
    if (!candles || candles.length < 10) return Object.assign(res, { allowShort: false, reason: "мало данных" });
    const cvd = buildCvd(candles);
    let oiTrend = "n/a", oiChangePct = 0;
    if (oiData && oiData.length >= 6) {
      const avg = (a) => a.reduce((s, x) => s + x.value, 0) / (a.length || 1), aR = avg(oiData.slice(-4)), aP = avg(oiData.slice(-8, -4));
      oiChangePct = aP > 0 ? (aR - aP) / aP * 100 : 0; oiTrend = oiChangePct > 1.5 ? "растёт" : oiChangePct < -1.0 ? "падает" : "флэт";
    }
    res.oiTrend = oiTrend; res.oiChangePct = oiChangePct;
    const tail = cvd.slice(-8); let cvdSlope = 0;
    if (tail.length >= 4) { const mid = Math.floor(tail.length / 2), av = (a) => a.reduce((s, x) => s + x.value, 0) / (a.length || 1), a1 = av(tail.slice(0, mid)), a2 = av(tail.slice(mid)); cvdSlope = (a2 - a1) / (Math.abs(a2) > 1e-9 ? Math.abs(a2) : 1); }
    res.cvdSlope = cvdSlope; res.cvdTrend = cvdSlope > 0.03 ? "покупки" : cvdSlope < -0.03 ? "продажи" : "нейтр.";
    const rsi = calculateRSI(candles, 14), last = candles[candles.length - 1], ago = candles[Math.max(0, candles.length - 12)], pumpPct = ago && ago.close ? (last.close - ago.close) / ago.close * 100 : 0;
    if (oiTrend === "растёт" && oiChangePct > 2.5) Object.assign(res, { allowShort: false, reason: "OI растёт — рано шортить" });
    else if (cvdSlope > 0.08 && oiTrend === "растёт") Object.assign(res, { allowShort: false, reason: "OI↑ + CVD покупки — сила лонга" });
    else if (cvdSlope > 0.12) Object.assign(res, { allowShort: false, reason: "CVD сильные покупки" });
    else if (oiTrend === "падает" || cvdSlope < -0.02) Object.assign(res, { allowShort: true, reason: oiTrend === "падает" ? "OI падает — деньги уходят" : "CVD продажи" });
    else if (pumpPct > 3 && (oiTrend === "флэт" || oiTrend === "n/a") && cvdSlope <= 0.05) Object.assign(res, { allowShort: true, reason: "памп выдыхается" });
    else if (rsi != null && rsi >= 70 && cvdSlope <= 0.04) Object.assign(res, { allowShort: true, reason: "RSI перекуп + CVD не тянет" });
    else Object.assign(res, { allowShort: true, reason: "нейтральный контекст" });
    return res;
  }

  // ---------- SELL GRID CYCLE (one symbol) ----------
  async function runSellGridCycle(symbol, mid, inst, volOk, isSpike, candles, w) {
    const category = P.category, shukin = P.shukinMode, levels = Math.round(P.gridLevels) || 4;
    let gridStepPctVal = P.gridStepPct || 0.25, tpPctVal = P.tpPct || 0.3, slPctVal = P.slPct || 1.0;
    if (shukin) { if (gridStepPctVal < 1) gridStepPctVal = 1; if (tpPctVal < 0.7) tpPctVal = 0.7; if (slPctVal < 10) slPctVal = 10; }
    const gridStep = gridStepPctVal / 100, tpPct = tpPctVal / 100, slPct = slPctVal / 100, usdt = P.orderUsdt || 20;
    let qty = floorToStep(usdt / mid, inst.qtyStep);
    if (qty < parseFloat(inst.minQty)) qty = parseFloat(inst.minQty);
    if (qty * mid < inst.minNotional) qty = roundToStep(Math.ceil(inst.minNotional * 1.02 / mid / parseFloat(inst.qtyStep)) * parseFloat(inst.qtyStep), inst.qtyStep);
    const live = P.liveEnabled;
    let pos = { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0, positionIdx: 0 };
    if (live) pos = await getPosition(symbol, category);
    // the short was closed on the exchange (maker TP filled or SL hit) -> record it
    if (live && w.pos && w.pos.size > 0 && w.pos.side === "Sell" && !pos.size && !w.closingByBot) {
      try { const cp = ((await priv("GET", "/v5/position/closed-pnl", { category, symbol, limit: 5 })).list || []).filter(x => Number(x.updatedTime || x.createdTime) > (w.posSince || 0) - 60000);
        const pnl = cp.reduce((a, x) => a + Number(x.closedPnl || 0), 0); recordTrade(pnl, symbol, pnl >= 0 ? "TP (биржа)" : "SL (биржа)"); } catch (e) { /* skip */ }
      await cancelOurOrders(symbol, category);
    }
    w.closingByBot = false;
    if (pos.size && !(w.pos && w.pos.size)) w.posSince = now();
    w.pos = pos;
    const targets = []; for (let i = 1; i <= levels; i++) targets.push(roundToStep(mid * (1 + gridStep * i), inst.tickSize));
    w.targets = targets;
    if (!live) { log(`[SIM${shukin ? " Щукин" : ""}] ${symbol}: Sell grid ${levels} ур. @ ${fmt(mid)}`, "info"); return; }

    if (pos.size > 0 && pos.side === "Sell") {
      const entry = pos.avgPrice || mid, movePct = (entry - mid) / entry;
      if (movePct >= tpPct) { const pnl = pos.unrealisedPnl || 0; log(`${symbol}: 🎯 TP ${(movePct * 100).toFixed(2)}%${shukin ? " (Щукин short take)" : ""}`, "ok"); w.closingByBot = true; await closeMarket(symbol, category); await cancelOurOrders(symbol, category); recordTrade(pnl, symbol, "TP"); w.pos = { size: 0 }; return; }
      if (movePct <= -slPct) { const pnl = pos.unrealisedPnl || 0; log(`${symbol}: 🚨 SL ${(movePct * 100).toFixed(2)}%`, "err"); w.closingByBot = true; await closeMarket(symbol, category); await cancelOurOrders(symbol, category); recordTrade(pnl, symbol, "SL"); w.pos = { size: 0 }; return; }
      if (P.makerExit) { const open = await getOpenOrders(symbol, category); if (!open.some(o => (o.orderLinkId || "").startsWith("sg_tp_"))) await placeMakerExit(symbol, category, pos, entry * (1 - tpPct), inst); }
      if (!shukin) return;   // normal mode: no new entries while in a position; Shukin: keep adding above (averaging)
    }
    // a new grid only on the current target; another coin is only led while it holds a short (its stray grid orders are removed)
    if (symbol !== (P.symbol || "").toUpperCase() && !(pos.size > 0)) {
      if (w.ourOrders > 0) { await cancelOurOrders(symbol, category); w.ourOrders = 0; }
      w.status = "свободна"; return;
    }
    if (isSpike || !volOk) { w.status = isSpike ? "SPIKE PAUSE" : "VOL FILTER"; return; }
    if (shukin) {
      let oi = w.oi || [];
      if (S.cycles % 5 === 0 || !oi.length) { try { const o = await getOpenInterest(symbol, category, P.interval, 50); if (o.length) { oi = o; w.oi = o; } } catch (e) { /* keep */ } }
      const sig = analyzeShukinContext(candles, oi); w.sig = sig; if (symbol === P.symbol) shukinSig = sig;
      if (!sig.allowShort) { w.status = "ЩУКИН ЖДАТЬ"; return; }
    }
    // sync the grid: cancel the extra, fill the holes
    const ourSells = (await getOpenOrders(symbol, category)).filter(o => (o.orderLinkId || "").startsWith("sg_s_") && o.side === "Sell");
    const tol = mid * gridStep * 0.35, matched = new Set(), toCancel = [];
    for (const o of ourSells) { const px = parseFloat(o.price); if (!px || isNaN(px)) { toCancel.push(o); continue; } let found = null; for (const t of targets) if (Math.abs(px - t) <= tol) { found = t; break; } if (found != null && !matched.has(found)) matched.add(found); else toCancel.push(o); }
    if (toCancel.length && ourSells.length) log(`${symbol}: ↕ трейлинг сетки за ценой ${fmt(mid)} — переставляю ${toCancel.length} ур.`, "info");
    for (const o of toCancel) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } }
    for (const px of targets) {
      if (matched.has(px)) continue;
      const tp = roundToStep(px * (1 - tpPct), inst.tickSize), sl = (!shukin || pos.size === 0) ? roundToStep(px * (1 + slPct), inst.tickSize) : null;
      await placeSellLimit(symbol, category, qty, px, tp, sl, inst);
    }
  }

  // ---------- AUTOPILOT AFTER THE PRICE: a grid for adding in the direction the price goes, takes and a stop ----------
  const emaArr = (a, n) => { const k = 2 / (n + 1); let e = a[0]; return a.map(x => (e = x * k + e * (1 - k))); };
  function followDir(c) {
    if (!c || c.length < 30) return { d: 0, why: "мало свечей" };
    const cl = c.map(x => x.close), f = emaArr(cl, 9), sl = emaArr(cl, 21), n = cl.length - 1, diff = (f[n] / sl[n] - 1) * 100, mv = (cl[n] / cl[Math.max(0, n - 5)] - 1) * 100;
    const d = diff > P.followBand && mv > 0 ? 1 : diff < -P.followBand && mv < 0 ? -1 : 0;
    return { d, diff: +diff.toFixed(3), mv: +mv.toFixed(2), why: d > 0 ? `цена идёт вверх (EMA9 выше EMA21 на ${diff.toFixed(2)}%, за 5 свечей ${mv >= 0 ? "+" : ""}${mv.toFixed(2)}%)` : d < 0 ? `цена идёт вниз (EMA9 ниже EMA21 на ${Math.abs(diff).toFixed(2)}%, за 5 свечей ${mv.toFixed(2)}%)` : `направления нет (EMA ${diff.toFixed(2)}%, 5 свечей ${mv.toFixed(2)}%)` };
  }
  // limits only: flat -> SELL bandPct above + BUY bandPct below the price (following it behind);
  // a fill -> take by a limit from the average entry, the opposite limit removed, adds further on, the stop on the exchange
  async function runFollowCycle(symbol, mid, inst, volOk, isSpike, candles, w) {
    const category = P.category, off = (P.bandPct || 2) / 100, tp = (P.bandTp || 1) / 100, sl = (P.bandSl || 3) / 100, usdt = P.orderUsdt || 20;
    let qty = floorToStep(usdt / mid, inst.qtyStep);
    if (qty < parseFloat(inst.minQty)) qty = parseFloat(inst.minQty);
    if (qty * mid < inst.minNotional) qty = roundToStep(Math.ceil(inst.minNotional * 1.02 / mid / parseFloat(inst.qtyStep)) * parseFloat(inst.qtyStep), inst.qtyStep);
    const n = Math.max(1, Math.round(P.bandRef) || 5), last = (candles || []).slice(-n), ref = last.length ? last.reduce((a, c) => a + c.close, 0) / last.length : mid;
    const live = P.liveEnabled;
    let pos = { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0, positionIdx: 0 };
    if (live) pos = await getPosition(symbol, category);
    if (live && w.pos && w.pos.size > 0 && !pos.size) {   // closed on the exchange (take or stop)
      try { const cp = ((await priv("GET", "/v5/position/closed-pnl", { category, symbol, limit: 5 })).list || []).filter(x => Number(x.updatedTime || x.createdTime) > (w.posSince || 0) - 60000);
        const pnl = cp.reduce((a, x) => a + Number(x.closedPnl || 0), 0); recordTrade(pnl, symbol, pnl >= 0 ? "тейк лимитом" : "стоп"); } catch (e) { /* skip */ }
      await cancelOurOrders(symbol, category);
    }
    if (pos.size && !(w.pos && w.pos.size)) w.posSince = now();
    w.pos = pos;
    const pd = pos.size ? (pos.side === "Buy" ? 1 : -1) : 0;
    await shortIdx(symbol, category);
    const idxOf = (d) => (HEDGE[symbol] ? (d > 0 ? 1 : 2) : 0);
    // the wanted orders
    let want = [];
    if (!pd) want = [{ side: "Sell", d: -1, px: roundToStep(ref * (1 + off), inst.tickSize) }, { side: "Buy", d: 1, px: roundToStep(ref * (1 - off), inst.tickSize) }];
    else for (let k = 1; k <= Math.max(0, Math.round(P.bandAdds)); k++) want.push({ side: pd > 0 ? "Buy" : "Sell", d: pd, px: roundToStep(pos.avgPrice * (1 - pd * off * k), inst.tickSize), add: true });
    w.levels = want.map(x => ({ px: x.px, side: x.d })); w.targets = want.map(x => x.px); w.side = pd || 0;
    w.dir = { d: pd, why: pd ? `${pd > 0 ? "лонг" : "шорт"} открыт — тейк лимитом ${(P.bandTp)}% от средней` : `жду: SELL +${P.bandPct}% / BUY −${P.bandPct}% от цены (средняя ${n} свечей ${fmt(ref)})` };
    if (!live) { log(`[SIM АВТОПИЛОТ] ${symbol}: ${want.map(x => `${x.side} @ ${x.px}`).join(" · ")}${pd ? "" : ` (цена ${fmt(mid)})`}`, "info"); return; }
    if (symbol !== (P.symbol || "").toUpperCase() && !pd) { if (w.ourOrders > 0) { await cancelOurOrders(symbol, category); w.ourOrders = 0; } w.status = "свободна"; return; }
    const open = await getOpenOrders(symbol, category), ours = open.filter(o => (o.orderLinkId || "").startsWith("sg_b_")), tps = open.filter(o => (o.orderLinkId || "").startsWith("sg_tp_"));
    if (pd) {
      // the take: one reduce-only limit for the whole position (re-placed when the size or the average changed)
      const tpPx = roundToStep(pos.avgPrice * (1 + pd * tp), inst.tickSize);
      const okTp = tps.find(o => Math.abs(parseFloat(o.price) - tpPx) <= parseFloat(inst.tickSize) * 2 && Math.abs(parseFloat(o.qty) - pos.size) < 1e-9);
      if (!okTp) { for (const o of tps) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } } await placeMakerExit(symbol, category, pos, tpPx, inst); }
      // the stop of the whole position on the exchange (from the average)
      const slPx = roundToStep(pos.avgPrice * (1 - pd * sl), inst.tickSize);
      if (Math.abs((w.slSet || 0) - slPx) > parseFloat(inst.tickSize) / 2) {
        try { await priv("POST", "/v5/position/trading-stop", { category, symbol, tpslMode: "Full", stopLoss: String(slPx), slTriggerBy: "LastPrice", positionIdx: pos.positionIdx }); w.slSet = slPx; log(`${symbol}: стоп позиции ${slPx} (−${P.bandSl}% от средней)`, "info"); }
        catch (e) { if (String(e.code) === "34040") w.slSet = slPx; else log(`${symbol}: стоп: ${e.message}`, "err"); }
      }
    } else w.slSet = 0;
    if (isSpike && !pd) { w.status = "SPIKE — лимиты держу"; }
    // sync the entry limits: the opposite side goes, the others follow the price behind it
    const tol = ref * off * 0.15, keep = new Set();
    for (const o of ours) {
      const m = want.find((x, i2) => !keep.has(i2) && x.side === o.side && Math.abs(parseFloat(o.price) - x.px) <= tol);
      if (m) { keep.add(want.indexOf(m)); continue; }
      const same = want.findIndex((x, i2) => !keep.has(i2) && x.side === o.side);
      if (same >= 0 && !isSpike) {   // move it after the price (amend keeps the order)
        try { await priv("POST", "/v5/order/amend", { category, symbol, orderId: o.orderId, price: String(want[same].px) }); keep.add(same); log(`${symbol}: ↕ ${o.side} ${o.price} → ${want[same].px}`, "info"); continue; } catch (e) { /* re-place below */ }
      }
      if (same >= 0 && isSpike) { keep.add(same); continue; }   // during a spike the limits stay where they are (that is when they fill)
      try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ }
    }
    for (let k = 0; k < want.length; k++) {
      if (keep.has(k)) continue;
      const x = want[k], stopPx = roundToStep(x.px * (1 - x.d * sl), inst.tickSize);
      const body = { category, symbol, side: x.side, orderType: "Limit", qty: String(qty), price: String(x.px), timeInForce: "PostOnly", positionIdx: idxOf(x.d), orderLinkId: "sg_b_" + now() + "_" + Math.floor(Math.random() * 999) };
      if (!pd) { body.stopLoss = String(stopPx); body.slTriggerBy = "LastPrice"; }
      try { await priv("POST", "/v5/order/create", body); log(`${symbol}: ${x.add ? "добор " : ""}${x.side} limit ${qty} @ ${x.px}`, "info"); }
      catch (e) { log(`${symbol}: ❌ ${x.side} limit: ${e.message}`, "err"); }
    }
    w.status = pd ? (pd > 0 ? "ЛОНГ · тейк лимитом" : "ШОРТ · тейк лимитом") : (isSpike ? "SPIKE — лимиты держу" : `±${P.bandPct}% лимитки`);
  }

  function ensureWatch(symbol) { symbol = symbol.toUpperCase().trim(); if (!W[symbol]) W[symbol] = { symbol, mid: 0, candles: [], priceHistory: [], pos: { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0 }, orders: 0, ourOrders: 0, isSpike: false, volOk: true, status: "—", atrPct: null, rsi: null }; return W[symbol]; }

  async function processSymbol(symbol) {
    const category = P.category, w = ensureWatch(symbol);
    const book = await getOrderBook(symbol, category);
    if (!book.bids.length || !book.asks.length) throw new Error(symbol + ": пустой стакан");
    const mid = (book.bids[0] + book.asks[0]) / 2; w.mid = mid;
    const inst = await getInstrument(symbol, category);
    const maxSpike = (P.maxSpikePct || 0.4) / 100;
    w.priceHistory.push(mid); if (w.priceHistory.length > 5) w.priceHistory.shift();
    let isSpike = false; if (P.superSpikeProtect && w.priceHistory.length >= 3) { const old = w.priceHistory[0]; if (Math.abs(mid - old) / old >= maxSpike) isSpike = true; }
    w.isSpike = isSpike;
    if (!w.candles.length || S.cycles % 3 === 0) { try { w.candles = await getKlines(symbol, category, P.interval, 60); } catch (e) { /* keep */ } }
    const candles = w.candles;
    w.atrPct = estimateAtrPct(candles, 14); w.rsi = calculateRSI(candles, 14);
    let volOk = true; if (P.volFilter && w.atrPct != null) { const mn = (P.minAtrPct || 0.15) / 100, mx = (P.maxAtrPct || 4) / 100; if (w.atrPct < mn || w.atrPct > mx) volOk = false; }
    w.volOk = volOk; w.status = isSpike ? "SPIKE" : !volOk ? "VOL" : "OK";
    if (P.liveEnabled) { try { const open = await getOpenOrders(symbol, category); w.orders = open.length; w.ourOrders = open.filter(o => (o.orderLinkId || "").startsWith("sg_")).length; } catch (e) { /* keep */ } }
    if (P.strategy === "follow") await runFollowCycle(symbol, mid, inst, volOk, isSpike, candles, w);
    else await runSellGridCycle(symbol, mid, inst, volOk, isSpike, candles, w);
  }

  // every cycle: ALL open positions + orders from the exchange are under control
  async function refreshOpenUniverse() {
    const category = P.category, active = (P.symbol || "").toUpperCase().trim();
    if (!P.liveEnabled) { if (active) ensureWatch(active); for (const s of Object.keys(W)) if (s !== active) delete W[s]; return Object.keys(W); }
    try {
      const [positions, orders] = await Promise.all([getAllPositions(category), getAllOpenOrders(category)]);
      const alive = new Set(); if (active) alive.add(active);
      for (const p of positions) { const s = (p.symbol || "").toUpperCase(); if (!s) continue; alive.add(s); const w = ensureWatch(s); w.posEx = { side: p.side, size: parseFloat(p.size), avgPrice: parseFloat(p.avgPrice), unrealisedPnl: parseFloat(p.unrealisedPnl || 0) }; }
      const oc = {}, ourc = {};
      for (const o of orders) { const s = (o.symbol || "").toUpperCase(); if (!s) continue; alive.add(s); oc[s] = (oc[s] || 0) + 1; if ((o.orderLinkId || "").startsWith("sg_")) ourc[s] = (ourc[s] || 0) + 1; }
      for (const s of alive) { const w = ensureWatch(s); w.orders = oc[s] || 0; w.ourOrders = ourc[s] || 0; }
      for (const s of Object.keys(W)) if (!alive.has(s)) delete W[s];
      return [...alive];
    } catch (e) { log("refresh universe: " + e.message, "err"); if (active) ensureWatch(active); return Object.keys(W); }
  }

  // ---------- MAIN LOOP ----------
  async function runCycle() {
    if (busy) return; busy = true;
    try {
      const symbol = (P.symbol || "").toUpperCase().trim();
      status = "цикл…"; ensureDayKey();
      const maxLoss = P.maxDailyLoss || 0;
      if (maxLoss > 0 && S.dayPnl <= -Math.abs(maxLoss)) {
        status = "DAILY LIMIT";
        if (P.liveEnabled) { for (const s of Object.keys(W)) { try { await cancelOurOrders(s, P.category); } catch (e) { /* skip */ } } try { await cancelOurOrders(symbol, P.category); } catch (e) { /* skip */ } }
        S.cycles++; return;
      }
      const list = await refreshOpenUniverse();
      if (!list.length && symbol) { ensureWatch(symbol); list.push(symbol); }
      const results = await Promise.allSettled(list.map(s => processSymbol(s)));
      results.forEach((r, i) => { if (r.status === "rejected") log(list[i] + ": " + (r.reason && r.reason.message || r.reason), "err"); });
      S.cycles++;
      if (P.running) { const posN = list.filter(s => W[s] && W[s].pos && W[s].pos.size > 0).length, spikes = list.filter(s => W[s] && W[s].isSpike).length;
        status = (P.strategy === "follow" ? "RUN АВТОПИЛОТ" : P.shukinMode ? "RUN ЩУКИН" : "RUN GRID") + " · " + list.length + " пар" + (posN ? " · POS×" + posN : "") + (spikes ? " · SPIKE×" + spikes : ""); }
      else status = "ВЫКЛ";
      conn = { ok: true, text: "OK" };
      if (S.cycles % 20 === 0) saveS();
    } catch (e) { log(e.message || String(e), "err"); conn = { ok: false, text: "error" }; status = "ERROR"; }
    finally { busy = false; }
  }
  function schedule() { if (timer) clearInterval(timer); timer = null; if (P.running) { const sec = Math.max(2, Math.round(P.intervalSec) || 3); timer = setInterval(runCycle, sec * 1000); } }

  // ---------- scanners ----------
  async function scanPump() {
    const category = P.category, interval = P.interval, r = await pub("/v5/market/tickers", { category });
    let pumps = (r.list || []).filter(t => t.symbol.endsWith("USDT") && parseFloat(t.turnover24h || 0) > 2e6 && parseFloat(t.price24hPcnt) >= 0.012);
    if (!pumps.length) pumps = (r.list || []).filter(t => t.symbol.endsWith("USDT") && parseFloat(t.turnover24h || 0) > 1e6).sort((a, b) => parseFloat(b.price24hPcnt) - parseFloat(a.price24hPcnt)).slice(0, 20);
    else pumps.sort((a, b) => parseFloat(b.price24hPcnt) - parseFloat(a.price24hPcnt));
    const found = [];
    for (const coin of pumps.slice(0, 12)) {
      try { const kl = await getKlines(coin.symbol, category, interval, 30), rsi = calculateRSI(kl, 14), chg = parseFloat(coin.price24hPcnt || 0) * 100;
        if (rsi == null || rsi >= 58 || chg >= 3) found.push({ symbol: coin.symbol, chg, rsi, turnover: parseFloat(coin.turnover24h || 0), scannedAt: now() }); } catch (e) { /* skip */ }
    }
    found.sort((a, b) => { const ra = a.rsi != null ? a.rsi : 50, rb = b.rsi != null ? b.rsi : 50; return rb !== ra ? rb - ra : b.chg - a.chg; });
    S.targets = found.slice(0, 10); saveS();
    if (!S.targets.length) { log("Сканер: подходящих пампов не найдено", "err"); return "Ничего не найдено"; }
    P.symbol = S.targets[0].symbol; save();
    log(`🔥 Найдено ${S.targets.length} целей. Топ: ${S.targets[0].symbol} (RSI ${S.targets[0].rsi != null ? S.targets[0].rsi.toFixed(0) : "—"}, ${S.targets[0].chg.toFixed(1)}%)`, "ok");
    return S.targets.length + " целей";
  }
  const rangePct = (k, n) => { const t = k.slice(-Math.max(1, n)); let hi = -Infinity, lo = Infinity; for (const c of t) { hi = Math.max(hi, c.high); lo = Math.min(lo, c.low); } const l = t[t.length - 1].close || 0; return l ? (hi - lo) / l * 100 : 0; };
  const movePct = (k, n) => { const l = k[k.length - 1], p = k[Math.max(0, k.length - 1 - n)]; return p && p.close ? (l.close - p.close) / p.close * 100 : 0; };
  const volSpike = (k, lb) => { if (k.length < lb + 2) return 1; const last = k[k.length - 1].volume || 0, prev = k.slice(-lb - 1, -1), avg = prev.reduce((s, c) => s + (c.volume || 0), 0) / (prev.length || 1); return avg > 0 ? last / avg : 1; };
  async function scanUltraVol1m() {
    const category = P.category; P.interval = "1"; save();
    const r = await pub("/v5/market/tickers", { category });
    const pool = (r.list || []).filter(t => t.symbol.endsWith("USDT") && parseFloat(t.turnover24h || 0) > 1.5e6).sort((a, b) => Math.abs(parseFloat(b.price24hPcnt || 0)) - Math.abs(parseFloat(a.price24hPcnt || 0))).slice(0, 48);
    log("Ультра 1m: проверяю " + pool.length + " пар…", "info");
    const out = [];
    for (let i = 0; i < pool.length; i += 8) {
      const part = await Promise.all(pool.slice(i, i + 8).map(async (coin) => {
        try { const k = await getKlines(coin.symbol, category, "1", 40); if (!k || k.length < 16) return null;
          const r15 = rangePct(k, 15), r5 = rangePct(k, 5), r1 = rangePct(k, 1), m5 = movePct(k, 5), m1 = movePct(k, 1), atr = estimateAtrPct(k, 14), atrPct = atr != null ? atr * 100 : 0, sp = volSpike(k, 12), rsi = calculateRSI(k, 14);
          const score = r15 * 1.4 + r5 * 1.8 + r1 * 2.2 + Math.abs(m5) * 1.2 + Math.abs(m1) * 1.5 + atrPct * 1.6 + Math.min(sp, 6) * 0.35;
          if (!(r15 >= 0.70 || r5 >= 0.45 || r1 >= 0.28 || Math.abs(m5) >= 0.40 || atrPct >= 0.22)) return null;
          return { symbol: coin.symbol, chg: m5, chg24: parseFloat(coin.price24hPcnt || 0) * 100, rsi, turnover: parseFloat(coin.turnover24h || 0), scannedAt: now(), range1m: r15, range5: r5, range1: r1, atrPct, volSpike: sp, score, tf: "1" };
        } catch (e) { return null; } }));
      out.push(...part.filter(Boolean));
    }
    const found = out.sort((a, b) => b.score - a.score).slice(0, 12);
    S.targets = found; saveS();
    if (!found.length) { log("Ультра 1m: сейчас нет сильных движений среди ликвидных пар", "err"); return "Ничего горячего"; }
    P.symbol = found[0].symbol; save();
    log(`⚡ Ультра 1m: ${found.length} пар. Топ ${found[0].symbol} · 15m-диап ${found[0].range1m.toFixed(2)}% · 5m ${found[0].chg >= 0 ? "+" : ""}${found[0].chg.toFixed(2)}% · ATR ${found[0].atrPct.toFixed(2)}%`, "ok");
    return found.length + " горячих пар";
  }

  // ---------- autopilot: every autoScanMin minutes - scan, move the grid to the best target ----------
  let autoBusy = false;
  async function autopilot() {
    if (!P.running || !P.autopilot || autoBusy || now() - (S.lastAuto || 0) < Math.max(1, P.autoScanMin) * 60000) return;
    autoBusy = true; S.lastAuto = now();
    const old = (P.symbol || "").toUpperCase();
    try {
      log(`🤖 Автопилот: скан ${P.autoScanMode === "ultra" ? "ультра-волатильных 1m" : "пампов под Short"}…`, "info");
      if (P.autoScanMode === "ultra" || P.strategy === "follow") await scanUltraVol1m(); else await scanPump();
      // hysteresis: the current coin stays while it is still among the top-3 targets
      if (old && S.targets.slice(0, 3).some(t => t.symbol === old)) { P.symbol = old; save(); }
      const neu = (P.symbol || "").toUpperCase();
      if (neu && neu !== old) {
        const w = W[old], hasPos = w && w.pos && w.pos.size > 0;
        if (!hasPos && P.liveEnabled && keys()) await cancelOurOrders(old, P.category);   // the old coin is free: its grid is removed
        log(`🤖 Автопилот: сетка → ${neu}${hasPos ? ` (по ${old} открыт шорт — веду его до TP/SL)` : old ? ` (с ${old} сетка снята)` : ""}`, "ok");
      } else log(`🤖 Автопилот: остаюсь на ${neu || old}`, "info");
    } catch (e) { log("Автопилот: " + e.message, "err"); }
    finally { autoBusy = false; saveS(); }
  }
  setInterval(() => { autopilot().catch(() => {}); }, 30000);

  // ---------- commands from the page ----------
  async function start() {
    if (P.liveEnabled && !keys()) return "Нет BYBIT_API_KEY / BYBIT_API_SECRET в Railway → Variables";
    P.running = true; save(); ensureDayKey();
    status = "SYNC…";
    await refreshOpenUniverse(); ensureWatch(P.symbol || "BTCUSDT");
    log(`START multi · ${Math.max(2, Math.round(P.intervalSec) || 3)}s · live=${P.liveEnabled} · контроль ${Object.keys(W).length} монет`, "ok");
    runCycle(); schedule();
    return "Бот запущен — все позиции под контролем";
  }
  async function stop(cancel) {
    P.running = false; save(); schedule(); status = "ВЫКЛ";
    if (cancel && P.liveEnabled && keys()) await cancelOurOrders((P.symbol || "").toUpperCase().trim(), P.category);
    log("STOP", "info"); return "Остановлен";
  }
  function setParams(o) {
    const ch = [];
    for (const k of NUMS) if (o[k] != null && o[k] !== "" && Number.isFinite(Number(o[k])) && Number(o[k]) !== P[k]) { P[k] = Number(o[k]); ch.push(k); }
    for (const k of BOOLS) if (typeof o[k] === "boolean" && o[k] !== P[k]) { P[k] = o[k]; ch.push(k); }
    if (typeof o.symbol === "string" && o.symbol.trim()) { const s = o.symbol.trim().toUpperCase(); if (s !== P.symbol) { P.symbol = s; ch.push("symbol"); } }
    if (["1", "3", "5", "15"].includes(String(o.interval)) && String(o.interval) !== P.interval) { P.interval = String(o.interval); ch.push("interval"); }
    if (["linear", "inverse"].includes(o.category) && o.category !== P.category) { P.category = o.category; ch.push("category"); }
    if (["sell", "follow"].includes(o.strategy) && o.strategy !== P.strategy) { P.strategy = o.strategy; ch.push("strategy"); }
    if (["pump", "ultra"].includes(o.autoScanMode) && o.autoScanMode !== P.autoScanMode) { P.autoScanMode = o.autoScanMode; ch.push("autoScanMode"); }
    save(); if (ch.includes("intervalSec")) schedule();
    if (ch.includes("liveEnabled")) log(P.liveEnabled ? "⚠ LIVE включён: реальные ордера" : "Live выключен: только симуляция", P.liveEnabled ? "err" : "info");
    return ch;
  }
  function toggleShukin() {
    P.shukinMode = !P.shukinMode;
    if (P.shukinMode) { Object.assign(P, SHUKIN); if (P.interval === "15") P.interval = "5"; log("⚡ Режим Щукина ВКЛ: шаг сетки 1%, тейк 0.7%, аварийный SL 10% + фильтр OI/CVD", "ok"); }
    else { shukinSig = null; log("Режим Щукина ВЫКЛ", "info"); }
    save(); schedule(); return P.shukinMode ? "Режим Щукина ВКЛ" : "Режим Щукина ВЫКЛ";
  }
  async function closePos(symbol) { symbol = (symbol || P.symbol).toUpperCase(); const pos = await getPosition(symbol, P.category); const w = ensureWatch(symbol); w.closingByBot = true; await closeMarket(symbol, P.category); if (pos && pos.size > 0) recordTrade(pos.unrealisedPnl || 0, symbol, "ручное закрытие"); return pos.size ? "Закрыто" : "Позиции нет"; }
  async function position(symbol) { return getPosition((symbol || P.symbol).toUpperCase(), P.category); }
  function clearTargets() { S.targets = []; saveS(); log("Список целей очищен", "info"); return "Очищено"; }
  async function sync() { await refreshOpenUniverse(); for (const s of Object.keys(W)) { try { const w = W[s]; w.candles = await getKlines(s, P.category, P.interval, 40); if (w.candles.length) { w.mid = w.candles[w.candles.length - 1].close; w.rsi = calculateRSI(w.candles, 14); } if (w.posEx) w.pos = w.posEx; } catch (e) { /* skip */ } } log("Синхр биржи: " + Object.keys(W).length + " монет", "ok"); return Object.keys(W).length + " активных монет"; }

  function view() {
    ensureDayKey();
    return { autoNext: P.running && P.autopilot ? Math.max(0, Math.round(((S.lastAuto || 0) + Math.max(1, P.autoScanMin) * 60000 - now()) / 60000)) : null, params: P, status, conn, cycles: S.cycles, trades: S.trades, wins: S.wins, losses: S.losses, dayPnl: +S.dayPnl.toFixed(4), targets: S.targets, history: S.history.slice(-50).reverse(),
      shukin: shukinSig, log: LOG.slice(0, 150),
      watches: Object.values(W).map(w => ({ symbol: w.symbol, mid: w.mid, pos: w.pos && w.pos.size ? w.pos : (w.posEx || null), orders: w.orders, ourOrders: w.ourOrders, isSpike: w.isSpike, volOk: w.volOk, status: w.status, atrPct: w.atrPct, rsi: w.rsi, levels: w.levels || null, targets: w.targets || [], side: w.side || (P.strategy === "sell" ? -1 : 0), dir: w.dir || null, spark: (w.candles || []).slice(-40).map(c => c.close), sig: w.sig || null })) };
  }
  if (P.running) setTimeout(() => { log("Сервер перезапущен — бот продолжает работу", "ok"); runCycle(); schedule(); }, 3000);
  return { start, stop, runCycle, setParams, toggleShukin, scanPump, scanUltraVol1m, closePos, position, clearTargets, sync, view, params: P, analyzeShukinContext, getKlines, getOpenInterest };
}
module.exports = { create };
