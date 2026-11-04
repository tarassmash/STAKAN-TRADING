// Sell Grid — Short: серверный движок (перенос логики страницы sell-grid-shukin.html, работает 24/7).
// Мульти-монеты: каждый цикл подтягивает ВСЕ открытые позиции и ордера с биржи и ведёт их одновременно:
//   защита от спайка, фильтр ATR, TP / SL по средней цене шорта, maker-тейк лимитом, сетка SELL-лимитов выше цены.
// Режим Щукина: шаг сетки от 1%, тейк от 0.7%, аварийный SL 10%, добор выше при открытом шорте, фильтр OI/CVD.
"use strict";
const OPT = require("./optimizer");

const DEFAULTS = {
  symbol: "BTCUSDT", interval: "5", category: "linear",
  gridStepPct: 0.40, gridLevels: 8, tpPct: 0.40, slPct: 10, orderUsdt: 20, maxDailyLoss: 15, intervalSec: 3,
  liveEnabled: false, makerExit: true, superSpikeProtect: true, maxSpikePct: 0.40, volFilter: true, minAtrPct: 0.15, maxAtrPct: 4,
  shukinMode: false, running: false,
  autopilot: true, autoScanMin: 5, autoScanMode: "pump",
  strategy: "sell",        // "sell" = Sell Grid (short from a pump) | "follow" = autopilot after the price (long or short where it goes)
  followBand: 0.08,
  leverage: 3,
  smartOn: true,           // ⚡ smart adaptation from the live tape: catch a move at its start and ride all of it
  fastK: 2.5,              // the 15 s move must be this many times the coin's usual 15 s move ...
  fastMinPct: 0.35,        // ... and at least this %
  fastAggr: 0.65,          // ... with this share of the volume hitting in one direction
  fastSpeed: 2,            // ... and the tape this many times faster than usual
  momIoc: false,           // NO market / IOC: entries with the move are limits at the best price, chased after the price
  chaseAggrSec: 20,        // an exit limit (maker, at the own best price) not filled in this many s -> a limit at the opposite best price (still a limit, chased)
  slLimitPct: 1,           // the stop on the exchange is a LIMIT: it fills no worse than this % past the stop price
  momCap: 0.15,
  momAdds: 2,              // adds while the move goes on
  aiAuto: false,           // 🧠 learn again by itself every aiEveryMin minutes
  aiEveryMin: 30,
  pulseOn: true,           // sudden pump / dump: adapt at once (cut the position against it, go with the move)
  pulsePct: 1.5,           // a move of this % ...
  pulseSec: 60,            // ... within this many seconds is a pump / dump
  momHoldMin: 10,          // after the move calms down for this long -> back to the +-limits
  momSl: 1.5,              // the stop of a position taken with the move, %            // set on every coin the bot trades
  autoStep: true,          // the step and the take from the account's real fees: at least feeX × the round trip
  feeX: 10,
  bandLevels: 3,           // limits per side while flat: the first at bandPct, the next every step
  bandPct: 3,              // autopilot after the price: SELL limit this % above, BUY limit this % below (they follow the price behind it)
  bandTp: 1.0,             // take (limit, maker) from the average entry, %
  bandSl: 10,             // stop on the exchange from the average entry, %
  bandTrail: true,         // trailing take: reaching the take activates a trailing exit on the exchange instead of closing at once
  bandTrailGap: 0.5,       // the trailing exit follows the best price at this %
  bandAdds: 5,             // extra adding limits in the position's direction, each bandPct further
  fullAuto: false,         // 🚀 one button: the grid only on the most volatile coin of the last 1 minute, re-picked every faEveryMin minutes
  faEveryMin: 3,
  hunt: false,             // 🎯 the impulse hunter (order books + tape of ~25 coins, entries seconds before a pump / dump)
  hUniverse: 25, hBand: 0.25, hSpeed: 3, hScore: 5, hLate: 0.8, hMaxSpread: 0.12, hSlipTicks: 1,
  hAct: 0.3, hGap: 0.25, hStop: 0.6, hTimeSec: 30, hChaseSec: 2, hCoolSec: 120, hMax: 2,
  bandRef: 5,              // the limits follow the average of this many last candles (a spike fills them instead of pushing them away)   // 24/7: re-scan and move the grid to the best target by itself
};
const SHUKIN = { gridStepPct: 1.00, gridLevels: 5, tpPct: 0.70, slPct: 10, orderUsdt: 12, intervalSec: 3, maxSpikePct: 0.50, minAtrPct: 0.20, maxAtrPct: 5, makerExit: true, superSpikeProtect: true, volFilter: true };
const NUMS = ["hUniverse", "hBand", "hSpeed", "hScore", "hLate", "hMaxSpread", "hSlipTicks", "hAct", "hGap", "hStop", "hTimeSec", "hChaseSec", "hCoolSec", "hMax", "chaseAggrSec", "slLimitPct", "faEveryMin", "fastK", "fastMinPct", "fastAggr", "fastSpeed", "momCap", "momAdds", "aiEveryMin", "pulsePct", "pulseSec", "momHoldMin", "momSl", "leverage", "feeX", "bandLevels", "bandTrailGap", "bandPct", "bandTp", "bandSl", "bandAdds", "bandRef", "followBand", "autoScanMin", "gridStepPct", "gridLevels", "tpPct", "slPct", "orderUsdt", "maxDailyLoss", "intervalSec", "maxSpikePct", "minAtrPct", "maxAtrPct"];
const BOOLS = ["fullAuto", "smartOn", "momIoc", "aiAuto", "pulseOn", "autoStep", "bandTrail", "autopilot", "liveEnabled", "makerExit", "superSpikeProtect", "volFilter"];

function create(host) {
  const { bx, lsGet, lsSet, keys } = host;
  const P = Object.assign({}, DEFAULTS, lsGet("sg.p", {}));
  const save = () => lsSet("sg.p", P);
  if (!lsGet("sg.v2", false)) { Object.assign(P, { leverage: 10, autoStep: true, feeX: 10, slPct: 10, bandSl: 10, bandPct: 3, bandLevels: 3, bandAdds: 5, gridLevels: 8 }); save(); lsSet("sg.v2", true); }
  if (!lsGet("sg.v3", false)) { P.leverage = 3; Object.assign(P, { pulseOn: true, pulsePct: 1.5, pulseSec: 60, momHoldMin: 10, momSl: 1.5 }); save(); lsSet("sg.v3", true); }
  if (!lsGet("sg.v5", false)) { P.momIoc = false; save(); lsSet("sg.v5", true); }   // no market / IOC orders anywhere: limits with chasing
  if (!lsGet("sg.v4", false)) { P.faEveryMin = 3; save(); lsSet("sg.v4", true); }   // the full autopilot re-picks the coin every 3 min (was 1)
  const S = Object.assign({ lastAuto: 0, cycles: 0, trades: 0, wins: 0, losses: 0, dayPnl: 0, dayKey: "", targets: [], history: [] }, lsGet("sg.s", {}));
  const saveS = () => lsSet("sg.s", { faAi: S.faAi, fa: S.fa, ai: S.ai, lastAuto: S.lastAuto, cycles: S.cycles, trades: S.trades, wins: S.wins, losses: S.losses, dayPnl: S.dayPnl, dayKey: S.dayKey, targets: S.targets, history: S.history.slice(-300) });
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
  // order requests go one by one (>= 120 ms apart, Bybit allows 10/s); a rate-limit answer waits 1.5 s and retries
  let qTail = Promise.resolve(), qLast = 0;
  function priv(method, path, params) {
    if (method === "GET") return bx(path, { query: params, auth: true });
    const run = async () => {
      const wait = Math.max(0, qLast + 120 - now()); if (wait) await new Promise(r => setTimeout(r, wait));
      qLast = now();
      try { return await bx(path, { method: "POST", body: params, auth: true }); }
      catch (e) { if (String(e.code) === "10006" || /rate limit|too many visits/i.test(e.message)) { await new Promise(r => setTimeout(r, 1500)); qLast = now(); return bx(path, { method: "POST", body: params, auth: true }); } throw e; }
    };
    const p2 = qTail.then(run, run); qTail = p2.catch(() => {}); return p2;
  }
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

  // ---------- real fees -> the densest step that still pays feeX × the round trip; leverage ----------
  const FEES = {};
  async function feeOf(symbol, category) {
    const f = FEES[symbol]; if (f && now() - f.at < 3600e3) return f;
    let maker = 0.02, taker = 0.055;
    if (keys()) { try { const r = ((await priv("GET", "/v5/account/fee-rate", { category, symbol })).list || [])[0]; if (r) { maker = Math.max(0, Number(r.makerFeeRate) * 100); taker = Number(r.takerFeeRate) * 100; } } catch (e) { /* defaults */ } }
    return (FEES[symbol] = { maker, taker, at: now() });
  }
  async function stepsOf(symbol, category) {
    const f = await feeOf(symbol, category), X = Math.max(1, P.feeX || 10);
    const lim = +(X * 2 * Math.max(f.maker, 0.01)).toFixed(3);            // limit in + limit out
    const mkt = +(X * (Math.max(f.maker, 0.01) + f.taker)).toFixed(3);   // limit in + market out (stop / trailing)
    return { maker: f.maker, taker: f.taker, lim, mkt };
  }
  const LEV = {};
  async function ensureLev(symbol, category) {
    const L = Math.max(1, Math.round(P.leverage || 10)); if (LEV[symbol] === L || !keys()) return;
    try { await priv("POST", "/v5/position/set-leverage", { category, symbol, buyLeverage: String(L), sellLeverage: String(L) }); log(`${symbol}: плечо x${L}`, "info"); }
    catch (e) { if (!/110043|not modified/i.test(String(e.code) + e.message)) log(`${symbol}: плечо: ${e.message}`, "err"); }
    LEV[symbol] = L;
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
  // ---------- exits: NO market. A fast limit at the best price, chased after the price every second ----------
  // first a maker limit (PostOnly) at the own best price; not filled in chaseAggrSec -> a limit at the opposite best price
  // (it fills at once at that price, but never worse: it is still a limit), re-placed after the price until the position is 0
  const CH = {};   // symbol -> { why, since, d, onDone }
  const chasing = (symbol) => !!CH[symbol];
  async function closeChase(symbol, category, why, opt) {
    symbol = symbol.toUpperCase();
    if (CH[symbol]) return;
    const pos = await getPosition(symbol, category);
    if (!pos.size) return;
    CH[symbol] = { why: why || "выход", since: now(), side: pos.side, aggrSec: opt && opt.aggrSec };
    const w = ensureWatch(symbol); w.closingByBot = true;
    log(`${symbol}: ⏩ выход лимиткой с догонкой (${CH[symbol].why}) — ${pos.side === "Buy" ? "SELL" : "BUY"} ${pos.size} по лучшей цене, без маркета`, "ok");
    await chaseStep(symbol);
  }
  const closeMarket = (symbol, category, why) => closeChase(symbol, category, why);   // the old name: it is a chased limit now
  async function chaseStep(symbol) {
    const c = CH[symbol]; if (!c || c.busy) return; c.busy = true;
    const category = P.category;
    try {
      const pos = await getPosition(symbol, category);
      const open = (await getOpenOrders(symbol, category)).filter(o => (o.orderLinkId || "").startsWith("sg_cl_"));
      if (!pos.size || pos.side !== c.side) {
        for (const o of open) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } }
        log(`${symbol}: ✅ закрыто лимиткой (${c.why}) за ${Math.round((now() - c.since) / 1000)} с`, "ok");
        delete CH[symbol]; const w = W[symbol]; if (w) { w.pos = { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0 }; w.slSet = 0; w.trailSet = null; w.trOn = false; }
        return;
      }
      const inst = await getInstrument(symbol, category), book = await getOrderBook(symbol, category);
      if (!book.bids.length || !book.asks.length) return;
      const sell = pos.side === "Buy", aggr = now() - c.since >= Math.max(c.aggrSec ? 0.5 : 3, c.aggrSec || P.chaseAggrSec || 20) * 1000;
      const px = roundToStep(sell ? (aggr ? book.bids[0] : book.asks[0]) : (aggr ? book.asks[0] : book.bids[0]), inst.tickSize);
      const tif = aggr ? "GTC" : "PostOnly", tick = parseFloat(inst.tickSize);
      if (aggr && !c.aggrLogged) { c.aggrLogged = true; log(`${symbol}: ⏩ ${c.aggrSec || P.chaseAggrSec || 20} с без исполнения — лимитка по встречной лучшей цене (всё равно лимит, не маркет)`, "info"); }
      const mine = open.filter(o => o.side === (sell ? "Sell" : "Buy"));
      for (const o of mine.slice(1)) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } }
      const o = mine[0];
      if (o && o.timeInForce === tif && Math.abs(parseFloat(o.price) - px) < tick / 2) return;   // it stands at the best price
      if (o && o.timeInForce === tif) {
        try { await priv("POST", "/v5/order/amend", { category, symbol, orderId: o.orderId, price: String(px), qty: String(pos.size) }); return; } catch (e) { /* re-place below */ }
      }
      if (o) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } }
      await priv("POST", "/v5/order/create", { category, symbol, side: sell ? "Sell" : "Buy", orderType: "Limit", qty: String(pos.size), price: String(px), timeInForce: tif, reduceOnly: true, positionIdx: pos.positionIdx, orderLinkId: "sg_cl_" + now() + "_" + Math.floor(Math.random() * 999) });
    } catch (e) { log(`${symbol}: догонка выхода: ${e.message}`, "err"); }
    finally { c.busy = false; }
  }
  setInterval(() => { for (const s2 of Object.keys(CH)) chaseStep(s2).catch(() => {}); }, 1000);

  // a stop on the exchange as a LIMIT (Partial TP/SL with slOrderType Limit): it works with the server down, never as market
  const slLim = (slPx, d, inst) => roundToStep(slPx * (1 - d * (P.slLimitPct || 1) / 100), inst.tickSize);
  async function clearPosStops(symbol, category, positionIdx) {
    try { await priv("POST", "/v5/position/trading-stop", { category, symbol, tpslMode: "Full", stopLoss: "0", trailingStop: "0", positionIdx }); } catch (e) { /* none */ }
    try { const t = (await priv("GET", "/v5/order/realtime", { category, symbol, orderFilter: "tpslOrder", limit: 50 })).list || [];
      for (const o of t) if (/StopLoss|TrailingStop/i.test(o.stopOrderType || "")) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } } } catch (e) { /* none */ }
  }
  async function setLimitStop(symbol, category, pos, slPx, inst) {
    const d = pos.side === "Buy" ? 1 : -1;
    await clearPosStops(symbol, category, pos.positionIdx);
    await priv("POST", "/v5/position/trading-stop", { category, symbol, tpslMode: "Partial", slSize: String(pos.size), stopLoss: String(slPx), slTriggerBy: "LastPrice", slOrderType: "Limit", slLimitPrice: String(slLim(slPx, d, inst)), positionIdx: pos.positionIdx });
  }
  // the stop attached to an entry limit: also a limit
  function attachLimitSl(body, stopPx, d, inst) {
    Object.assign(body, { tpslMode: "Partial", stopLoss: String(stopPx), slTriggerBy: "LastPrice", slOrderType: "Limit", slLimitPrice: String(slLim(stopPx, d, inst)) });
  }
  // ---------- the order size from the money on the account: never "not enough", the lot is fitted to what is free ----------
  let AV = { v: null, at: 0 }, noMoneyAt = 0;
  async function availUsdt(force) {
    if (!force && AV.v != null && now() - AV.at < 2000) return AV.v;
    try { const r = await priv("GET", "/v5/account/wallet-balance", { accountType: "UNIFIED" }); const a = (r.list || [])[0] || {};
      AV = { v: Number(a.totalAvailableBalance || 0), eq: Number(a.totalEquity || 0), at: now() }; } catch (e) { /* keep the last */ }
    return AV.v;
  }
  const isNoMoney = (e) => /^(110004|110007|110012|110044|110045|110052|110014)$/.test(String(e && e.code)) || /not enough|insufficient|ab not enough/i.test(String(e && e.message));
  function noMoney(symbol, av, minN) {
    if (now() - noMoneyAt < 60000) return; noMoneyAt = now();
    log(`${symbol}: на счёте свободно ${av != null ? av.toFixed(2) : "?"} USDT — не хватает даже на минимальный ордер Bybit (${minN} USDT позиции = ~${(minN / Math.max(1, P.leverage || 1)).toFixed(2)} USDT при плече x${P.leverage}), пропускаю (жду, пока освободятся деньги)`, "info");
  }
  // an opening (not reduce-only) limit: its qty is cut to the free balance × leverage; "not enough" from Bybit -> half the lot, again
  // share: how many orders still to place this cycle - the free money is split evenly between them (the nearest first)
  async function placeEntry(body, inst, share) {
    const symbol = body.symbol, px = Number(body.price), step = inst.qtyStep, stepN = parseFloat(step), lev = Math.max(1, Number(P.leverage) || 1);
    const minQty = Math.max(parseFloat(inst.minQty), Math.ceil(inst.minNotional * 1.02 / px / stepN - 1e-9) * stepN);
    const want = Number(body.qty);
    const av = keys() ? await availUsdt() : null;
    if (av != null) {
      const all = floorToStep(av * lev * 0.95 / px, step);
      let maxQty = floorToStep(av * lev * 0.95 / Math.max(1, share || 1) / px, step);
      if (maxQty < minQty - 1e-12 && all >= minQty - 1e-12) maxQty = Math.min(all, roundToStep(minQty, step));   // not enough for an even split: the minimum lot
      if (maxQty < minQty - 1e-12) { noMoney(symbol, av, inst.minNotional); return null; }
      if (maxQty < want) body.qty = String(maxQty);
    }
    for (let i = 0; i < 4; i++) {
      try {
        const r = await priv("POST", "/v5/order/create", body);
        if (Number(body.qty) < want) log(`${symbol}: лот уменьшен под свободные деньги: ${body.qty} вместо ${want} (свободно ${AV.v != null ? AV.v.toFixed(2) : "?"} USDT, плечо x${lev})`, "info");
        if (AV.v != null) AV.v = Math.max(0, AV.v - Number(body.qty) * px / lev);
        return r;
      } catch (e) {
        if (!isNoMoney(e)) throw e;
        AV.at = 0;
        const q = floorToStep(Number(body.qty) / 2, step);
        if (q < minQty - 1e-12) { noMoney(symbol, AV.v, inst.minNotional); return null; }
        body.qty = String(q);
      }
    }
    return null;
  }
  async function placeMakerExit(symbol, category, pos, tpPrice, inst) {
    if (!pos.size) return;
    const price = roundToStep(tpPrice, inst.tickSize);
    const body = { category, symbol, side: pos.side === "Buy" ? "Sell" : "Buy", orderType: "Limit", qty: String(pos.size), price: String(price), timeInForce: "PostOnly", reduceOnly: true, positionIdx: pos.positionIdx, orderLinkId: "sg_tp_" + now() };
    try { await priv("POST", "/v5/order/create", body); log(`${symbol}: Maker TP @ ${price}`, "ok"); }
    catch (e) { body.timeInForce = "GTC"; body.orderLinkId = "sg_tp_" + now() + "_g"; try { await priv("POST", "/v5/order/create", body); log(`${symbol}: TP @ ${price} (GTC)`, "ok"); } catch (e2) { log(`${symbol}: Maker TP fail: ${e2.message}`, "err"); } }
  }
  async function placeSellLimit(symbol, category, qty, price, tp, sl, inst, share) {
    const limitPrice = roundToStep(price, inst.tickSize);
    const body = { category, symbol, side: "Sell", orderType: "Limit", qty: String(qty), price: String(limitPrice), timeInForce: "GTC", positionIdx: await shortIdx(symbol, category), orderLinkId: "sg_s_" + now() + "_" + Math.floor(Math.random() * 999) };
    if (sl) attachLimitSl(body, sl, -1, inst);
    if (tp && !P.makerExit) Object.assign(body, { tpslMode: "Partial", takeProfit: String(tp), tpTriggerBy: "LastPrice", tpOrderType: "Limit", tpLimitPrice: String(tp) });
    log(`${symbol}: → Sell limit ${qty} @ ${limitPrice}`, "info");
    try { const r = await placeEntry(body, inst, share); if (r) log(`${symbol}: ✅ Sell ${body.qty} #${r.orderId}`, "ok"); }
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
    const ST = await stepsOf(symbol, category); w.steps = ST;
    if (P.autoStep) { gridStepPctVal = ST.lim; tpPctVal = P.makerExit ? ST.lim : ST.mkt; }
    else { gridStepPctVal = Math.max(gridStepPctVal, ST.lim); tpPctVal = Math.max(tpPctVal, P.makerExit ? ST.lim : ST.mkt); }   // never below feeX × fees
    if (P.liveEnabled) await ensureLev(symbol, category);
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
      if (movePct >= tpPct) { const pnl = pos.unrealisedPnl || 0; log(`${symbol}: 🎯 TP ${(movePct * 100).toFixed(2)}%${shukin ? " (Щукин short take)" : ""}`, "ok"); w.closingByBot = true; await cancelOurOrders(symbol, category); await closeChase(symbol, category, "TP"); recordTrade(pnl, symbol, "TP"); w.pos = { size: 0 }; return; }
      if (movePct <= -slPct) { const pnl = pos.unrealisedPnl || 0; log(`${symbol}: 🚨 SL ${(movePct * 100).toFixed(2)}%`, "err"); w.closingByBot = true; await cancelOurOrders(symbol, category); await closeChase(symbol, category, "SL"); recordTrade(pnl, symbol, "SL"); w.pos = { size: 0 }; return; }
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
    let left = targets.filter(px => !matched.has(px)).length;
    for (const px of targets) {
      if (matched.has(px)) continue;
      const tp = roundToStep(px * (1 - tpPct), inst.tickSize), sl = (!shukin || pos.size === 0) ? roundToStep(px * (1 + slPct), inst.tickSize) : null;
      await placeSellLimit(symbol, category, qty, px, tp, sl, inst, left--);
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
    const category = P.category, ST = await stepsOf(symbol, category); w.steps = ST;
    const off = (P.bandPct || 3) / 100, step = ST.lim / 100;
    let tpP = P.bandTrail ? Math.max(P.bandTp || 0, ST.mkt) : Math.max(P.bandTp || 0, ST.lim); if (P.autoStep) tpP = P.bandTrail ? ST.mkt : ST.lim;
    const tp = tpP / 100, gapP = P.autoStep ? +(tpP * 0.4).toFixed(3) : P.bandTrailGap, sl = (P.bandSl || 10) / 100, usdt = P.orderUsdt || 20;
    w.band = { off: P.bandPct, step: ST.lim, tp: +tpP.toFixed(3), gap: gapP };
    if (P.liveEnabled) await ensureLev(symbol, category);
    let qty = floorToStep(usdt / mid, inst.qtyStep);
    if (qty < parseFloat(inst.minQty)) qty = parseFloat(inst.minQty);
    if (qty * mid < inst.minNotional) qty = roundToStep(Math.ceil(inst.minNotional * 1.02 / mid / parseFloat(inst.qtyStep)) * parseFloat(inst.qtyStep), inst.qtyStep);
    const n = Math.max(1, Math.round(P.bandRef) || 5), last = (candles || []).slice(-n), ref = last.length ? last.reduce((a, c) => a + c.close, 0) / last.length : mid;
    const live = P.liveEnabled;
    let pos = { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0, positionIdx: 0 };
    if (live) pos = await getPosition(symbol, category);
    if (live && w.pos && w.pos.size > 0 && !pos.size) {   // closed on the exchange (take or stop)
      try { const cp = ((await priv("GET", "/v5/position/closed-pnl", { category, symbol, limit: 5 })).list || []).filter(x => Number(x.updatedTime || x.createdTime) > (w.posSince || 0) - 60000);
        const pnl = cp.reduce((a, x) => a + Number(x.closedPnl || 0), 0); recordTrade(pnl, symbol, pnl >= 0 ? (P.bandTrail ? "трейлинг-тейк" : "тейк лимитом") : "стоп"); } catch (e) { /* skip */ }
      await cancelOurOrders(symbol, category);
    }
    // ⚡ pulse: the price moved pulsePct % within pulseSec -> a pump / dump: adapt at once
    w.ph = (w.ph || []).filter(x => now() - x.t <= (P.pulseSec || 60) * 1000); w.ph.push({ t: now(), p: mid });
    { const o0 = w.ph[0], span = (now() - o0.t) / 1000, mv = (mid / o0.p - 1) * 100;
      if (P.pulseOn && span >= Math.min(20, (P.pulseSec || 60) / 3) && Math.abs(mv) >= P.pulsePct) {
        const d0 = Math.sign(mv);
        if (!w.mom || w.mom.d !== d0) log(`⚡ ${symbol}: ${d0 > 0 ? "ПАМП" : "ДАМП"} ${mv >= 0 ? "+" : ""}${mv.toFixed(2)}% за ${Math.round(span)} с — адаптируюсь: встречные лимитки убираю, вхожу по движению`, "ok");
        w.mom = { d: d0, at: now(), mv: +mv.toFixed(2) };
      } }
    if (pos.size && !(w.pos && w.pos.size)) { w.posSince = now(); w.posMom = !!(w.mom && (pos.side === "Buy" ? 1 : -1) === w.mom.d); w.momAddN = 0; w.lastAddPx = pos.avgPrice; }
    w.pos = pos;
    const pd = pos.size ? (pos.side === "Buy" ? 1 : -1) : 0;
    if (!pd) w.posMom = false;
    if (w.mom && !pd && now() - w.mom.at > (P.momHoldMin || 10) * 60000) { w.mom = null; log(`${symbol}: движение утихло — снова лимитки ±${P.bandPct}%`, "info"); }
    const md = w.mom && P.pulseOn ? w.mom.d : 0;
    let pre = 0; { const o0 = w.ph[0], mv = (mid / o0.p - 1) * 100; if (P.pulseOn && Math.abs(mv) >= P.pulsePct / 2) pre = Math.sign(mv); }   // a pump / dump is starting
    // the tape turned against a profitable position taken with the move: take the profit now
    if (live && pd && w.exitNow) {
      const pnl = pos.unrealisedPnl || 0, why = w.exitNow.why;
      log(`${symbol}: 💰 ${why} — фиксирую ${pd > 0 ? "лонг" : "шорт"} сразу (+${w.exitNow.prof}%, ${pnl >= 0 ? "+" : ""}${pnl.toFixed(3)} USDT)`, "ok");
      w.exitNow = null; w.closingByBot = true; await cancelOurOrders(symbol, category); await closeChase(symbol, category, "разворот ленты"); recordTrade(pnl, symbol, "выход на развороте ленты");
      w.pos = { size: 0 }; w.slSet = 0; w.trailSet = null; w.mom = null; return;
    }
    if (!pd) w.exitNow = null;
    // a position against a pump / dump is cut at once (not left to the 10 % stop)
    if (live && pd && md && pd === -md) {
      const pnl = pos.unrealisedPnl || 0;
      log(`${symbol}: ⚡ ${md > 0 ? "памп" : "дамп"} против ${pd > 0 ? "лонга" : "шорта"} — закрываю сразу (${pnl >= 0 ? "+" : ""}${pnl.toFixed(3)}), дальше по движению`, pnl >= 0 ? "ok" : "err");
      w.closingByBot = true; await cancelOurOrders(symbol, category); await closeChase(symbol, category, (md > 0 ? "памп" : "дамп") + " против позиции"); recordTrade(pnl, symbol, "адаптация к " + (md > 0 ? "пампу" : "дампу")); w.pos = { size: 0 }; w.slSet = 0; w.trailSet = null; return;
    }
    await shortIdx(symbol, category);
    const idxOf = (d) => (HEDGE[symbol] ? (d > 0 ? 1 : 2) : 0);
    // the wanted orders
    let want = [];
    const nL = Math.max(1, Math.round(P.bandLevels) || 3);
    const cap = (P.momCap || 0.15) / 100, iocPx = (d) => roundToStep(d > 0 ? (w.ask || mid) * (1 + cap) : (w.bid || mid) * (1 - cap), inst.tickSize);
    if (md && !pd) {
      if (P.momIoc) { if (now() - (w.lastIoc || 0) > 1500) want.push({ side: md > 0 ? "Buy" : "Sell", d: md, px: iocPx(md), mom: true, ioc: true }); }
      else want.push({ side: md > 0 ? "Buy" : "Sell", d: md, px: roundToStep(md > 0 ? (w.bid || mid) : (w.ask || mid), inst.tickSize), mom: true });   // a limit at the best price, chased every cycle
    }
    else if (md && pd === md) {   // riding the move: adds while the tape keeps pushing (each one further along the move)
      const tp0 = w.tape, typ = typical1m(w), go = tp0 && Math.sign(tp0.mv15) === pd && (pd > 0 ? tp0.buy : 1 - tp0.buy) >= P.fastAggr;
      if (!w.lastAddPx) w.lastAddPx = pos.avgPrice;
      if (w.posMom && go && (w.momAddN || 0) < (P.momAdds || 0) && (mid / w.lastAddPx - 1) * 100 * pd >= Math.max(step * 100, typ) && now() - (w.lastIoc || 0) > 1500)
        want.push(P.momIoc ? { side: pd > 0 ? "Buy" : "Sell", d: pd, px: iocPx(pd), mom: true, ioc: true, add: true, momAdd: true }
          : { side: pd > 0 ? "Buy" : "Sell", d: pd, px: roundToStep(pd > 0 ? (w.bid || mid) : (w.ask || mid), inst.tickSize), mom: true, add: true, momAdd: true });   // a limit at the best price, chased
    }
    else if (!pd) for (let k = 0; k < nL; k++) { want.push({ side: "Sell", d: -1, px: roundToStep(ref * (1 + off + step * k), inst.tickSize) }); want.push({ side: "Buy", d: 1, px: roundToStep(ref * (1 - off - step * k), inst.tickSize) }); }
    else if (pre && pre === -pd) { /* the price is starting to run against the position: no adds against it */ }
    else for (let k = 1; k <= Math.max(0, Math.round(P.bandAdds)); k++) want.push({ side: pd > 0 ? "Buy" : "Sell", d: pd, px: roundToStep(pos.avgPrice * (1 - pd * step * k), inst.tickSize), add: true });   // dense adds beyond the average
    w.levels = want.map(x => ({ px: x.px, side: x.d })); w.targets = want.map(x => x.px); w.side = pd || 0;
    w.dir = md ? { d: pd || md, why: `⚡ ${md > 0 ? "памп" : "дамп"} ${w.mom.mv}% — ${pd ? `еду по движению, трейлинг-тейк, стоп ${P.momSl}%` : "вход по движению лимиткой по лучшей цене"}` } : { d: pd, why: pd ? `${pd > 0 ? "лонг" : "шорт"} открыт — ${P.bandTrail ? `трейлинг-тейк с +${tpP.toFixed(2)}%, отступ ${gapP}%` : `тейк лимитом ${tpP.toFixed(2)}%`} от средней, доборы каждые ${ST.lim}%` : `жду: SELL от +${P.bandPct}% / BUY от −${P.bandPct}%, по ${nL} ур. каждые ${ST.lim}% (средняя ${n} свечей ${fmt(ref)})` };
    if (!live) { log(`[SIM АВТОПИЛОТ] ${symbol}: ${md ? `⚡ ${md > 0 ? "памп" : "дамп"} → ` : ""}${want.map(x => `${x.side} @ ${x.px}`).join(" · ")}${pd ? "" : ` (цена ${fmt(mid)})`}`, "info"); return; }
    if (symbol !== (P.symbol || "").toUpperCase() && !pd) { if (w.ourOrders > 0) { await cancelOurOrders(symbol, category); w.ourOrders = 0; } w.status = "свободна"; return; }
    const open = await getOpenOrders(symbol, category), ours = open.filter(o => (o.orderLinkId || "").startsWith("sg_b_")), tps = open.filter(o => (o.orderLinkId || "").startsWith("sg_tp_"));
    if (pd) {
      // a position with the move: the trailing distance from the coin's volatility (it must not shake out a big move),
      // and the trailing starts only when even its distance still leaves a profit after fees
      let gapUse = w.posMom ? +Math.min(3, Math.max(gapP || 0.3, 1.2 * typical1m(w))).toFixed(3) : gapP;
      if (w.posMom && w.trailGap && Math.abs(gapUse / w.trailGap - 1) < 0.15) gapUse = w.trailGap;   // small changes do not move it (no re-setting every cycle)
      const tpUse = w.posMom ? Math.max(ST.mkt, gapUse + ST.mkt * 0.5) / 100 : Math.max(tp, ((gapUse || 0) + ST.mkt * 0.5) / 100);
      const tpPx = roundToStep(pos.avgPrice * (1 + pd * tpUse), inst.tickSize);
      if (P.bandTrail) {
        // trailing take by the bot (the exchange trailing stop would close by market): it activates at the take level,
        // follows the best price, and a pull-back of gapUse % from the best -> exit by a fast limit chased after the price
        for (const o of tps) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } }
        w.trailGap = gapUse;
        if (!w.trOn && (pd > 0 ? mid >= tpPx : mid <= tpPx)) { w.trOn = true; w.trBest = mid; log(`${symbol}: 🎯 трейлинг-тейк включён на ${fmt(mid)} (+${(tpUse * 100).toFixed(2)}% от средней), еду за лучшей ценой, откат ${gapUse}% → выход лимиткой`, "ok"); }
        if (w.trOn) {
          w.trBest = pd > 0 ? Math.max(w.trBest, mid) : Math.min(w.trBest, mid);
          const back = (pd > 0 ? (w.trBest - mid) / w.trBest : (mid - w.trBest) / w.trBest) * 100;
          if (back >= gapUse) {
            const pnl = pos.unrealisedPnl || 0;
            log(`${symbol}: 💰 трейлинг-тейк: откат ${back.toFixed(2)}% от лучшей ${fmt(w.trBest)} — выхожу лимиткой`, "ok");
            await cancelOurOrders(symbol, category); await closeChase(symbol, category, "трейлинг-тейк"); recordTrade(pnl, symbol, "трейлинг-тейк"); return;
          }
        }
      } else {
        // the take: one reduce-only limit for the whole position (re-placed when the size or the average changed)
        const okTp = tps.find(o => Math.abs(parseFloat(o.price) - tpPx) <= parseFloat(inst.tickSize) * 2 && Math.abs(parseFloat(o.qty) - pos.size) < 1e-9);
        if (!okTp) { for (const o of tps) { try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ } } await placeMakerExit(symbol, category, pos, tpPx, inst); }
      }
      // the stop of the whole position on the exchange (from the average)
      const slUse = w.posMom ? (P.momSl || 1.5) / 100 : sl;
      const slPx = roundToStep(pos.avgPrice * (1 - pd * slUse), inst.tickSize);
      if (pd > 0 ? mid <= slPx : mid >= slPx) {
        const pnl = pos.unrealisedPnl || 0;
        log(`${symbol}: 🚨 стоп ${slPx} пройден — выхожу лимиткой с догонкой`, "err");
        await cancelOurOrders(symbol, category); await closeChase(symbol, category, "стоп"); recordTrade(pnl, symbol, "стоп"); return;
      }
      const slKey = slPx + "|" + pos.size;
      if (w.slSet !== slKey) {
        try { await setLimitStop(symbol, category, pos, slPx, inst); w.slSet = slKey; log(`${symbol}: стоп позиции ${slPx} ЛИМИТКОЙ (не хуже ${slLim(slPx, pd, inst)}; −${w.posMom ? P.momSl : P.bandSl}% от средней, плечо x${P.leverage})`, "info"); }
        catch (e) { if (String(e.code) === "34040") w.slSet = slKey; else log(`${symbol}: стоп: ${e.message}`, "err"); }
      }
    } else { w.slSet = 0; w.trailSet = null; w.trailGap = 0; w.trOn = false; w.trBest = 0; }
    if (isSpike && !pd) { w.status = "SPIKE — лимиты держу"; }
    // sync the entry limits: the opposite side goes, the others follow the price behind it
    const tol = md && !pd ? mid * 0.0005 : ref * Math.min(off * 0.15, step * 0.4), keep = new Set(), freeze = isSpike && !md;
    for (const o of ours) {
      const m = want.find((x, i2) => !keep.has(i2) && x.side === o.side && Math.abs(parseFloat(o.price) - x.px) <= tol);
      if (m) { keep.add(want.indexOf(m)); continue; }
      const same = want.findIndex((x, i2) => !keep.has(i2) && x.side === o.side);
      if (same >= 0 && !freeze) {   // move it after the price (amend keeps the order)
        try { await priv("POST", "/v5/order/amend", { category, symbol, orderId: o.orderId, price: String(want[same].px) }); keep.add(same); log(`${symbol}: ↕ ${o.side} ${o.price} → ${want[same].px}`, "info"); continue; } catch (e) { /* re-place below */ }
      }
      if (same >= 0 && freeze) { keep.add(same); continue; }   // during a spike the limits stay where they are (that is when they fill)
      try { await priv("POST", "/v5/order/cancel", { category, symbol, orderId: o.orderId }); } catch (e) { /* gone */ }
    }
    let left = want.length - keep.size;
    for (let k = 0; k < want.length; k++) {
      if (keep.has(k)) continue;
      const share = left--;
      const x = want[k], stopPx = roundToStep(x.px * (1 - x.d * sl), inst.tickSize);
      const body = { category, symbol, side: x.side, orderType: "Limit", qty: String(qty), price: String(x.px), timeInForce: x.ioc ? "IOC" : "PostOnly", positionIdx: idxOf(x.d), orderLinkId: "sg_b_" + now() + "_" + Math.floor(Math.random() * 999) };
      if (x.ioc) { w.lastIoc = now(); if (x.momAdd) { w.momAddN = (w.momAddN || 0) + 1; w.lastAddPx = mid; } }
      if (!pd) attachLimitSl(body, stopPx, x.d, inst);
      try { if (!(await placeEntry(body, inst, share))) continue; log(`${symbol}: ${x.momAdd ? "⚡ добор по движению " : x.ioc || x.mom ? "⚡ вход по движению " : x.add ? "добор " : ""}${x.side} ${x.ioc ? "IOC-лимит (не хуже " + x.px + ")" : "limit"} ${body.qty}${x.ioc ? "" : " @ " + x.px}`, x.ioc ? "ok" : "info"); }
      catch (e) { log(`${symbol}: ❌ ${x.side} limit: ${e.message}`, "err"); }
    }
    w.status = md ? `⚡ ${md > 0 ? "ПАМП" : "ДАМП"} · ${pd ? "по движению" : "вход лимиткой"}` : pd ? (pd > 0 ? "ЛОНГ" : "ШОРТ") + (P.bandTrail ? " · трейлинг-тейк" : " · тейк лимитом") : (isSpike ? "SPIKE — лимиты держу" : `±${P.bandPct}% лимитки`);
  }

  // the exchange's real orders for the chart: price, side, what it is
  function openForChart(open) {
    return open.filter(o => parseFloat(o.price) > 0 || parseFloat(o.triggerPrice) > 0).slice(0, 40).map(o => {
      const id = o.orderLinkId || "", stop = /StopLoss/i.test(o.stopOrderType || "");
      const kind = stop ? "sl" : id.startsWith("sg_cl_") ? "close" : id.startsWith("sg_tp_") ? "tp" : id.startsWith("sg_") ? "entry" : "other";
      return { px: parseFloat(stop ? (o.triggerPrice || o.price) : o.price), side: o.side === "Buy" ? 1 : -1, kind, qty: o.qty };
    });
  }
  // the account's fills of a coin (points on the chart)
  async function fills(symbol) {
    if (!keys()) return [];
    const r = await priv("GET", "/v5/execution/list", { category: P.category, symbol: symbol.toUpperCase(), limit: 100 });
    return (r.list || []).filter(x => x.execType === "Trade" || !x.execType).map(x => ({ t: Number(x.execTime), side: x.side === "Buy" ? 1 : -1, px: parseFloat(x.execPrice), qty: x.execQty, maker: x.isMaker === true || x.isMaker === "true" }));
  }
  function ensureWatch(symbol) { symbol = symbol.toUpperCase().trim(); if (!W[symbol]) W[symbol] = { symbol, mid: 0, candles: [], priceHistory: [], pos: { side: null, size: 0, avgPrice: 0, unrealisedPnl: 0 }, orders: 0, ourOrders: 0, isSpike: false, volOk: true, status: "—", atrPct: null, rsi: null }; return W[symbol]; }

  async function processSymbol(symbol) {
    const w0 = ensureWatch(symbol); if (w0.lock) return; w0.lock = true;
    try { await processSymbol0(symbol); } finally { w0.lock = false; if (w0.again) { w0.again = false; setTimeout(() => react(symbol), 50); } }
  }
  async function processSymbol0(symbol) {
    const category = P.category, w = ensureWatch(symbol);
    const book = await getOrderBook(symbol, category);
    if (!book.bids.length || !book.asks.length) throw new Error(symbol + ": пустой стакан");
    const mid = (book.bids[0] + book.asks[0]) / 2; w.mid = mid; w.bid = book.bids[0]; w.ask = book.asks[0];
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
    if (P.liveEnabled) { try { const open = await getOpenOrders(symbol, category); w.orders = open.length; w.ourOrders = open.filter(o => (o.orderLinkId || "").startsWith("sg_")).length; w.open = openForChart(open); w.openAt = now(); } catch (e) { /* keep */ } }
    else w.open = null;
    if (chasing(symbol)) { w.status = "⏩ выход лимиткой (догонка)"; return; }
    if (HUNT.has(symbol)) { w.status = "🎯 импульс"; return; }
    if (P.strategy === "impulse") { w.status = HUNT.has(symbol) ? "🎯 импульс" : "🎯 охота"; return; }
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
      if (P.strategy === "follow" && P.smartOn) TAPE.watch(list.length ? list : [symbol]); else TAPE.watch([]);
      if (!list.length && symbol) { ensureWatch(symbol); list.push(symbol); }
      const results = await Promise.allSettled(list.map(s => processSymbol(s)));
      results.forEach((r, i) => { if (r.status === "rejected") log(list[i] + ": " + (r.reason && r.reason.message || r.reason), "err"); });
      S.cycles++;
      if (P.running) { const posN = list.filter(s => W[s] && W[s].pos && W[s].pos.size > 0).length, spikes = list.filter(s => W[s] && W[s].isSpike).length;
        status = (P.strategy === "impulse" ? "🎯 ОХОТНИК" : P.fullAuto ? "🚀 ПОЛНЫЙ АВТОПИЛОТ" : P.strategy === "follow" ? "RUN АВТОПИЛОТ" : P.shukinMode ? "RUN ЩУКИН" : "RUN GRID") + " · " + list.length + " пар" + (posN ? " · POS×" + posN : "") + (spikes ? " · SPIKE×" + spikes : ""); }
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

  // ---------- 🎯 the impulse hunter ----------
  const HUNT = require("./hunter").create({ P, log, lsGet, lsSet, placeEntry: (b, i) => placeEntry(b, i), wsUrl: () => host.wsUrl ? host.wsUrl() : "", pub, priv, keys,
    getInstrument, getPosition, roundToStep, floorToStep, ensureLev, setLimitStop, cancelOurOrders, recordTrade,
    closeChase: (s2, c, why, opt) => closeChase(s2, c, why, opt), chasing: (s2) => chasing(s2),
    posIdx: async (s2, d) => { await shortIdx(s2, P.category); return HEDGE[s2] ? (d > 0 ? 1 : 2) : 0; },
    dayStopped: () => (P.maxDailyLoss || 0) > 0 && S.dayPnl <= -Math.abs(P.maxDailyLoss),
    skip: (s2) => s2 === (P.symbol || "").toUpperCase(),
    hasPosition: (s2) => { const w = W[s2]; return !!(w && ((w.pos && w.pos.size > 0) || (w.posEx && w.posEx.size > 0))); } });
  function syncHunt() {
    const want = !!(P.hunt && P.running && P.fullAuto);
    if (want && !HUNT.isOn()) HUNT.start(); else if (!want && HUNT.isOn()) HUNT.stop();
  }
  async function hunt() {
    if (P.hunt && HUNT.isOn()) { P.hunt = false; HUNT.stop(); P.strategy = "follow"; save(); return await stop(false); }
    if (P.liveEnabled && !keys()) return "Нет BYBIT_API_KEY / BYBIT_API_SECRET в Railway → Variables";
    Object.assign(P, { hunt: true, strategy: "impulse", fullAuto: false, autopilot: false, shukinMode: false }); save();
    if (P.liveEnabled) { const s0 = (P.symbol || "").toUpperCase(); if (s0 && !(W[s0] && W[s0].pos && W[s0].pos.size > 0)) await cancelOurOrders(s0, P.category).catch(() => {}); }
    HUNT.start();
    const r = P.running ? "бот уже работает" : await start();
    return `🎯 Охотник за импульсом запущен${P.liveEnabled ? "" : " (СИМУЛЯЦИЯ — Live выкл)"} — ${r}`;
  }

  // ---------- 🚀 full autopilot: the most volatile coin of the LAST 1 MINUTE ----------
  // score = the range of the last closed 1m candle + of the current one + |the 1-minute move| (all %), the volume spike as a tie-break
  async function scanLast1m(exclude) {
    const category = P.category, ex = new Set((exclude || []).map(s => s.toUpperCase()));
    const r = await pub("/v5/market/tickers", { category });
    const liq = (r.list || []).filter(t => t.symbol.endsWith("USDT") && parseFloat(t.turnover24h || 0) > 3e6 && !ex.has(t.symbol));
    const byMove = liq.slice().sort((a, b) => Math.abs(parseFloat(b.price24hPcnt || 0)) - Math.abs(parseFloat(a.price24hPcnt || 0))).slice(0, 50);
    const byTurn = liq.slice().sort((a, b) => parseFloat(b.turnover24h || 0) - parseFloat(a.turnover24h || 0)).slice(0, 30);
    const pool = [...new Map([...byMove, ...byTurn].map(t => [t.symbol, t])).values()];
    const out = [];
    for (let i = 0; i < pool.length; i += 10) {
      const part = await Promise.all(pool.slice(i, i + 10).map(async (coin) => {
        try { const k = await getKlines(coin.symbol, category, "1", 20); if (!k || k.length < 5) return null;
          const cur = k[k.length - 1], prev = k[k.length - 2];
          const rPrev = (prev.high - prev.low) / prev.close * 100, rCur = (cur.high - cur.low) / cur.close * 100;
          const mv1 = prev.open ? (cur.close - prev.open) / prev.open * 100 : 0, sp = volSpike(k.slice(0, -1), 12);
          const score = Math.max(rPrev, rCur) * 2 + Math.abs(mv1) * 1.5 + Math.min(sp, 6) * 0.1;
          return { symbol: coin.symbol, chg: mv1, chg24: parseFloat(coin.price24hPcnt || 0) * 100, rsi: calculateRSI(k, 14), turnover: parseFloat(coin.turnover24h || 0), scannedAt: now(), range1m: Math.max(rPrev, rCur), range1: rCur, range5: rangePct(k, 5), atrPct: 0, volSpike: sp, score, tf: "1" };
        } catch (e) { return null; } }));
      out.push(...part.filter(Boolean));
    }
    const found = out.sort((a, b) => b.score - a.score).slice(0, 12);
    S.targets = found; saveS();
    return found;
  }
  function faState() { if (!S.fa) S.fa = { sym: "", since: 0, lastPos: 0 }; return S.fa; }
  async function faPick(why, exclude, ready) {
    const found = ready || await scanLast1m(exclude);
    if (!found.length) { log("🚀 Автопилот: нет ликвидных пар с движением за 1 мин — повторю через минуту", "err"); return null; }
    const old = (P.symbol || "").toUpperCase(), neu = found[0].symbol, wo = W[old], hasPos = wo && wo.pos && wo.pos.size > 0;
    if (old && old !== neu && !hasPos && P.liveEnabled && keys()) { try { await cancelOurOrders(old, P.category); } catch (e) { log(`${old}: снять лимитки: ${e.message}`, "err"); } }
    P.symbol = neu; P.interval = "1"; save();
    const fa = faState(); fa.sym = neu; fa.since = now(); fa.at = now(); saveS();
    log(`🚀 Автопилот (${why}): лимитки → ${neu} · за 1 мин диапазон ${found[0].range1m.toFixed(2)}%, ход ${found[0].chg >= 0 ? "+" : ""}${found[0].chg.toFixed(2)}%${old && old !== neu ? (hasPos ? ` · по ${old} позиция — веду её до выхода` : ` · с ${old} лимитки сняты`) : ""}`, "ok");
    ensureWatch(neu); react(neu);
    return neu;
  }
  // ---------- 🧠 AI pilot (fal.ai): every faEveryMin minutes the AI gets the whole picture and decides
  // which coin to trade and how the strategy is tuned. Risk limits (order size, leverage, daily loss, live) stay the user's.
  const AI_KEYS = {   // what the AI may change, and the safe ranges
    bandPct: [0.3, 5], bandLevels: [1, 6], bandAdds: [0, 8], bandSl: [1.5, 15], bandTp: [0.3, 3], bandTrailGap: [0.1, 2], bandRef: [1, 20],
    momSl: [0.5, 5], pulsePct: [0.5, 5], pulseSec: [20, 180], momHoldMin: [2, 30], momAdds: [0, 4],
    fastK: [1.5, 5], fastAggr: [0.55, 0.85], fastSpeed: [1.2, 4], chaseAggrSec: [5, 60], faEveryMin: [1, 10],
    leverage: [1, 10], orderUsdt: [5, 5000],
    hScore: [4, 8], hAct: [0.15, 1.5], hGap: [0.1, 1], hStop: [0.3, 2], hTimeSec: [10, 120], hMax: [1, 4],
    pulseOn: "bool", smartOn: "bool", bandTrail: "bool", autoStep: "bool", hunt: "bool",
  };
  function aiApply(params) {
    const ch = [];
    for (const [k, v] of Object.entries(params || {})) {
      const rg = AI_KEYS[k]; if (!rg) continue;
      if (rg === "bool") { if (typeof v === "boolean" && P[k] !== v) { ch.push(`${k} ${P[k]}→${v}`); P[k] = v; } continue; }
      let n = Number(v); if (!Number.isFinite(n)) continue;
      n = Math.min(rg[1], Math.max(rg[0], n));
      if (["bandLevels", "bandAdds", "bandRef", "momAdds", "faEveryMin", "pulseSec", "momHoldMin", "chaseAggrSec", "leverage", "orderUsdt", "hScore", "hTimeSec", "hMax"].includes(k)) n = Math.round(n); else n = +n.toFixed(3);
      if (P[k] !== n) { ch.push(`${k} ${P[k]}→${n}`); P[k] = n; }
    }
    if (ch.length) save();
    syncHunt();
    return ch;
  }
  async function aiPilot(found, cur) {
    const cw = W[cur], fa = faState();
    const cands = found.slice(0, 8).map(t => ({ symbol: t.symbol, range1m: +t.range1m.toFixed(2), move1m: +t.chg.toFixed(2), range5m: +t.range5.toFixed(2), volX: +(t.volSpike || 1).toFixed(1), chg24: +(t.chg24 || 0).toFixed(1), turnoverM: Math.round((t.turnover || 0) / 1e6) }));
    const positions = Object.values(W).filter(w => w.pos && w.pos.size > 0).map(w => ({ symbol: w.symbol, side: w.pos.side, avg: w.pos.avgPrice, mid: w.mid, uPnl: +(w.pos.unrealisedPnl || 0).toFixed(3) }));
    const trades = S.history.slice(-20).map(h => ({ minAgo: Math.round((now() - h.at) / 60000), sym: h.sym, pnl: h.pnl, how: h.how }));
    const params = {}; for (const k of Object.keys(AI_KEYS)) params[k] = P[k];
    const steps = cw && cw.steps ? { maker: cw.steps.maker, taker: cw.steps.taker, minStepPct: cw.steps.lim } : null;
    await availUsdt(true);
    const hv = HUNT.view();
    const d = await host.fal(`You FULLY run a crypto futures bot on Bybit 24/7 (USDT perpetuals). Goal: the maximum profit; do not blow the account.
Account: equity ${AV.eq != null ? AV.eq.toFixed(2) : "?"} USDT, free ${AV.v != null ? AV.v.toFixed(2) : "?"} USDT. You set the leverage (1-10, now x${P.leverage}) and orderUsdt = the position size of ONE order in USDT (now ${P.orderUsdt}); every opening order is cut to the free money anyway. The only hard limit you cannot change: the day loss limit ${P.maxDailyLoss} USDT (then all stops till the next day).
ONLY limit orders are used (never market).
How the bot trades (only limit orders, never market): with no position it keeps SELL limits from +bandPct% above and BUY limits from -bandPct% below the average of the last bandRef 1m candles (bandLevels per side, each further by the fee step). A fill -> the other side is removed, adds every step beyond the average (bandAdds), a take: trailing from bandTp% with a pull-back of bandTrailGap% (bandTrail) or a limit take; the stop bandSl% from the average. Pulse: a move of pulsePct% within pulseSec s -> the position against it is cut, the bot enters with the move (stop momSl%), back to the limits after momHoldMin calm minutes. smartOn: the live tape catches a move at its start (fastK × usual move, fastAggr share of volume, fastSpeed × tape speed). Exits are limits chased after the price; after chaseAggrSec s a limit at the opposite best price. autoStep=true ignores bandTp/bandTrailGap and takes them from the fees.
Parallel impulse hunter (hunt=true/false): watches the order books + tape of ~25 liquid coins, 4x a second scores an explosion seconds ahead (book imbalance up to 2, pulled liquidity up to 2, tape speed + aggression up to 3, an early move 1; out of 8), score >= hScore -> enters by a limit at the opposite best price, a trailing exit by a limit from +hAct% with a pull-back hGap%, stop hStop%, out after hTimeSec s without the impulse, up to hMax at once. Its stats: ${JSON.stringify(hv.stats)}, active: ${JSON.stringify(hv.hunts.map(x => ({ sym: x.sym, d: x.d, prof: x.prof, sec: x.sec })))}.
Fees: ${JSON.stringify(steps)}. Day PnL ${S.dayPnl.toFixed(3)} USDT (daily loss limit ${P.maxDailyLoss}).
Now trading: ${cur} for ${fa.since ? Math.round((now() - fa.since) / 60000) : 0} min, its status "${cw ? cw.status : "—"}", our open orders ${cw ? cw.ourOrders : 0}.
Open positions: ${JSON.stringify(positions)}
Last trades (newest last): ${JSON.stringify(trades)}
Last backtest of the settings (on 1m history): ${S.ai ? JSON.stringify({ sym: S.ai.sym, minAgo: Math.round((now() - S.ai.at) / 60000), best: S.ai.best, current: S.ai.current, applied: S.ai.applied }) : "none"}
The most volatile coins of the LAST 1 MINUTE (range %, move %, 5m range %, volume × usual, 24h %, turnover M$):
${cands.map(c => JSON.stringify(c)).join("\n")}
Current settings: ${JSON.stringify(params)}
Decide: which coin to trade for the next ${P.faEveryMin} min (one of the list, or "${cur}" to stay; switching removes the limits of the old coin, an open position there is still led to its exit) and the settings (only keys you want to change, within sane ranges). Prefer coins whose volatility is steady and liquid over one-candle spikes; tune bandPct to the coin's real range so the limits get filled; widen the stop / cut adds after losses.
Answer ONE JSON: {"symbol": "XXXUSDT", "params": {...}, "reason": "коротко по-русски, почему"}`);
    let sym = String(d.symbol || "").toUpperCase().trim();
    if (sym !== cur && !found.some(t => t.symbol === sym)) sym = "";
    return { symbol: sym, params: d.params || {}, reason: String(d.reason || "").slice(0, 300) };
  }
  // the decision of one tick: the AI if fal is there, else the algorithm (the top-1)
  async function faDecide(why, exclude) {
    const cur = (P.symbol || "").toUpperCase(), fa = faState();
    const found = await scanLast1m(exclude);
    if (!found.length) { log("🚀 Автопилот: нет ликвидных пар с движением за 1 мин — повторю позже", "err"); return; }
    let target = found[0].symbol, by = "алгоритм (топ-1)";
    if (host.fal) {
      try {
        const d = await aiPilot(found, cur);
        const ch = aiApply(d.params);
        target = d.symbol || cur || target; by = "🧠 AI";   // a coin outside the list -> stay where it is
        S.faAi = { at: now(), symbol: target, reason: d.reason, changes: ch }; saveS();
        log(`🧠 AI-пилот: ${target === cur ? "остаюсь на " + cur : "монета → " + target}${ch.length ? " · настройки: " + ch.join(", ") : ""}${d.reason ? " — " + d.reason : ""}`, "ok");
      } catch (e) { log("🧠 AI-пилот (fal.ai) недоступен: " + e.message + " — решает алгоритм", "err"); }
    }
    if (target === cur && fa.sym === cur && fa.since) { if (by !== "🧠 AI") log(`🚀 Автопилот: ${cur} всё ещё самая волатильная за 1 мин — сетка остаётся`, "info"); return; }
    const t = found.find(x => x.symbol === target);
    const list = t ? [t, ...found.filter(x => x !== t)] : found;
    if (!t) { faState().since = now(); return; }   // stay on the current coin (it is not in the list any more)
    const neu = await faPick(`${why} · ${by}`, null, list);
    // a new coin -> the AI learning (backtest + fal) for it, not more often than every 10 min
    if (neu && neu !== cur && host.fal && !aiBusy && now() - ((S.ai && S.ai.at) || 0) > 10 * 60000) setTimeout(() => aiLearn("новая монета " + neu).catch(() => {}), 20000);
  }

  let faBusy = false;
  async function faTick() {
    if (!P.running || !P.fullAuto || faBusy) return;
    const fa = faState(), every = Math.max(1, P.faEveryMin || 3) * 60000;
    if (fa.sym && fa.since && now() - (fa.at || fa.since) < every - 2000) return;
    faBusy = true;
    try {
      const sym = (P.symbol || "").toUpperCase();
      fa.at = now();
      // every faEveryMin minutes: a fresh scan of the last 1 minute -> the AI (or the algorithm) decides the coin and the settings
      // (the old coin's limits are removed; a position there is still led to its exit)
      await faDecide(!fa.sym || fa.sym !== sym || !fa.since ? "старт" : "раз в " + Math.max(1, P.faEveryMin || 3) + " мин");
    } catch (e) { log("🚀 Автопилот: " + e.message, "err"); }
    finally { faBusy = false; saveS(); }
  }
  setInterval(() => { faTick().catch(() => {}); }, 5000);
  async function fullAuto() {
    if (!keys()) return "Нет BYBIT_API_KEY / BYBIT_API_SECRET в Railway → Variables";
    Object.assign(P, { fullAuto: true, strategy: "follow", momIoc: false, autopilot: false, liveEnabled: true, interval: "1", shukinMode: false, aiAuto: !!host.fal });
    save(); faState().since = 0;
    log(host.fal ? "🧠 Управление отдано AI (fal.ai): монета и все настройки стратегии — каждые " + Math.max(1, P.faEveryMin || 3) + " мин; размер ордера, плечо и дневной лимит убытка — ваши" : "⚠ Нет FAL_KEY в Railway → Variables — автопилот работает без AI (алгоритм: топ-1 по волатильности)", host.fal ? "ok" : "err");
    log(`🚀 ПОЛНЫЙ АВТОПИЛОТ: сетка лимиток только на самой волатильной за последнюю 1 мин монете; каждые ${Math.max(1, P.faEveryMin || 3)} мин — новый скан и перенос сетки на новый топ`, "ok");
    await faDecide("старт"); const sym = faState().sym;
    if (P.running) { runCycle(); return "Полный автопилот включён" + (sym ? " · " + sym : ""); }
    const r = await start(); syncHunt();
    return "🚀 Полный автопилот" + (host.fal ? " под управлением AI" : " (без AI: нет FAL_KEY)") + (sym ? " · " + sym : "") + " — " + r;
  }

  // ---------- autopilot: every autoScanMin minutes - scan, move the grid to the best target ----------
  let autoBusy = false;
  async function autopilot() {
    if (!P.running || !P.autopilot || P.fullAuto || autoBusy || now() - (S.lastAuto || 0) < Math.max(1, P.autoScanMin) * 60000) return;
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

  // ---------- ⚡ smart adaptation: the live tape decides at every trade ----------
  const TAPE = require("./tape").create({ url: () => host.wsUrl ? host.wsUrl() : "", onTrade: (sym) => onTrade(sym) });
  function typical1m(w) {   // the coin's usual 1-minute range, %
    const c = (w.candles || []).slice(-30); if (c.length < 5) return 0.3;
    const iv = Math.max(1, Number(P.interval) || 1);
    return c.reduce((a, k) => a + (k.high - k.low) / k.close * 100, 0) / c.length / Math.sqrt(iv);
  }
  function react(sym) {   // run the coin's cycle right now (not waiting for the 3 s loop)
    const w = W[sym]; if (!w || w.lock) { if (w) w.again = true; return; }
    processSymbol(sym).catch(e => log(sym + ": " + e.message, "err"));
  }
  const lastCheck = {};
  function onTrade(sym) {
    if (P.strategy !== "follow" || !P.running || !P.smartOn) return;
    if (now() - (lastCheck[sym] || 0) < 250) return; lastCheck[sym] = now();
    const w = W[sym]; if (!w) return;
    const f15 = TAPE.flow(sym, 15000); if (!f15) return;
    const typ = typical1m(w), thr = Math.max(P.fastMinPct, P.fastK * typ * 0.5), d = Math.sign(f15.mv), aggr = d > 0 ? f15.buyShare : 1 - f15.buyShare;
    w.tape = { mv15: f15.mv, buy: f15.buyShare, speedX: f15.speedX, thr: +thr.toFixed(3), typ: +typ.toFixed(3) };
    if (d && Math.abs(f15.mv) >= thr && aggr >= P.fastAggr && f15.speedX >= P.fastSpeed) {
      const isNew = !w.mom || w.mom.d !== d;
      w.mom = { d, at: now(), mv: f15.mv, fast: true };
      if (isNew) { log(`⚡ ${sym}: старт ${d > 0 ? "ПАМПА ▲" : "ДАМПА ▼"} по ленте — ${f15.mv >= 0 ? "+" : ""}${f15.mv}% за 15 с (обычно ~${(typ * 0.5).toFixed(2)}%), ${Math.round(aggr * 100)}% объёма в одну сторону, лента ×${f15.speedX} — реагирую сразу`, "ok"); react(sym); }
      else if (w.pos && w.pos.size && w.posMom) react(sym);   // the move goes on: maybe an add
    }
    // the move turns: a profitable position with the move is closed at once (not waiting for the trailing)
    if (w.pos && w.pos.size > 0 && w.posMom && !w.exitNow) {
      const pd = w.pos.side === "Buy" ? 1 : -1, f10 = TAPE.flow(sym, 10000);
      if (f10) { const against = pd > 0 ? 1 - f10.buyShare : f10.buyShare, prof = (f10.last / w.pos.avgPrice - 1) * 100 * pd;
        if (against >= P.fastAggr && -pd * f10.mv >= typ * 0.3 && prof > 0.1) { w.exitNow = { why: `лента развернулась: ${Math.round(against * 100)}% объёма против, ${f10.mv}% за 10 с`, prof: +prof.toFixed(2) }; react(sym); } }
    }
  }

  // ---------- 🧠 lightning learning: history of the coin -> thousands of settings -> the best on unseen data -> applied ----------
  let aiBusy = false;
  async function history1m(symbol, category, n) {
    let end = now(), out = [];
    while (out.length < n) {
      const r = await pub("/v5/market/kline", { category, symbol, interval: "1", limit: Math.min(1000, n - out.length), end });
      const rows = (r.list || []).map(x => ({ t: +x[0], open: +x[1], high: +x[2], low: +x[3], close: +x[4] }));
      if (!rows.length) break; out = rows.concat(out); end = rows[rows.length - 1].t - 1; if (rows.length < 200) break;
    }
    const seen = new Set(); return out.filter(x => !seen.has(x.t) && seen.add(x.t)).sort((a, b) => a.t - b.t);
  }
  const fmtP = (st, p) => st === "follow"
    ? `лимитки ±${p.bandPct}% × ${p.levels}, доборов ${p.adds}, трейлинг с ${p.tp}% отступ ${p.gap}%, стоп ${p.sl}%, пульс ${p.pulse ? p.pulse + "%" : "выкл"}`
    : `шаг ${p.step}%, уровней ${p.levels}, тейк ${p.tp}%, стоп ${p.sl}%`;
  async function aiLearn(why) {
    if (aiBusy) return "Обучение уже идёт"; aiBusy = true;
    const t0 = now(), symbol = (P.symbol || "BTCUSDT").toUpperCase(), category = P.category, st = P.strategy === "follow" ? "follow" : "sell";
    try {
      log(`🧠 Обучение (${why}): ${symbol}, ${st === "follow" ? "Автопилот за ценой" : "Sell Grid"} — гружу историю…`, "info");
      const c = await history1m(symbol, category, 3000);
      if (c.length < 500) throw new Error("мало истории (" + c.length + " свечей)");
      const steps = await stepsOf(symbol, category);
      const R = await OPT.optimize(c, P, steps, st);
      const ok = R.top.filter(x => x.enough);
      let pick = ok[0] || null, by = "оптимизатор", reason = "";
      // the AI (fal.ai) chooses among the candidates that held on unseen data
      if (host.fal && ok.length) {
        try {
          const live = S.history.slice(-30).map(h => ({ min: Math.round((now() - h.at) / 60000), sym: h.sym, pnl: h.pnl, how: h.how }));
          const d = await host.fal(`Strategy: ${st === "follow" ? "limits +-X% around the price, adds, trailing take, stop, pump/dump pulse" : "sell grid above the price, take/stop from the average"}. Coin ${symbol}, ${c.length} one-minute candles, fees maker ${steps.maker}% taker ${steps.taker}%.
Candidates (results in % of one order, with fees; train = first 70% of history, valid = last 30% never seen while choosing):
${ok.slice(0, 8).map((x, i) => `${i}: ${JSON.stringify(x.p)} train ${JSON.stringify(x.train)} valid ${JSON.stringify(x.valid)}`).join("\n")}
Current settings: ${JSON.stringify(R.current.p)} train ${JSON.stringify(R.current.train)} valid ${JSON.stringify(R.current.valid)}
Live trades of the bot (newest last): ${JSON.stringify(live)}
Pick the candidate most likely to make money next hours (robust on valid, enough trades, low drawdown), or -1 to keep the current settings.
Answer ONE JSON: {"choice": <index or -1>, "reason": "коротко по-русски"}`);
          const ch = Number(d.choice); reason = String(d.reason || "").slice(0, 300);
          if (ch === -1) { pick = null; by = "AI (fal.ai)"; } else if (ok[ch]) { pick = ok[ch]; by = "AI (fal.ai)"; }
        } catch (e) { log("🧠 AI (fal.ai) недоступен: " + e.message + " — выбирает оптимизатор", "err"); }
      }
      const curV = R.current.valid, curScore = curV.pnl - 0.5 * curV.dd;
      let applied = false;
      if (pick && pick.valid.pnl > 0 && pick.vscore > curScore + 0.5) {
        const p = pick.p;
        if (st === "follow") Object.assign(P, { autoStep: false, bandPct: p.bandPct, bandLevels: p.levels, bandAdds: p.adds, bandTp: p.tp, bandTrailGap: p.gap, bandSl: p.sl, pulseOn: p.pulse > 0, pulsePct: p.pulse || P.pulsePct });
        else Object.assign(P, { autoStep: false, gridStepPct: p.step, gridLevels: p.levels, tpPct: p.tp, slPct: p.sl });
        save(); applied = true;
      }
      S.ai = { at: now(), ms: now() - t0, why, sym: symbol, strategy: st, tested: R.tested, candles: c.length, by, reason, applied,
        best: pick && { p: fmtP(st, pick.p), train: pick.train, valid: pick.valid }, current: { p: fmtP(st, R.current.p), train: R.current.train, valid: R.current.valid } };
      saveS();
      const msg = applied ? `🧠 Обучено за ${((now() - t0) / 1000).toFixed(1)} с (${R.tested} вариантов, ${c.length} свечей): ${fmtP(st, pick.p)} — на непросмотренных данных ${pick.valid.pnl >= 0 ? "+" : ""}${pick.valid.pnl}% (${pick.valid.n} сделок) против ${curV.pnl >= 0 ? "+" : ""}${curV.pnl}% у прежних — ПРИМЕНЕНО (${by})${reason ? ": " + reason : ""}`
        : `🧠 Обучено за ${((now() - t0) / 1000).toFixed(1)} с (${R.tested} вариантов): ${pick ? `лучший вариант на непросмотренных данных ${pick.valid.pnl >= 0 ? "+" : ""}${pick.valid.pnl}% — не лучше текущих (${curV.pnl >= 0 ? "+" : ""}${curV.pnl}%) или не в плюсе` : "мало сделок в истории / AI решил оставить"} — настройки не меняю${reason ? " (" + reason + ")" : ""}`;
      log(msg, applied ? "ok" : "info");
      return msg;
    } catch (e) { log("🧠 Обучение: " + e.message, "err"); return "Ошибка: " + e.message; }
    finally { aiBusy = false; }
  }
  setInterval(() => { if (P.aiAuto && P.running && !aiBusy && now() - ((S.ai && S.ai.at) || 0) >= Math.max(5, P.aiEveryMin || 30) * 60000) aiLearn("автоматически").catch(() => {}); }, 60000);

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
    P.running = false; P.fullAuto = false; if (HUNT.isOn()) HUNT.stop(); P.hunt = false; save(); schedule(); status = "ВЫКЛ";
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
    if (["sell", "follow", "impulse"].includes(o.strategy) && o.strategy !== P.strategy) { P.strategy = o.strategy; ch.push("strategy"); }
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
  async function closePos(symbol) { symbol = (symbol || P.symbol).toUpperCase(); const pos = await getPosition(symbol, P.category); const w = ensureWatch(symbol); w.closingByBot = true; await cancelOurOrders(symbol, P.category); await closeChase(symbol, P.category, "ручное закрытие"); if (pos && pos.size > 0) recordTrade(pos.unrealisedPnl || 0, symbol, "ручное закрытие"); return pos.size ? "Закрываю лимиткой с догонкой (без маркета)" : "Позиции нет"; }
  async function position(symbol) { return getPosition((symbol || P.symbol).toUpperCase(), P.category); }
  function clearTargets() { S.targets = []; saveS(); log("Список целей очищен", "info"); return "Очищено"; }
  async function sync() { await refreshOpenUniverse(); for (const s of Object.keys(W)) { try { const w = W[s]; w.candles = await getKlines(s, P.category, P.interval, 40); if (w.candles.length) { w.mid = w.candles[w.candles.length - 1].close; w.rsi = calculateRSI(w.candles, 14); } if (w.posEx) w.pos = w.posEx; } catch (e) { /* skip */ } } log("Синхр биржи: " + Object.keys(W).length + " монет", "ok"); return Object.keys(W).length + " активных монет"; }

  function view() {
    ensureDayKey();
    const fa = S.fa || {};
    return { hunt: HUNT.view(), fa: P.fullAuto && P.running ? { ai: S.faAi || null, sym: fa.sym, left: fa.at ? Math.max(0, Math.round((Math.max(1, P.faEveryMin || 3) * 60000 - (now() - fa.at)) / 1000)) : null } : null, ai: S.ai || null, aiBusy, aiFal: !!host.fal, autoNext: P.running && P.autopilot ? Math.max(0, Math.round(((S.lastAuto || 0) + Math.max(1, P.autoScanMin) * 60000 - now()) / 60000)) : null, params: P, status, conn, cycles: S.cycles, trades: S.trades, wins: S.wins, losses: S.losses, dayPnl: +S.dayPnl.toFixed(4), targets: S.targets, history: S.history.slice(-50).reverse(),
      shukin: shukinSig, log: LOG.slice(0, 150),
      watches: Object.values(W).map(w => ({ symbol: w.symbol, mid: w.mid, pos: w.pos && w.pos.size ? w.pos : (w.posEx || null), orders: w.orders, ourOrders: w.ourOrders, isSpike: w.isSpike, volOk: w.volOk, status: w.status, atrPct: w.atrPct, rsi: w.rsi, tape: w.tape || null, mom: w.mom || null, steps: w.steps || null, band: w.band || null, levels: w.levels || null, hl: HUNT.levels(w.symbol), open: w.open || null, sl: w.slSet ? parseFloat(String(w.slSet).split("|")[0]) : null, targets: w.targets || [], side: w.side || (P.strategy === "sell" ? -1 : 0), dir: w.dir || null, spark: (w.candles || []).slice(-40).map(c => c.close), sig: w.sig || null })) };
  }
  if (P.running) setTimeout(() => { log("Сервер перезапущен — бот продолжает работу", "ok"); runCycle(); schedule(); if (!P.fullAuto || P.strategy !== "follow") { log("Остался только полный автопилот под управлением AI — перевожу бота в него", "ok"); fullAuto().catch(e => log("Автопилот: " + e.message, "err")); } else syncHunt(); }, 3000);
  return { hunt, fills, fullAuto, aiLearn, start, stop, runCycle, setParams, toggleShukin, scanPump, scanUltraVol1m, closePos, position, clearTargets, sync, view, params: P, analyzeShukinContext, getKlines, getOpenInterest };
}
module.exports = { create };
