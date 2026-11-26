// ЩУКИН · МИКРО-ХОДЫ — серверный движок, торгует 24/7. ТОЛЬКО PostOnly-лимитки (всегда мейкер), маркета нет вообще.
//  • раз в минуту: скан USDT-перпов Bybit — самые «бешеные» монеты (крупные минутные свечи), оборот от minTurn млн $, узкий спред;
//  • у каждой монеты на взводе стоят ДВЕ лимитки PostOnly: BUY ниже цены и SELL выше цены (на off % от середины стакана),
//    каждые 3 с переставляются за ценой — куда бы цена ни дёрнулась, одна из них исполнится;
//  • исполнилась одна — вторая снимается, сразу ставится тейк лимиткой PostOnly на +tp % от входа;
//  • ВЫХОД ТОЛЬКО В ПЛЮС: стопа нет ни у бота, ни на бирже. Лимитка выхода никогда не ниже входа + комиссии;
//  • держим дольше maxHold мин — тейк опускается до минимального плюса (вход + комиссии), ждёт возврата цены;
//  • 📉 фильтр OI (опция): ставить вход, только если открытый интерес за 15 мин растёт;
//  • ⚡ турбо: ближе вход, короче тейк; само выключается при минусе 3 % депозита.
"use strict";
const fs = require("fs");
const path = require("path");

const DEF = { live: false, usdt: 5, lev: 10, maxPos: 3, maxCoins: 6, minTurn: 5, maxCand: 60, lookback: 15, minAtr: 0.25, maxSpread: 0.08, off: 0.1, tp: 0.35, sl: 0.6, maxHold: 15, oiOn: false, oiPct: 0, turbo: false, turboSave: null, turboFrom: 0 };
const TURBO = { off: 0.05, tp: 0.2, maxHold: 5 };
const TURBO_LOSS_PCT = 3;
const NUMS = ["usdt", "lev", "maxPos", "maxCoins", "minTurn", "maxCand", "lookback", "minAtr", "maxSpread", "off", "tp", "maxHold", "oiPct"];
const FEE = 0.02;   // мейкер Bybit, % за сторону
const MIN_PLUS = 2.5 * FEE;   // минимальный плюс выхода, % от входа: покрывает комиссии входа и выхода

function create(host) {
  const { bx, lsGet, lsSet, keys } = host;
  const now = () => Date.now(), sleep = (ms) => new Promise(r => setTimeout(r, ms));
  // ---------------- 📒 журнал ----------------
  const JDIR = path.join(host.dataDir || ".", "journal");
  try { fs.mkdirSync(JDIR, { recursive: true }); } catch (e) { /* read-only */ }
  const dayOf = (t) => new Date(t || Date.now()).toISOString().slice(0, 10);
  function jlog(type, data) { fs.appendFile(path.join(JDIR, dayOf() + ".jsonl"), JSON.stringify(Object.assign({ t: now(), type, live: isLive() }, data)) + "\n", () => {}); }

  const prev = lsGet("bo.cfg", {});
  const CFG = Object.assign({}, DEF, lsGet("mk.cfg", null) || { live: !!prev.live, usdt: +prev.usdt || DEF.usdt, lev: +prev.lev || DEF.lev, minTurn: prev.minTurn != null ? +prev.minTurn : DEF.minTurn, oiOn: !!prev.oiOn, oiPct: +prev.oiPct || 0 });
  const saveCfg = () => lsSet("mk.cfg", CFG);
  saveCfg();
  const STAT = Object.assign({ scans: 0, entries: 0, wins: 0, losses: 0, since: 0, realized: 0, closed: 0, realDone: 0, closedDone: 0, doneUntil: 0 }, lsGet("bo.stat", {}));
  const saveStat = () => lsSet("bo.stat", STAT);
  const OURS = new Set(lsGet("bo.syms", []));
  const saveOurs = () => lsSet("bo.syms", [...OURS]);
  const TRADES = lsGet("mk.trades", {});         // "SYM|Buy" -> открытая сделка
  const saveTrades = () => lsSet("mk.trades", TRADES);
  const ENTRY = lsGet("mk.entry", {});           // sym -> { Buy: {id, px, qty}, Sell: {...} }
  const saveEntry = () => lsSet("mk.entry", ENTRY);
  let running = !!lsGet("bo.running", false);
  const LOG = lsGet("bo.log", []);
  let logSaved = 0;
  function log(msg, cls) {
    LOG.unshift([now(), String(msg), cls || "info"]); LOG.length = Math.min(LOG.length, 400);
    if (now() - logSaved > 2000) { logSaved = now(); lsSet("bo.log", LOG); }
    host.log && host.log(msg);
  }

  // ---------------- Bybit ----------------
  const pub = (p, q) => bx(p, { query: q });
  async function priv(method, p, params) {
    const call = () => bx(p, { method, query: method === "GET" ? params : null, body: method === "POST" ? params : null, auth: true });
    try { return await call(); } catch (e) { if (e.code === 10006) { await sleep(1500); return call(); } throw e; }
  }
  let chain = Promise.resolve();   // ордера по одному (~8/с)
  function enqueue(fn) { const p = chain.then(fn); chain = p.catch(() => {}).then(() => sleep(120)); return p; }
  const post = (p, params) => enqueue(() => priv("POST", p, params));
  const INST = {};
  async function inst(sym) {
    if (INST[sym]) return INST[sym];
    const r = await pub("/v5/market/instruments-info", { category: "linear", symbol: sym });
    const x = (r.list || [])[0]; if (!x) throw new Error("нет инструмента " + sym);
    return (INST[sym] = { tick: x.priceFilter.tickSize, step: x.lotSizeFilter.qtyStep, minQty: +x.lotSizeFilter.minOrderQty, minNot: +(x.lotSizeFilter.minNotionalValue || 5), maxLev: +(x.leverageFilter && x.leverageFilter.maxLeverage) || 50 });
  }
  const decs = (s) => (String(s).split(".")[1] || "").length;
  function rnd(v, step, mode) { const s = +step, f = mode === "up" ? Math.ceil : mode === "down" ? Math.floor : Math.round; return (f(v / s - (mode === "up" ? 1e-9 : -1e-9)) * s).toFixed(decs(step)); }
  function qtyFor(i, px) {
    let q = +rnd(CFG.usdt * CFG.lev / px, i.step, "down");
    if (q < i.minQty) q = i.minQty;
    if (q * px < i.minNot) q = +rnd(i.minNot * 1.02 / px, i.step, "up");
    return q;
  }
  const fmt = (v) => v == null || !Number.isFinite(+v) ? "—" : String(+(+v).toPrecision(6));
  const isLive = () => !!(CFG.live && keys());
  const word = (side) => (side === "Buy" ? "лонг" : "шорт");
  const short = (s) => s.replace(/USDT$/, "");
  const linkId = (k) => "mk_" + k + now() + Math.floor(Math.random() * 1e4);

  // ---------------- состояние ----------------
  const C = {};   // sym -> { hedge, lev, last, bid, ask }
  const coin = (sym) => C[sym] || (C[sym] = { sym, hedge: null, lev: false, last: 0, bid: 0, ask: 0 });
  let POS = {}, BUSY_SYMS = new Set(), OPEN = new Map();   // OPEN: orderId -> ордер с биржи
  let ARMED = [], CAND = [], scanning = false, fastBusy = false, scanTimer = null, fastTimer = null, lastScanAt = 0, ACC = { eq: 0, avail: 0 };
  const posOf = (sym) => Object.values(POS).filter(p => p.sym === sym);
  const ourOpen = () => Object.keys(TRADES).length;
  async function hedgeOf(sym) {
    const c = coin(sym);
    if (c.hedge == null) { try { const r = await priv("GET", "/v5/position/list", { category: "linear", symbol: sym }); c.hedge = (r.list || []).some(p => +p.positionIdx > 0); } catch (e) { c.hedge = false; } }
    return c.hedge;
  }
  const idxFor = (c, side) => (c.hedge ? (side === "Buy" ? 1 : 2) : 0);
  async function account() {
    try { const w = await priv("GET", "/v5/account/wallet-balance", { accountType: "UNIFIED" }); const a = (w.list || [])[0] || {}; ACC = { eq: +a.totalEquity || 0, avail: +a.totalAvailableBalance || 0 }; } catch (e) { /* прошлое значение */ }
    return ACC;
  }
  const canOpen = () => ourOpen() < Math.round(CFG.maxPos) && (!isLive() || ACC.avail >= CFG.usdt * 1.1);

  // ---------------- 1) скан: какие монеты держать на взводе ----------------
  async function scan() {
    if (scanning || !running) return; scanning = true;
    try {
      const t = await pub("/v5/market/tickers", { category: "linear" });
      const mv = (x) => Math.max(Math.abs(+x.price24hPcnt || 0) * 100 / 4, +x.prevPrice1h > 0 ? Math.abs(+x.lastPrice / +x.prevPrice1h - 1) * 100 : 0);
      const spr = (x) => +x.bid1Price > 0 ? (+x.ask1Price / +x.bid1Price - 1) * 100 : 99;
      const pool = (t.list || []).filter(x => x.symbol.endsWith("USDT") && +x.turnover24h >= CFG.minTurn * 1e6 && +x.lastPrice > 0)
        .sort((a, b) => mv(b) - mv(a)).slice(0, Math.max(10, Math.round(CFG.maxCand)));
      const out = [];
      for (let i = 0; i < pool.length; i += 10) {
        const part = await Promise.all(pool.slice(i, i + 10).map(async (x) => {
          try {
            const k = await pub("/v5/market/kline", { category: "linear", symbol: x.symbol, interval: "1", limit: Math.round(CFG.lookback) + 1 });
            const rows = (k.list || []).slice(1).map(r => ({ h: +r[2], l: +r[3] }));
            if (rows.length < Math.round(CFG.lookback)) return null;
            const atr = rows.reduce((a, r) => a + (r.h / r.l - 1) * 100, 0) / rows.length;
            const hi = Math.max(...rows.map(r => r.h)), lo = Math.min(...rows.map(r => r.l)), last = +x.lastPrice;
            let oi = null;
            if (CFG.oiOn) { try { const o = await pub("/v5/market/open-interest", { category: "linear", symbol: x.symbol, intervalTime: "5min", limit: 4 }); const l = (o.list || []).map(r => +r.openInterest); if (l.length >= 2 && l[l.length - 1] > 0) oi = (l[0] / l[l.length - 1] - 1) * 100; } catch (e) { /* нет данных */ } }
            return { sym: x.symbol, hi, lo, range: (hi / lo - 1) * 100, atr, spread: spr(x), last, ch1h: +x.prevPrice1h > 0 ? (last / +x.prevPrice1h - 1) * 100 : 0, turn: +x.turnover24h, oi };
          } catch (e) { return null; }
        }));
        out.push(...part.filter(Boolean));
      }
      for (const x of out) coin(x.sym).last = x.last;
      const oiOK = (x) => !CFG.oiOn || (x.oi != null && x.oi > 0 && x.oi >= Math.max(0, CFG.oiPct || 0));
      // монета годится: «бешеная» (минутная свеча ≥ minAtr) и минутная свеча больше тейка — дрожит достаточно; спред узкий
      const why = (x) => x.atr < CFG.minAtr ? "спокойная" : x.atr < CFG.tp ? "свеча < тейка" : x.spread > CFG.maxSpread ? "спред" : !oiOK(x) ? "OI" : "";
      const good = out.filter(x => !why(x)).sort((a, b) => b.atr - a.atr);
      if (isLive()) await account();
      turboGuard(ACC.eq);
      const money = !isLive() || ACC.avail >= CFG.usdt * 1.1;
      if (!money && !scan.noMoney) log(`Свободной маржи ${ACC.avail.toFixed(2)} USDT меньше залога на сделку ${CFG.usdt} — новые входы не ставлю`, "err");
      scan.noMoney = !money;
      const next = good.filter(x => !BUSY_SYMS.has(x.sym) || TRADES[x.sym + "|Buy"] || TRADES[x.sym + "|Sell"]).slice(0, Math.round(CFG.maxCoins)).map(x => x.sym);
      const removed = ARMED.filter(s => !next.includes(s)), added = next.filter(s => !ARMED.includes(s));
      ARMED = next;
      CAND = out.sort((a, b) => b.atr - a.atr).slice(0, 15).map(x => ({ sym: x.sym, range: x.range, atr: x.atr, spread: x.spread, hi: x.hi, lo: x.lo, last: x.last, ch1h: x.ch1h, oi: x.oi, turn: x.turn, armed: next.includes(x.sym), why: next.includes(x.sym) ? "" : why(x) || (BUSY_SYMS.has(x.sym) ? "чужая позиция" : "лимит") }));
      STAT.scans++; lastScanAt = now(); saveStat();
      jlog("scan", { eq: ACC.eq, armed: next, open: Object.keys(TRADES), top: out.slice(0, 10).map(x => [x.sym, +x.atr.toFixed(2), +x.spread.toFixed(3), +x.ch1h.toFixed(2)]) });
      if (added.length || removed.length) log(`Скан: на взводе ${next.length} монет → ${next.map(short).join(", ") || "—"}${added.length ? " · + " + added.map(short).join(", ") : ""}${removed.length ? " · − " + removed.map(short).join(", ") : ""}`, "ok");
      for (const s of removed) await dropEntry(s);
    } catch (e) { log("Скан: " + e.message, "err"); }
    finally { scanning = false; }
  }

  // ---------------- 2) входы: две PostOnly-лимитки вокруг цены, едут за ценой ----------------
  function targets(sym, i) {
    const c = coin(sym); if (!(c.bid > 0 && c.ask > 0)) return null;
    const mid = (c.bid + c.ask) / 2;
    return { Buy: +rnd(Math.min(c.bid, mid * (1 - CFG.off / 100)), i.tick, "down"), Sell: +rnd(Math.max(c.ask, mid * (1 + CFG.off / 100)), i.tick, "up") };
  }
  async function quote(sym) {
    const i = await inst(sym), c = coin(sym), T = targets(sym, i); if (!T) return;
    const e = ENTRY[sym] || (ENTRY[sym] = {});
    if (!isLive()) { e.Buy = { px: T.Buy }; e.Sell = { px: T.Sell }; return; }
    OURS.add(sym); saveOurs();
    await hedgeOf(sym);
    if (!c.lev) { const lev = String(Math.min(CFG.lev, i.maxLev)); try { await post("/v5/position/set-leverage", { category: "linear", symbol: sym, buyLeverage: lev, sellLeverage: lev }); } catch (er) { if (er.code !== 110043) log(`${short(sym)}: плечо: ${er.message}`, "err"); } c.lev = true; }
    const drift = Math.max(+i.tick, ((T.Buy + T.Sell) / 2) * Math.max(0.02, CFG.off / 3) / 100);
    for (const side of ["Buy", "Sell"]) {
      const px = T[side], o = e[side];
      if (o && o.id && !OPEN.has(o.id)) e[side] = null;   // исполнилась или биржа сняла PostOnly (цена прошла сквозь)
      const cur = e[side];
      if (cur && cur.id) {
        if (Math.abs(cur.px - px) < drift) continue;
        try { await post("/v5/order/amend", { category: "linear", symbol: sym, orderId: cur.id, price: String(px) }); cur.px = px; }
        catch (er) { await cancel(sym, cur.id); e[side] = null; }
        continue;
      }
      const qty = qtyFor(i, px);
      try {
        const r = await post("/v5/order/create", { category: "linear", symbol: sym, side, orderType: "Limit", price: String(px), timeInForce: "PostOnly", qty: String(qty), positionIdx: idxFor(c, side), orderLinkId: linkId("e") });
        e[side] = { id: r.orderId, px, qty };
        OPEN.set(r.orderId, { orderId: r.orderId });   // до следующего опроса считаю живой
      } catch (er) {
        if (er.code === 10001 && /position idx/i.test(er.message || "")) { c.hedge = !c.hedge; log(`${short(sym)}: режим позиций Bybit другой — переключаюсь`, "info"); }
        else log(`${short(sym)}: вход ${side === "Buy" ? "BUY" : "SELL"} @ ${px}: ${er.message}`, "err");
      }
    }
    saveEntry();
  }
  async function cancel(sym, id) { try { await post("/v5/order/cancel", { category: "linear", symbol: sym, orderId: id }); } catch (e) { /* уже нет */ } OPEN.delete(id); }
  async function dropEntry(sym) {
    const e = ENTRY[sym]; if (!e) return;
    for (const side of ["Buy", "Sell"]) if (e[side]) { if (e[side].id && isLive()) await cancel(sym, e[side].id); e[side] = null; }
    delete ENTRY[sym]; saveEntry();
  }

  // ---------------- 3) позиции: тейк / мин. плюс — одна PostOnly-лимитка выхода ----------------
  async function pollOrders() {
    const m = new Map(); let cursor;
    for (let page = 0; page < 5; page++) {
      const r = await priv("GET", "/v5/order/realtime", { category: "linear", settleCoin: "USDT", limit: 50, cursor });
      for (const o of r.list || []) m.set(o.orderId, o);
      cursor = r.nextPageCursor; if (!cursor) break;
    }
    OPEN = m;
  }
  async function pollPositions() {
    const r = await priv("GET", "/v5/position/list", { category: "linear", settleCoin: "USDT" });
    const seen = {}, busy = new Set();
    for (const p of r.list || []) {
      if (!(+p.size > 0)) continue;
      busy.add(p.symbol);
      if (!OURS.has(p.symbol)) continue;
      seen[p.symbol + "|" + p.side] = { sym: p.symbol, side: p.side, size: +p.size, avg: +p.avgPrice, upnl: +p.unrealisedPnl || 0, idx: +p.positionIdx || 0, mark: +p.markPrice || 0 };
    }
    POS = seen; BUSY_SYMS = busy;
    for (const [key, p] of Object.entries(seen)) {
      let tr = TRADES[key];
      if (!tr) {
        tr = TRADES[key] = { sym: p.sym, side: p.side, open: now(), entry: p.avg, qty: p.size, maxPx: p.mark, minPx: p.mark, mode: "тейк", cfg: { off: CFG.off, tp: CFG.tp, maxHold: CFG.maxHold, turbo: !!CFG.turbo } };
        STAT.entries++; saveStat(); saveTrades();
        log(`${short(p.sym)}: ${p.side === "Buy" ? "🟢 лонг" : "🔴 шорт"} ${p.size} @ ${fmt(p.avg)} — тейк +${CFG.tp}% лимиткой, выход только в плюс`, "ok");
        jlog("entry", { sym: p.sym, side: p.side, px: p.avg, qty: p.size });
        await dropEntry(p.sym);   // вторую лимитку входа снимаю
      }
      tr.entry = p.avg; tr.qty = p.size;
      const px = coin(p.sym).last || p.mark;
      tr.maxPx = Math.max(tr.maxPx, px); tr.minPx = Math.min(tr.minPx, px);
      if (!tr.slOff && isLive()) await noStop(p, tr);
      await manage(p, tr);
    }
    for (const key of Object.keys(TRADES)) if (!seen[key]) await closeTrade(key);
  }
  // выход только в плюс: стоп на бирже по позиции снимаю (если остался от старой версии)
  async function noStop(p, tr) {
    try { await post("/v5/position/trading-stop", { category: "linear", symbol: p.sym, tpslMode: "Full", stopLoss: "0", positionIdx: p.idx }); } catch (e) { /* стопа и не было */ }
    try { const o = (await priv("GET", "/v5/order/realtime", { category: "linear", symbol: p.sym, orderFilter: "StopOrder" })).list || []; for (const x of o) if (x.reduceOnly || /Stop/i.test(x.stopOrderType || "")) await cancel(p.sym, x.orderId); } catch (e) { /* нет */ }
    tr.slOff = true; saveTrades();
  }
  // цена лимитки выхода: тейк, а после maxHold мин — минимальный плюс. НИКОГДА не хуже входа + комиссии.
  function exitPrice(p, tr, i) {
    const L = p.side === "Buy", d = L ? 1 : -1, c = coin(p.sym), held = (now() - tr.open) / 60000;
    if (tr.mode !== "мин. плюс" && held >= CFG.maxHold) { tr.mode = "мин. плюс"; log(`${short(p.sym)}: ⏱ держу ${held.toFixed(0)} мин — тейк опускаю до минимального плюса +${MIN_PLUS}% (жду возврата цены, в минус не выхожу)`, "info"); saveTrades(); }
    const goal = tr.entry * (1 + d * (tr.mode === "мин. плюс" ? MIN_PLUS : CFG.tp) / 100);
    const best = L ? c.ask : c.bid;   // цена ушла дальше тейка — выхожу по лучшей мейкерской цене, но не хуже цели
    const want = best ? (L ? Math.max(goal, best) : Math.min(goal, best)) : goal;
    return rnd(want, i.tick, L ? "up" : "down");
  }
  async function manage(p, tr) {
    if (!isLive()) return;
    const i = await inst(p.sym), L = p.side === "Buy";
    const price = exitPrice(p, tr, i);
    const o = tr.exitId && OPEN.get(tr.exitId);
    if (o) {
      const left = o.leavesQty != null ? +o.leavesQty : +o.qty;
      if (Math.abs(left - p.size) > 1e-12) { await cancel(p.sym, tr.exitId); tr.exitId = null; }   // позиция выросла / частично закрылась — переставляю на весь размер
      else if (Math.abs(+price - tr.exitPx) < +i.tick / 2) return;
      else { try { await post("/v5/order/amend", { category: "linear", symbol: p.sym, orderId: tr.exitId, price }); tr.exitPx = +price; saveTrades(); return; } catch (e) { await cancel(p.sym, tr.exitId); tr.exitId = null; } }
    }
    try {
      const r = await post("/v5/order/create", { category: "linear", symbol: p.sym, side: L ? "Sell" : "Buy", orderType: "Limit", qty: String(p.size), price, timeInForce: "PostOnly", reduceOnly: true, positionIdx: p.idx, orderLinkId: linkId("x") });
      tr.exitId = r.orderId; tr.exitPx = +price; OPEN.set(r.orderId, { orderId: r.orderId, qty: String(p.size), leavesQty: String(p.size) }); saveTrades();
    } catch (e) { log(`${short(p.sym)}: лимитка выхода: ${e.message}`, "err"); }
  }
  async function closeTrade(key) {
    const tr = TRADES[key]; if (!tr) return;
    let pnl = null;
    try { const r = await priv("GET", "/v5/position/closed-pnl", { category: "linear", symbol: tr.sym, startTime: tr.open - 60000, limit: 50 }); pnl = (r.list || []).filter(x => (x.side === "Buy" ? "Sell" : "Buy") === tr.side || !x.side).reduce((a, x) => a + (+x.closedPnl || 0), 0); } catch (e) { pnl = null; }
    try { const o = (await priv("GET", "/v5/order/realtime", { category: "linear", symbol: tr.sym })).list || []; for (const x of o) if (x.reduceOnly && x.side === (tr.side === "Buy" ? "Sell" : "Buy")) await cancel(tr.sym, x.orderId); } catch (e) { /* ничего */ }
    const hold = (now() - tr.open) / 60000, L = tr.side === "Buy";
    if (pnl != null) { if (pnl > 0) STAT.wins++; else STAT.losses++; }
    log(`${short(tr.sym)}: ${pnl == null ? "позиция закрыта" : pnl > 0 ? "✅ +" + pnl.toFixed(3) + " USDT" : "🛑 " + pnl.toFixed(3) + " USDT"} (${word(tr.side)}, ${tr.mode}, ${hold.toFixed(1)} мин)`, pnl > 0 ? "ok" : "err");
    jlog("trade", Object.assign({}, tr, { close: now(), holdMin: +hold.toFixed(1), best: +((L ? tr.maxPx / tr.entry - 1 : 1 - tr.minPx / tr.entry) * 100).toFixed(2), adverse: +((L ? 1 - tr.minPx / tr.entry : tr.maxPx / tr.entry - 1) * 100).toFixed(2), pnl: pnl == null ? null : +pnl.toFixed(4) }));
    delete TRADES[key]; saveTrades(); saveStat();
  }

  // ---------------- заработано с запуска ----------------
  let realAt = 0;
  async function refreshRealized() {
    if (!STAT.since || now() - realAt < 30000) return; realAt = now();
    const W = 7 * 864e5 - 60000;
    if (!STAT.doneUntil) STAT.doneUntil = STAT.since;
    const sumWin = async (from, to) => {
      let sum = 0, n = 0, cursor;
      for (let page = 0; page < 20; page++) {
        const r = await priv("GET", "/v5/position/closed-pnl", { category: "linear", startTime: from, endTime: to, limit: 100, cursor });
        for (const x of r.list || []) if (OURS.has(x.symbol)) { sum += +x.closedPnl || 0; n++; }
        cursor = r.nextPageCursor; if (!cursor) break;
      }
      return { sum, n };
    };
    try {
      while (now() - STAT.doneUntil > W + 3600e3) { const w = await sumWin(STAT.doneUntil, STAT.doneUntil + W); STAT.realDone += w.sum; STAT.closedDone += w.n; STAT.doneUntil += W; }
      const cur = await sumWin(STAT.doneUntil, now());
      STAT.realized = +(STAT.realDone + cur.sum).toFixed(4); STAT.closed = STAT.closedDone + cur.n; saveStat();
    } catch (e) { log("PnL с запуска: " + e.message, "err"); }
  }

  // ---------------- циклы ----------------
  async function lastPrices() {
    const t = await pub("/v5/market/tickers", { category: "linear" });
    for (const x of t.list || []) if (C[x.symbol] || ARMED.includes(x.symbol)) { const c = coin(x.symbol); c.last = +x.lastPrice; c.bid = +x.bid1Price; c.ask = +x.ask1Price; }
  }
  async function fast() {
    if (fastBusy) return; fastBusy = true;
    try {
      await lastPrices();
      if (isLive()) {
        await pollOrders();
        await pollPositions();
        if (running) {
          const ok = canOpen();
          for (const s of ARMED) {
            if (posOf(s).length || BUSY_SYMS.has(s)) { if (ENTRY[s]) await dropEntry(s); continue; }
            if (!ok) { if (ENTRY[s]) await dropEntry(s); continue; }
            try { await quote(s); } catch (e) { log(`${short(s)}: вход: ${e.message}`, "err"); }
          }
        }
        await refreshRealized();
      } else if (running) for (const s of ARMED) { try { await quote(s); } catch (e) { /* следующий раз */ } }
    } catch (e) { log("Опрос: " + e.message, "err"); }
    finally { fastBusy = false; }
  }
  function schedule() {
    clearInterval(scanTimer); clearInterval(fastTimer); scanTimer = null;
    fastTimer = setInterval(() => { fast().catch(() => {}); }, 3000);   // позиции ведём всегда, даже после СТОП
    if (running) scanTimer = setInterval(() => { scan().catch(() => {}); }, 60000);
  }

  // ---------------- команды ----------------
  function setCfg(o) {
    const ch = [];
    if (CFG.turbo && !o._turbo) { o = Object.assign({}, o); for (const k of Object.keys(TURBO)) delete o[k]; }
    for (const k of NUMS) if (k in DEF && o[k] != null && o[k] !== "" && Number.isFinite(Number(o[k])) && Number(o[k]) !== CFG[k]) { CFG[k] = Number(o[k]); ch.push(k); }
    CFG.usdt = Math.max(1, CFG.usdt); CFG.lev = Math.max(1, Math.min(50, Math.round(CFG.lev))); CFG.maxPos = Math.max(1, Math.min(30, Math.round(CFG.maxPos))); CFG.maxCoins = Math.max(1, Math.min(20, Math.round(CFG.maxCoins)));
    CFG.lookback = Math.max(3, Math.min(120, Math.round(CFG.lookback))); CFG.minAtr = Math.max(0, Math.min(5, +CFG.minAtr || 0)); CFG.maxSpread = Math.max(0.005, Math.min(1, +CFG.maxSpread || 0.08));
    CFG.off = Math.max(0, Math.min(2, +CFG.off || 0)); CFG.tp = Math.max(0.06, Math.min(5, +CFG.tp || 0.35)); CFG.maxHold = Math.max(1, Math.min(600, +CFG.maxHold || 15)); CFG.oiPct = Math.max(0, Math.min(50, +CFG.oiPct || 0));
    if (ch.includes("lev")) for (const c of Object.values(C)) c.lev = false;
    if (typeof o.live === "boolean" && o.live !== CFG.live) { CFG.live = o.live; ch.push("live"); log(CFG.live ? "⚠ LIVE включён: реальные ордера" : "LIVE выключен: только сигналы", CFG.live ? "err" : "info"); }
    if (typeof o.oiOn === "boolean" && o.oiOn !== CFG.oiOn) { CFG.oiOn = o.oiOn; ch.push("oiOn"); log(CFG.oiOn ? `📉 Фильтр OI ВКЛ: вход только если открытый интерес за 15 мин растёт${CFG.oiPct > 0 ? " на " + CFG.oiPct + "% и больше" : ""}` : "📉 Фильтр OI ВЫКЛ", "info"); }
    saveCfg();
    if (ch.length) jlog("cfg", { changed: ch, cfg: CFG });
    return ch;
  }
  function turboPnl() {
    if (!CFG.turboFrom) return 0;
    let sum = 0; for (let t = CFG.turboFrom; t <= now() + 864e5; t += 864e5) { const d = readDay(dayOf(t)); if (d) for (const x of d.trades) if ((x.close || 0) >= CFG.turboFrom) sum += +x.pnl || 0; }
    return +sum.toFixed(4);
  }
  function setTurbo(on, why) {
    if (!!on === !!CFG.turbo) return CFG.turbo ? "⚡ Турбо уже ВКЛ" : "⚡ Турбо уже ВЫКЛ";
    if (on) {
      const save = {}; for (const k of Object.keys(TURBO)) save[k] = CFG[k];
      CFG.turboSave = save; CFG.turbo = true; CFG.turboFrom = now();
      setCfg(Object.assign({ _turbo: true }, TURBO));
      log(`⚡ ТУРБО ВКЛ: вход ${TURBO.off}% от цены, тейк +${TURBO.tp}%, мин. плюс через ${TURBO.maxHold} мин · авто-выкл при −${TURBO_LOSS_PCT}% депозита`, "ok");
      jlog("turbo", { on: true, save });
      return "⚡ Турбо включено";
    }
    const pnl = turboPnl(), save = CFG.turboSave || {};
    CFG.turbo = false; CFG.turboSave = null; CFG.turboFrom = 0;
    setCfg(Object.assign({ _turbo: true }, save));
    log(`⚡ ТУРБО ВЫКЛ${why ? " (" + why + ")" : ""}: обычные настройки возвращены · за турбо ${pnl >= 0 ? "+" : ""}${pnl.toFixed(2)} USDT`, why ? "err" : "info");
    jlog("turbo", { on: false, why: why || "", pnl });
    return "⚡ Турбо выключено";
  }
  function turboGuard(eq) {
    if (!CFG.turbo || !(eq > 0)) return;
    const pnl = turboPnl();
    if (pnl <= -eq * TURBO_LOSS_PCT / 100) setTurbo(false, `убыток ${pnl.toFixed(2)} USDT ≥ ${TURBO_LOSS_PCT}% депозита`);
  }
  async function start() {
    if (CFG.live && !keys()) return "Нет BYBIT_API_KEY / BYBIT_API_SECRET в Railway → Variables";
    running = true; lsSet("bo.running", true);
    if (!STAT.since) { STAT.since = now(); STAT.doneUntil = STAT.since; saveStat(); }
    log(`СТАРТ ${isLive() ? "LIVE" : "(только сигналы)"}: микро-ходы в обе стороны, только PostOnly-лимитки · до ${CFG.maxCoins} монет · до ${CFG.maxPos} позиций · залог ${CFG.usdt} USDT × ${CFG.lev} · вход ${CFG.off}% от цены · тейк +${CFG.tp}% · выход только в плюс · мин. плюс через ${CFG.maxHold} мин${CFG.oiOn ? " · фильтр OI" : ""}${CFG.turbo ? " · ⚡ турбо" : ""}`, "ok");
    jlog("start", { cfg: CFG });
    schedule();
    await scan().catch(() => {});
    return "Бот запущен — микро-ходы лимитками 24/7";
  }
  async function stop() {
    running = false; lsSet("bo.running", false); schedule();
    for (const s of Object.keys(ENTRY)) await dropEntry(s);
    ARMED = [];
    log("СТОП: лимитки входа сняты. Открытые позиции бот доводит до выхода в плюс лимитками.", "info"); jlog("stop", {});
    return "Остановлен";
  }
  function resetStats() { Object.assign(STAT, { scans: 0, entries: 0, wins: 0, losses: 0, since: running ? now() : 0, realized: 0, closed: 0, realDone: 0, closedDone: 0, doneUntil: running ? now() : 0 }); realAt = 0; saveStat(); return "Статистика сброшена"; }
  // разово: снять старые условные ордера пробоя (bo_…) — они исполнялись как тейкер
  async function dropOld() {
    if (!keys() || lsGet("mk.oldDropped", false)) return;
    try {
      const orders = (await priv("GET", "/v5/order/realtime", { category: "linear", settleCoin: "USDT", limit: 50 })).list || [];
      let n = 0; for (const o of orders) if (/^bo_/.test(o.orderLinkId || "") && !o.reduceOnly) { await cancel(o.symbol, o.orderId); n++; }
      log(`Новая стратегия «микро-ходы»: маркета и условных ордеров больше нет, только PostOnly-лимитки. Снято старых ордеров пробоя: ${n}.`, "ok");
    } catch (e) { log("Переход: " + e.message, "err"); return; }
    lsSet("mk.oldDropped", true);
  }

  // ---------------- 📒 дни ----------------
  const DAYC = {};
  function readDay(day) {
    const f = path.join(JDIR, day + ".jsonl"); let st;
    try { st = fs.statSync(f); } catch (e) { return null; }
    if (DAYC[day] && DAYC[day].mtime === st.mtimeMs) return DAYC[day];
    const trades = [];
    for (const l of fs.readFileSync(f, "utf8").split("\n")) { if (!l || l.indexOf('"type":"trade"') < 0) continue; try { trades.push(JSON.parse(l)); } catch (e) { /* битая строка */ } }
    const pnl = trades.reduce((a, x) => a + (+x.pnl || 0), 0);
    return (DAYC[day] = { mtime: st.mtimeMs, trades, pnl: +pnl.toFixed(4), n: trades.length, wins: trades.filter(x => x.pnl > 0).length, size: st.size });
  }
  function days() {
    let files = []; try { files = fs.readdirSync(JDIR).filter(f => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)).sort().reverse().slice(0, 60); } catch (e) { /* нет */ }
    return files.map(f => { const day = f.slice(0, 10), d = readDay(day) || {}; return { day, n: d.n || 0, wins: d.wins || 0, pnl: d.pnl || 0, kb: Math.round((d.size || 0) / 1024) }; });
  }
  const journalPath = (day) => /^\d{4}-\d{2}-\d{2}$/.test(day) ? path.join(JDIR, day + ".jsonl") : null;

  function view() {
    const syms = [...new Set([...ARMED, ...Object.values(TRADES).map(t => t.sym)])];
    const rows = syms.map(s => {
      const e = ENTRY[s] || {}, ps = posOf(s), c = coin(s);
      return { sym: s, last: c.last, bid: c.bid, ask: c.ask, buy: e.Buy ? e.Buy.px : null, sell: e.Sell ? e.Sell.px : null,
        pos: ps.map(p => { const tr = TRADES[p.sym + "|" + p.side] || {}; return { side: p.side, size: p.size, avg: p.avg, upnl: p.upnl, exit: tr.exitPx || null, mode: tr.mode || "", held: tr.open ? Math.round((now() - tr.open) / 60000) : 0 }; }) };
    });
    const unreal = Object.values(POS).reduce((a, p) => a + (p.upnl || 0), 0), realized = STAT.realized || 0;
    const pnl = { since: STAT.since, realized: +realized.toFixed(4), unreal: +unreal.toFixed(4), total: +(realized + unreal).toFixed(4), closed: STAT.closed || 0 };
    return { turbo: { on: !!CFG.turbo, from: CFG.turboFrom || 0, pnl: CFG.turbo ? turboPnl() : 0, lossPct: TURBO_LOSS_PCT, preset: TURBO }, oi: { on: CFG.oiOn, pct: CFG.oiPct },
      open: ourOpen(), pnl, acc: ACC, running, live: isLive(), cfg: CFG, stat: STAT, rows, cand: CAND, nextScan: running && lastScanAt ? Math.max(0, Math.round((lastScanAt + 60000 - now()) / 1000)) : null, log: LOG.slice(0, 200) };
  }

  const wasRunning = running;
  (async () => {
    await dropOld();
    if (isLive()) { try { await pollOrders(); await pollPositions(); } catch (e) { /* следующий опрос */ } }
    if (wasRunning && running) { log("Сервер перезапущен — бот продолжает микро-ходы лимитками", "ok"); schedule(); scan().catch(() => {}); } else if (!running) schedule();
  })();
  return { start, stop, setCfg, setTurbo, resetStats, view, days, journalPath };
}
module.exports = { create };
