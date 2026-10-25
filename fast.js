// FAST — швидка торгівля по стакану в стилі інституційних трейдерів (Bybit USDT-перпетуали).
// Два режими:
//  • micro — мікроструктура: дисбаланс потоку ордерів (OFI), microprice, дисбаланс 5 рівнів, дисбаланс стрічки (TFI).
//            Вхід лімітом (maker) на найкращій ціні або на тік усередину спреду, тейк 1–3 тіки, вихід одразу, коли потік розвернувся.
//  • mm    — маркет-мейкінг: котирування з обох боків навколо microprice, зсув котирувань від запасу позиції (inventory skew),
//            знімання котирування з боку, проти якого йде «токсичний» потік, виграш на спреді.
//  • auto  — для кожної монети вибирається режим: mm там, де один тік ≥ комісій (широкий тік), інакше micro; далі — за результатами.
// Дані: публічний WS (стакан 50 рівнів кожні 20 мс + стрічка), приватний WS (ордери, виконання, позиції — без опитування REST).
// PnL рахується з виконань біржі (включно з комісіями): угода = від нуля позиції до нуля.
"use strict";
const WebSocketNode = (() => { try { return require("ws"); } catch (e) { return null; } })();

const DEFAULTS = {
  on: false,
  mode: "auto",            // "micro" | "mm" | "auto"
  symbols: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"],
  autoCoins: true,
  coinCount: 6,
  minTurnoverM: 100,       // 24 h turnover, million $
  pickEveryMin: 15,
  margin: 10,              // USDT margin per coin (position / inventory cap = margin × leverage)
  leverage: 10,
  maxPositions: 4,         // coins with an open position at once
  // micro
  thr: 0.55,               // |score| to enter (score -1..1)
  ofiSec: 3,               // window of the order-flow imbalance and the tape imbalance, s
  takeTicks: 2,            // take in ticks (at least what beats the fees)
  stopPct: 0.15,           // stop from the entry, %
  maxHoldSec: 20,          // longer -> leave at the touch
  entryWaitSec: 2,         // an unfilled entry is cancelled after this
  maxSpreadTicks: 2,       // wider spread -> no micro entry
  stepIn: true,            // spread ≥ 2 ticks: stand one tick inside the spread (first in the queue)
  cooldownSec: 2,
  fastExit: true,          // the touch exit did not fill in 3 s and the price runs away -> market exit
  // market making
  mmEdgePct: 0.05,         // min distance of each quote from the fair price, % (round trip must beat 2 × maker fee)
  mmQtyPct: 30,            // size of each quote, % of the inventory cap
  mmSkew: 1,               // how strongly the inventory shifts the quotes (0..3)
  mmToxic: 0.6,            // flow score against a side above this -> that quote is pulled
  mmStopPct: 0.5,          // stop on the inventory, %
  mmMaxHoldSec: 90,        // inventory older than this -> flatten at the touch
  // learning
  learn: true,             // adapt the micro threshold, MM edge and the mode per coin from own results
};
const NUM = { coinCount: [1, 12], minTurnoverM: [5, 5000], pickEveryMin: [2, 240], margin: [1, 100000], leverage: [1, 50], maxPositions: [1, 12],
  thr: [0.3, 0.95], ofiSec: [1, 15], takeTicks: [1, 30], stopPct: [0.05, 3], maxHoldSec: [3, 600], entryWaitSec: [0.5, 30], maxSpreadTicks: [1, 10],
  cooldownSec: [0, 600], mmEdgePct: [0.01, 1], mmQtyPct: [5, 100], mmSkew: [0, 3], mmToxic: [0.2, 1], mmStopPct: [0.1, 5], mmMaxHoldSec: [10, 3600] };
const BOOL = ["autoCoins", "stepIn", "fastExit", "learn", "on"];

function create(host) {
  const { bx, spec, align, sign, log, engineLog, lsGet, lsSet, env, keys } = host;
  const WS = host.WebSocket || WebSocketNode;
  const MAKER = host.makerFee != null ? host.makerFee : 0.02, TAKER = host.takerFee != null ? host.takerFee : 0.055;   // %
  const PUB = host.testWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/public/linear" : "wss://stream.bybit.com/v5/public/linear");
  const PRIV = host.testPrivWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/private" : "wss://stream.bybit.com/v5/private");

  const P = Object.assign({}, DEFAULTS, lsGet("fx", {}));
  const save = () => lsSet("fx", P);
  const TR = lsGet("fxTrades", []);                     // closed round trips
  const LEARN = Object.assign({ thr: {}, edge: {}, modeStat: {} }, lsGet("fxLearn", {}));
  const saveLearn = () => lsSet("fxLearn", LEARN);
  const M = {};      // market state per symbol
  const A = {};      // own accounting per symbol (from executions)
  const ORD = {};    // our live orders by orderLinkId
  const LAT = { wsLag: 0, rtt: 0, fillMs: 0, n: 0 };
  const ST = { pubUp: false, privUp: false, lastPub: 0, err: "", vol: [], pickedAt: 0, hedge: {}, lev: {}, seenExec: new Set(), lastExecPoll: 0 };
  const now = () => Date.now();
  const setErr = (t) => { ST.err = t; ST.errAt = Date.now(); };

  // ---------------- helpers ----------------
  const tickOf = (sym) => Number((M[sym] && M[sym].tick) || 0);
  const dec = (s) => { s = String(s); return s.includes(".") ? s.split(".")[1].length : 0; };
  const px = (sym, v, up) => align(v, M[sym].tickStr, up);
  const lid = (role) => ("fx" + role + now().toString(36) + Math.random().toString(36).slice(2, 7)).slice(0, 36);
  const roleOf = (id) => String(id || "").slice(2, 3);   // e entry, t take, x exit, b bid, a ask
  const ewma = (old, v, k = 0.1) => old ? old * (1 - k) + v * k : v;
  const managed = () => new Set([...P.symbols, ...Object.keys(A).filter(s => A[s] && Math.abs(A[s].q) > 0)]);

  // ---------------- public data: book + tape ----------------
  function mstate(sym) {
    return M[sym] = M[sym] || { sym, bids: new Map(), asks: new Map(), bb: 0, ba: 0, bbSz: 0, baSz: 0, ofi: [], tape: [], mids: [], ready: false, last: 0,
      tick: 0, tickStr: "0.01", step: "0.001", minQty: 0, minNotional: 5, maxLev: 10, tickPct: 0, sig: null, busy: false, cool: 0, mode: null };
  }
  function best(m) {
    let bb = 0, ba = Infinity;
    for (const p of m.bids.keys()) if (p > bb) bb = p;
    for (const p of m.asks.keys()) if (p < ba) ba = p;
    return [bb, ba === Infinity ? 0 : ba];
  }
  function onBook(sym, d, snap, ts) {
    const m = mstate(sym);
    if (snap) { m.bids.clear(); m.asks.clear(); }
    for (const [p, s] of d.b || []) { const P_ = Number(p), S = Number(s); if (S === 0) m.bids.delete(P_); else m.bids.set(P_, S); }
    for (const [p, s] of d.a || []) { const P_ = Number(p), S = Number(s); if (S === 0) m.asks.delete(P_); else m.asks.set(P_, S); }
    const [bb, ba] = best(m);
    if (!bb || !ba || bb >= ba) return;
    const bbSz = m.bids.get(bb), baSz = m.asks.get(ba), t = now();
    // order-flow imbalance at the touch (Cont, Kukanov, Stoikov): what was added to the bid minus what was added to the ask
    if (m.bb) {
      const e = (bb >= m.bb ? bbSz : 0) - (bb <= m.bb ? m.bbSz : 0) - (ba <= m.ba ? baSz : 0) + (ba >= m.ba ? m.baSz : 0);
      if (e) m.ofi.push([t, e]);
    }
    m.bb = bb; m.ba = ba; m.bbSz = bbSz; m.baSz = baSz; m.ready = true;
    if (m.tick) m.tickPct = m.tick / bb * 100;
    const mid = (bb + ba) / 2;
    if (!m.mids.length || t - m.mids[m.mids.length - 1][0] >= 250) m.mids.push([t, mid]);
    if (ts) LAT.wsLag = ewma(LAT.wsLag, Math.max(0, t - ts), 0.05);
  }
  function onTrades(sym, list) {
    const m = mstate(sym);
    for (const x of list) { m.tape.push([Number(x.T), x.S === "Buy" ? 1 : -1, Number(x.v), Number(x.p)]); m.last = Number(x.p); }
  }
  function trim(m) {
    const t = now();
    const cut = (arr, ms) => { let i = 0; while (i < arr.length && t - arr[i][0] > ms) i++; if (i) arr.splice(0, i); };
    cut(m.ofi, 15000); cut(m.tape, 15000); cut(m.mids, 120000);
  }
  // the signal: -1 (sell pressure) .. +1 (buy pressure)
  function features(sym) {
    const m = M[sym];
    if (!m || !m.ready || !m.tick) return null;
    trim(m);
    const t = now(), w = P.ofiSec * 1000, spread = m.ba - m.bb, mid = (m.ba + m.bb) / 2;
    const micro = (m.bb * m.baSz + m.ba * m.bbSz) / (m.bbSz + m.baSz);
    const microN = Math.max(-1, Math.min(1, (micro - mid) / (spread / 2)));
    // weighted imbalance of the first 5 levels
    const lv = (map, desc) => [...map.entries()].sort((a, b) => desc ? b[0] - a[0] : a[0] - b[0]).slice(0, 5);
    const wts = [1, 0.8, 0.6, 0.4, 0.2];
    let B = 0, Aq = 0; lv(m.bids, true).forEach(([, s], i) => B += s * wts[i]); lv(m.asks, false).forEach(([, s], i) => Aq += s * wts[i]);
    const imb = B + Aq > 0 ? (B - Aq) / (B + Aq) : 0;
    // OFI normalised by the touch depth
    const ofiSum = m.ofi.filter(x => t - x[0] <= w).reduce((a, x) => a + x[1], 0);
    const depth = (m.bbSz + m.baSz) / 2 || 1;
    const ofiN = Math.tanh(ofiSum / (depth * 2));
    // tape: who is aggressive
    let bv = 0, sv = 0, n = 0; for (const x of m.tape) if (t - x[0] <= w) { if (x[1] > 0) bv += x[2]; else sv += x[2]; n++; }
    const tfi = bv + sv > 0 ? (bv - sv) / (bv + sv) : 0;
    const activity = Math.min(1, n / (P.ofiSec * 3));          // a quiet tape weakens the signal
    const score = (0.35 * ofiN + 0.25 * microN + 0.2 * imb + 0.2 * tfi) * (0.5 + 0.5 * activity);
    // volatility: std of 250 ms mid changes over the last minute, in ticks
    const ms = m.mids.filter(x => t - x[0] <= 60000).map(x => x[1]); let vol = 0;
    if (ms.length > 4) { const ch = ms.slice(1).map((v, i) => (v - ms[i]) / m.tick); const mu = ch.reduce((a, b) => a + b, 0) / ch.length; vol = Math.sqrt(ch.reduce((a, b) => a + (b - mu) ** 2, 0) / ch.length); }
    return { sym, bb: m.bb, ba: m.ba, mid, micro, spreadTicks: Math.round(spread / m.tick), tickPct: m.tickPct, microN: +microN.toFixed(2), imb: +imb.toFixed(2),
      ofi: +ofiN.toFixed(2), tfi: +tfi.toFixed(2), activity: +activity.toFixed(2), score: +score.toFixed(3), vol: +vol.toFixed(2) };
  }

  let pub = null, pubPing = null, pubSubs = new Set();
  function connectPub() {
    try { pub = new WS(PUB); } catch (e) { setErr("WS: " + e.message); setTimeout(connectPub, 3000); return; }
    pub.on("open", () => { ST.pubUp = true; pubSubs = new Set(); resub(); clearInterval(pubPing); pubPing = setInterval(() => { try { pub.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    pub.on("message", (raw) => {
      let j; try { j = JSON.parse(String(raw)); } catch (e) { return; }
      if (!j.topic || !j.data) return;
      ST.lastPub = now();
      const [kind, , symA] = j.topic.split("."), sym = symA || j.topic.split(".")[1];
      if (kind === "orderbook") onBook(j.data.s || symA, j.data, j.type === "snapshot", Number(j.cts || j.ts));
      else if (kind === "publicTrade") onTrades(sym, j.data);
    });
    pub.on("close", () => { ST.pubUp = false; clearInterval(pubPing); for (const s of Object.keys(M)) { M[s].ready = false; } setTimeout(connectPub, 2000); });
    pub.on("error", (e) => { setErr("WS: " + (e && e.message || e)); });
  }
  function resub() {
    if (!pub || !ST.pubUp) return;
    const want = managed();
    const add = [...want].filter(s => !pubSubs.has(s)), del = [...pubSubs].filter(s => !want.has(s));
    if (add.length) { pub.send(JSON.stringify({ op: "subscribe", args: add.flatMap(s => [`orderbook.50.${s}`, `publicTrade.${s}`]) })); add.forEach(s => pubSubs.add(s)); }
    if (del.length) { pub.send(JSON.stringify({ op: "unsubscribe", args: del.flatMap(s => [`orderbook.50.${s}`, `publicTrade.${s}`]) })); del.forEach(s => { pubSubs.delete(s); if (M[s]) M[s].ready = false; }); }
  }

  // ---------------- private data: orders, executions ----------------
  let priv = null, privPing = null;
  function connectPriv() {
    if (!keys || !keys.key) return;
    try { priv = new WS(PRIV); } catch (e) { setTimeout(connectPriv, 5000); return; }
    priv.on("open", () => {
      const expires = now() + 10000;
      priv.send(JSON.stringify({ op: "auth", args: [keys.key, expires, sign("GET/realtime" + expires)] }));
      clearInterval(privPing); privPing = setInterval(() => { try { priv.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000);
    });
    priv.on("message", (raw) => {
      let j; try { j = JSON.parse(String(raw)); } catch (e) { return; }
      if (j.op === "auth") { if (j.success) { ST.privUp = true; priv.send(JSON.stringify({ op: "subscribe", args: ["order", "execution"] })); } else setErr("приватний WS: " + (j.ret_msg || "auth")); return; }
      if (j.topic === "execution") for (const x of j.data || []) onExec(x);
      else if (j.topic === "order") for (const o of j.data || []) onOrder(o);
    });
    priv.on("close", () => { ST.privUp = false; clearInterval(privPing); setTimeout(connectPriv, 3000); });
    priv.on("error", () => {});
  }
  function onOrder(o) {
    const id = o.orderLinkId; if (!ORD[id]) return;
    const st = o.orderStatus;
    if (["Filled", "Cancelled", "Rejected", "Deactivated", "PartiallyFilledCanceled"].includes(st)) {
      if (st === "Cancelled" && /PostOnly/i.test(o.rejectReason || o.cancelType || "")) ORD[id].postOnlyKill = true;
      delete ORD[id];
    } else { ORD[id].filled = Number(o.cumExecQty || 0); ORD[id].live = true; }
  }
  // own accounting: a round trip runs from a flat position back to flat
  function acc(sym) { return A[sym] = A[sym] || { q: 0, avg: 0, real: 0, fees: 0, at: 0, mode: null, side: null, fills: 0, maxAdv: 0, sig: null, how: "" }; }
  function onExec(x) {
    if (x.category && x.category !== "linear") return;
    if (ST.seenExec.has(x.execId)) return; ST.seenExec.add(x.execId);
    if (ST.seenExec.size > 5000) ST.seenExec = new Set([...ST.seenExec].slice(-2000));
    const sym = x.symbol;
    const ours = String(x.orderLinkId || "").startsWith("fx");
    if (!ours && !(A[sym] && A[sym].q)) return;           // someone else's trade on a coin we do not hold
    if (x.execType && x.execType !== "Trade") return;      // funding etc.
    const qty = Number(x.execQty), p = Number(x.execPrice), fee = Number(x.execFee || 0), s = x.side === "Buy" ? 1 : -1;
    if (!(qty > 0)) return;
    const a = acc(sym), o = ORD[x.orderLinkId];
    if (o && o.at) LAT.fillMs = ewma(LAT.fillMs, now() - o.at, 0.1);
    if (ours && M[sym] && M[sym].sizeK && M[sym].sizeK < 1) M[sym].sizeK = Math.min(1, M[sym].sizeK * 1.25);
    if (!a.q) { a.at = now(); a.real = 0; a.fees = 0; a.fills = 0; a.mode = (M[sym] && M[sym].mode) || P.mode; a.side = s > 0 ? "Buy" : "Sell"; a.sig = M[sym] && M[sym].sig; a.maxAdv = 0; a.how = ""; }
    a.fees += fee; a.fills++;
    if (!a.q || Math.sign(a.q) === s) { a.avg = (a.avg * Math.abs(a.q) + p * qty) / (Math.abs(a.q) + qty); a.q += s * qty; }
    else {
      const close = Math.min(qty, Math.abs(a.q));
      a.real += close * (p - a.avg) * Math.sign(a.q); a.q += s * close;
      const rest = qty - close;
      const r = roleOf(x.orderLinkId);
      a.how = !ours ? "стоп" : r === "t" ? "тейк" : r === "x" ? (a.how || "вихід") : r === "b" || r === "a" ? "спред" : a.how || "вихід";
      if (Math.abs(a.q) < 1e-12) { a.q = 0; closeTrip(sym, p); }
      if (rest > 0) { a.at = now(); a.real = 0; a.fees = 0; a.fills = 1; a.side = s > 0 ? "Buy" : "Sell"; a.mode = (M[sym] && M[sym].mode) || P.mode; a.avg = p; a.q = s * rest; }
    }
    stopDirty(sym);
  }
  function closeTrip(sym, exitPx) {
    const a = A[sym];
    const pnl = +(a.real - a.fees).toFixed(5);
    const t = { sym, mode: a.mode, side: a.side, pnl, gross: +a.real.toFixed(5), fees: +a.fees.toFixed(5), exit: exitPx, holdMs: now() - a.at, fills: a.fills, how: a.how || "вихід",
      closedAt: now(), sig: a.sig ? { score: a.sig.score, ofi: a.sig.ofi, microN: a.sig.microN, imb: a.sig.imb, tfi: a.sig.tfi, spread: a.sig.spreadTicks } : null };
    TR.push(t); if (TR.length > 3000) TR.splice(0, TR.length - 3000);
    lsSet("fxTrades", TR);
    learnFrom(t);
    engineLog(`${a.mode === "mm" ? "🏦" : "⚡"} ${sym}: ${a.mode === "mm" ? "MM-цикл" : a.side === "Buy" ? "лонг" : "шорт"} закрито (${t.how}) ${pnl >= 0 ? "+" : ""}${pnl} USDT · комісії ${t.fees} · ${(t.holdMs / 1000).toFixed(1)} с`, pnl >= 0 ? "up" : "down");
  }

  // ---------------- orders ----------------
  async function hedgeOf(sym) {
    if (ST.hedge[sym] != null) return ST.hedge[sym];
    try { const r = await bx("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true }); ST.hedge[sym] = (r.list || []).some(x => Number(x.positionIdx) > 0); }
    catch (e) { ST.hedge[sym] = false; }
    return ST.hedge[sym];
  }
  // in hedge mode a Sell while long must close the long (idx 1, reduceOnly); in one-way mode idx 0
  function idxFor(sym, side, reduce) {
    if (!ST.hedge[sym]) return { positionIdx: 0, reduceOnly: !!reduce };
    const q = (A[sym] && A[sym].q) || 0;
    if (side === "Sell" && q > 0) return { positionIdx: 1, reduceOnly: true };
    if (side === "Buy" && q < 0) return { positionIdx: 2, reduceOnly: true };
    return { positionIdx: side === "Buy" ? 1 : 2, reduceOnly: false };
  }
  // free money on the account (USDT), refreshed every few seconds and right after a "not enough" rejection
  async function refreshWallet() {
    if (!keys || !keys.key) return;
    try {
      const w = await bx("/v5/account/wallet-balance", { query: { accountType: "UNIFIED" }, auth: true });
      const a = (w.list || [])[0] || {};
      ST.avail = Number(a.totalAvailableBalance || 0); ST.availAt = now();
    } catch (e) { /* keep the old value */ }
  }
  setInterval(() => { if (P.on) refreshWallet(); }, 3000);
  // little money: the order is made smaller and, if even the exchange minimum does not fit, the leverage of that coin goes up (to its max)
  function sizing(sym, price) {
    const m = M[sym], base = Math.min(P.leverage, Math.floor(m.maxLev || P.leverage));
    const minNotional = Math.max(m.minNotional * 1.05, m.minQty * price);
    const free = ST.avail == null ? Infinity : Math.max(0, ST.avail * 0.9);
    const margin = Math.min(P.margin, free) * (m.sizeK || 1);
    let lev = base;
    if (margin * lev < minNotional) lev = Math.min(Math.floor(m.maxLev || base), Math.ceil(minNotional * 1.1 / Math.max(margin, 1e-9)));
    const ok = margin * lev >= minNotional;
    return { margin, lev, ok, minNotional, free };
  }
  async function setLev(sym, want) {
    const m = M[sym], lev = Math.max(1, Math.round(want || Math.min(P.leverage, Math.floor(m.maxLev || P.leverage))));
    if (ST.lev[sym] === lev) return;
    if (ST.lev[sym] > lev && now() - ((ST.levAt || {})[sym] || 0) < 60000) return;   // a higher leverage stays at least a minute (no flapping)
    (ST.levAt = ST.levAt || {})[sym] = now();
    try { await bx("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); }
    catch (e) { if (e.code !== 110043) { log("leverage", sym, e.message); if (e.code === 110013 || e.code === 110012) return; } }   // cannot change now (open position) -> keep the old one
    if (lev > P.leverage && ST.lev[sym] !== lev) engineLog(`💰 ${sym}: мало вільних коштів — плече x${lev}, щоб відкрити мінімальний ордер біржі`, "mute");
    ST.lev[sym] = lev;
  }
  async function place(sym, side, price, qty, role, { reduce = false, postOnly = true, market = false } = {}) {
    const id = lid(role), ix = idxFor(sym, side, reduce), t0 = now();
    ORD[id] = { id, sym, side, price: Number(price), qty: Number(qty), role, at: t0, ix: ix.positionIdx, ro: ix.reduceOnly, filled: 0 };
    const body = { category: "linear", symbol: sym, side, orderType: market ? "Market" : "Limit", qty: String(qty), orderLinkId: id,
      timeInForce: market ? "IOC" : postOnly ? "PostOnly" : "GTC", positionIdx: ix.positionIdx, reduceOnly: ix.reduceOnly };
    if (!market) body.price = String(price);
    try { await bx("/v5/order/create", { method: "POST", auth: true, body }); LAT.rtt = ewma(LAT.rtt, now() - t0, 0.1); LAT.n++; return id; }
    catch (e) {
      delete ORD[id];
      if (e.code === 10001 && ST.hedge[sym] !== true) { ST.hedge[sym] = true; }       // the account is in hedge mode
      else if (e.code === 110017 || e.code === 110025) { /* reduce-only with nothing to reduce: position already flat */ }
      else if ([110007, 110004, 110012, 110044, 110045].includes(e.code) || /not enough|insufficient/i.test(e.message)) {
        const m = M[sym]; m.sizeK = Math.max(0.1, (m.sizeK || 1) * 0.5); m.cool = now() + 800; refreshWallet();
        if (now() - (ST.nmLog || 0) > 60000) { ST.nmLog = now(); engineLog(`💰 ${sym}: біржа каже «мало коштів» — зменшую розмір ордера і пробую знову`, "mute"); }
      }
      else { setErr(`${sym}: ${e.message}`); M[sym].cool = now() + 3000; }
      return null;
    }
  }
  async function cancel(id) {
    const o = ORD[id]; if (!o) return;
    o.cancelAt = now();
    try { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: o.sym, orderLinkId: id } }); }
    catch (e) { if (e.code === 110001) delete ORD[id]; }      // already gone
  }
  async function amend(id, price, qty) {
    const o = ORD[id]; if (!o) return false;
    if (now() - (o.amendAt || 0) < 200) return false;        // Bybit rate limits: one amend per order per 200 ms
    o.amendAt = now();
    const body = { category: "linear", symbol: o.sym, orderLinkId: id };
    if (price != null) body.price = String(price);
    if (qty != null) body.qty = String(qty);
    try { await bx("/v5/order/amend", { method: "POST", auth: true, body }); if (price != null) o.price = Number(price); if (qty != null) o.qty = Number(qty); return true; }
    catch (e) { if (e.code === 110001 || e.code === 10001) delete ORD[id]; return false; }
  }
  const ordersOf = (sym, role) => Object.values(ORD).filter(o => o.sym === sym && (!role || o.role === role));
  // order size from the money that is really free (0 = not even the exchange minimum fits)
  const minQtyOf = (sym, price) => { const m = M[sym]; return Math.max(m.minQty, Number(align((m.minNotional * 1.05) / price, m.step, true))); };
  const qtyFor = (sym, price, part = 1) => {
    const m = M[sym], z = sizing(sym, price);
    if (!z.ok) return 0;
    let q = Number(align(z.margin * z.lev * part / price, m.step, false));
    const minQ = minQtyOf(sym, price);
    if (q < minQ) q = minQ;
    return Number(align(q, m.step, false)) || minQ;
  };
  const noMoney = (sym, price) => { const z = sizing(sym, price); return `мало коштів: вільно ${z.free === Infinity ? "?" : z.free.toFixed(2)} USDT, мінімальний ордер ${z.minNotional.toFixed(2)} USDT навіть з плечем x${Math.floor(M[sym].maxLev || 1)}`; };

  // exchange stop on every position (last-price trigger: faster than the mark price)
  const stopQ = new Set();
  function stopDirty(sym) { stopQ.add(sym); }
  async function stopFlush() {
    for (const sym of [...stopQ]) {
      stopQ.delete(sym);
      const a = A[sym]; if (!a || !a.q || !M[sym] || !M[sym].tick) continue;
      const long = a.q > 0, pct = a.mode === "mm" ? P.mmStopPct : P.stopPct;
      const sl = px(sym, a.avg * (1 - (long ? 1 : -1) * pct / 100), !long);
      if (a.slPx === sl && a.slQ === a.q) continue;
      try {
        await bx("/v5/position/trading-stop", { method: "POST", auth: true, body: { category: "linear", symbol: sym, stopLoss: sl, slTriggerBy: "LastPrice", tpslMode: "Full", positionIdx: ST.hedge[sym] ? (long ? 1 : 2) : 0 } });
        a.slPx = sl; a.slQ = a.q;
      } catch (e) { if (e.code !== 34040) log("stop", sym, e.message); }   // 34040 = not modified
    }
  }

  // ---------------- learning ----------------
  function learnFrom(t) {
    if (!P.learn) return;
    const k = t.sym + ":" + t.mode;
    const s = LEARN.modeStat[k] = LEARN.modeStat[k] || { n: 0, pnl: 0, ew: 0 };
    s.n++; s.pnl = +(s.pnl + t.pnl).toFixed(5); s.ew = +ewma(s.n === 1 ? 0 : s.ew, t.pnl, 0.15).toFixed(6);
    if (t.mode === "micro") {
      // 3 micro losses in a row on a coin -> stricter threshold there; profits -> slowly back
      const last = TR.filter(x => x.sym === t.sym && x.mode === "micro").slice(-3);
      const cur = LEARN.thr[t.sym] || P.thr;
      if (last.length === 3 && last.every(x => x.pnl <= 0)) { LEARN.thr[t.sym] = +Math.min(0.9, cur + 0.05).toFixed(2); engineLog(`🧪 ${t.sym}: 3 мікро-угоди в мінусі — поріг сигналу ${cur}→${LEARN.thr[t.sym]}`, "mute"); }
      else if (t.pnl > 0 && cur > P.thr) LEARN.thr[t.sym] = +Math.max(P.thr, cur - 0.01).toFixed(2);
    } else {
      // MM: losing cycles -> wider quotes (less adverse selection); winning -> tighter (more fills)
      const last = TR.filter(x => x.sym === t.sym && x.mode === "mm").slice(-5);
      const cur = LEARN.edge[t.sym] || 1;
      if (last.length >= 3 && last.reduce((a, x) => a + x.pnl, 0) < 0) LEARN.edge[t.sym] = +Math.min(4, cur + 0.25).toFixed(2);
      else if (t.pnl > 0 && cur > 1) LEARN.edge[t.sym] = +Math.max(1, cur - 0.05).toFixed(2);
    }
    saveLearn();
  }
  const thrOf = (sym) => (P.learn && LEARN.thr[sym]) || P.thr;
  const edgeOf = (sym) => P.mmEdgePct * ((P.learn && LEARN.edge[sym]) || 1);
  // which mode for this coin now
  function modeFor(sym, f) {
    if (P.mode !== "auto") return P.mode;
    const m = M[sym], a = A[sym];
    if (a && a.q && a.mode) return a.mode;                          // finish what is open in its own mode
    const st = (md) => LEARN.modeStat[sym + ":" + md];
    const mm = st("mm"), mi = st("micro");
    if (P.learn && mm && mi && mm.n >= 10 && mi.n >= 10) return mm.ew >= mi.ew ? "mm" : "micro";
    if (P.learn && mm && mm.n >= 10 && mm.ew < 0) return "micro";
    if (P.learn && mi && mi.n >= 10 && mi.ew < 0 && m.tickPct >= P.mmEdgePct) return "mm";
    // one tick is already worth more than the fees and the price is not running: market making; otherwise microstructure
    return m.tickPct >= P.mmEdgePct && f.vol < 2.5 ? "mm" : "micro";
  }

  // ---------------- strategies ----------------
  const minTakeTicks = (sym) => Math.max(P.takeTicks, Math.ceil((2 * MAKER + 0.01) / (M[sym].tickPct || 0.01)));
  async function runMicro(sym, f) {
    const m = M[sym], a = acc(sym), thr = thrOf(sym);
    const entries = ordersOf(sym, "e"), takes = ordersOf(sym, "t"), exits = ordersOf(sym, "x");
    if (!a.q) {
      for (const o of [...takes, ...exits]) await cancel(o.id);
      if (entries.length) {
        const o = entries[0], dir = o.side === "Buy" ? 1 : -1;
        const fade = f.score * dir < thr * 0.5;
        const away = o.side === "Buy" ? f.bb > o.price + m.tick * 0.5 : f.ba < o.price - m.tick * 0.5;
        if (fade || away || now() - o.at > P.entryWaitSec * 1000) await cancel(o.id);
        return;
      }
      if (!P.on || now() < m.cool) return;
      if (openCount() >= P.maxPositions) { m.why = "ліміт позицій"; return; }
      if (f.spreadTicks > P.maxSpreadTicks) { m.why = `спред ${f.spreadTicks} тік.`; return; }
      if (Math.abs(f.score) < thr) { m.why = `сигнал ${f.score} (треба ±${thr})`; return; }
      const side = f.score > 0 ? "Buy" : "Sell";
      let price = side === "Buy" ? f.bb : f.ba;
      if (P.stepIn && f.spreadTicks >= 2) price = side === "Buy" ? f.bb + m.tick : f.ba - m.tick;   // first in the queue
      price = Number(px(sym, price, side === "Sell"));
      m.why = "вхід"; m.sig = f; m.cool = now() + P.cooldownSec * 1000;
      const qty = qtyFor(sym, price);
      if (!qty) { m.why = noMoney(sym, price); m.cool = now() + 5000; return; }
      await setLev(sym, sizing(sym, price).lev);
      await place(sym, side, price, qty, "e");
      return;
    }
    // in a position
    for (const o of entries) await cancel(o.id);
    const long = a.q > 0, size = Number(align(Math.abs(a.q), m.step, false));
    const mv = (long ? f.mid - a.avg : a.avg - f.mid) / a.avg * 100;
    a.maxAdv = Math.min(a.maxAdv, mv);
    const against = f.score * (long ? -1 : 1);
    const held = (now() - a.at) / 1000;
    const exitSide = long ? "Sell" : "Buy";
    const touch = long ? f.ba : f.bb;                              // maker exit at the best price on our side
    const leave = against >= thr * 0.6 ? "потік розвернувся" : held > P.maxHoldSec ? "час" : !P.on ? "зупинка" : "";
    if (leave && !exits.length) {
      for (const o of takes) await cancel(o.id);
      a.how = "вихід: " + leave; a.leaveAt = now();
      await place(sym, exitSide, touch, size, "x", { reduce: true });
      return;
    }
    if (exits.length) {
      const o = exits[0];
      if (P.fastExit && now() - (a.leaveAt || o.at) > 3000 && mv < 0) {     // not filled and the price runs away: cross the spread
        await cancel(o.id); a.how = "швидкий вихід"; await place(sym, exitSide, 0, size, "x", { reduce: true, market: true }); return;
      }
      if (Math.abs(o.price - touch) >= m.tick * 0.5 || o.qty !== size) await amend(o.id, touch, o.qty !== size ? size : null);
      return;
    }
    const tp = Number(px(sym, a.avg + (long ? 1 : -1) * minTakeTicks(sym) * m.tick, long));
    if (!takes.length) { await place(sym, exitSide, tp, size, "t", { reduce: true }); return; }
    const o = takes[0];
    if (o.qty !== size || Math.abs(o.price - tp) >= m.tick * 0.5) await amend(o.id, tp, o.qty !== size ? size : null);
  }

  async function runMM(sym, f) {
    const m = M[sym], a = acc(sym), tick = m.tick;
    const cap = qtyFor(sym, f.mid) || (a.q ? Math.abs(a.q) : 0);       // inventory cap in coins (from the money that is free)
    if (!cap) { for (const o of ordersOf(sym)) await cancel(o.id); m.why = noMoney(sym, f.mid); return; }
    const inv = a.q / cap;                                             // -1..1
    const held = a.q ? (now() - a.at) / 1000 : 0;
    // fair price: microprice shifted against the inventory (long -> lower quotes, sells come easier)
    const edge = Math.max(tick, f.mid * edgeOf(sym) / 100, f.vol * tick * 0.5);
    const fair = f.micro - P.mmSkew * inv * edge;
    let bid = Number(px(sym, Math.min(f.bb, fair - edge), false)), ask = Number(px(sym, Math.max(f.ba, fair + edge), true));
    if (ask - bid < tick * 2 && f.spreadTicks < 2) { bid = f.bb; ask = f.ba; }
    const base = Math.max(Number(align(cap * P.mmQtyPct / 100, m.step, false)) || 0, Math.min(cap, minQtyOf(sym, f.mid)));
    let bq = inv >= 1 ? 0 : base, aq = inv <= -1 ? 0 : base;
    // with inventory the opposite quote closes exactly the inventory (no flip through zero), the same side adds only below the cap
    if (a.q > 0) aq = Number(align(a.q, m.step, false));
    if (a.q < 0) bq = Number(align(-a.q, m.step, false));
    // inventory is offered out with a profit over the fees, unless it is getting old or the flow runs against it (then at the market touch)
    const urgent = held > P.mmMaxHoldSec * 0.5 || (a.q > 0 && f.score <= -P.mmToxic) || (a.q < 0 && f.score >= P.mmToxic);
    if (a.q > 0 && !urgent) ask = Math.max(ask, Number(px(sym, a.avg * (1 + (2 * MAKER + 0.005) / 100), true)));
    if (a.q < 0 && !urgent) bid = Math.min(bid, Number(px(sym, a.avg * (1 - (2 * MAKER + 0.005) / 100), false)));
    if (a.q > 0 && urgent) ask = f.ba;
    if (a.q < 0 && urgent) bid = f.bb;
    // toxic flow: do not stand in front of a strong one-way flow
    if (f.score <= -P.mmToxic && a.q >= 0) bq = 0;
    if (f.score >= P.mmToxic && a.q <= 0) aq = 0;
    // stale inventory: flatten at the touch, stop adding
    if (held > P.mmMaxHoldSec) { if (a.q > 0) { bq = 0; ask = f.ba; } if (a.q < 0) { aq = 0; bid = f.bb; } a.how = "вихід: час"; }
    if (!P.on) { if (!a.q) { bq = 0; aq = 0; } else if (a.q > 0) { bq = 0; ask = f.ba; } else { aq = 0; bid = f.bb; } }
    if (P.on && !a.q && openCount() >= P.maxPositions) { bq = 0; aq = 0; m.why = "ліміт позицій"; }
    else m.why = bq || aq ? `котирую ${bq ? bid : "—"} / ${aq ? ask : "—"}` : f.score <= -P.mmToxic ? "токсичний потік вниз" : f.score >= P.mmToxic ? "токсичний потік вгору" : "без котирувань";
    if (P.on && (bq || aq) && !a.q) await setLev(sym, sizing(sym, f.mid).lev);
    await quote(sym, "b", "Buy", bid, bq);
    await quote(sym, "a", "Sell", ask, aq);
    m.sig = f;
  }
  async function quote(sym, role, side, price, qty) {
    const os = ordersOf(sym, role);
    for (const o of os.slice(1)) await cancel(o.id);
    const o = os[0];
    if (!qty) { if (o) await cancel(o.id); return; }
    const ix = idxFor(sym, side, false);
    if (o && (o.ix !== ix.positionIdx || o.ro !== ix.reduceOnly)) { await cancel(o.id); return; }   // the position side changed: re-place next tick
    if (!o) { await place(sym, side, price, qty, role); return; }
    if (Math.abs(o.price - price) >= M[sym].tick * 0.5 || Math.abs(o.qty - qty) > 1e-12) await amend(o.id, price, o.qty !== qty ? qty : null);
  }
  const openCount = () => Object.values(A).filter(a => a.q).length;

  // ---------------- the loop: every 100 ms ----------------
  async function step(sym) {
    const m = M[sym];
    if (!m || !m.ready || m.busy || now() - ST.lastPub > 5000) return;
    const f = features(sym); if (!f) return;
    m.busy = true;
    try {
      const a = A[sym];
      let mode = (a && a.q && a.mode) || modeFor(sym, f);
      if (m.mode && mode !== m.mode && !(a && a.q) && now() - (m.modeAt || 0) < 60000) mode = m.mode;   // hysteresis: a coin keeps its mode at least a minute
      if (m.mode && m.mode !== mode && !(a && a.q)) {               // switching mode on a flat coin: clear the old orders
        for (const o of ordersOf(sym)) await cancel(o.id);
        engineLog(`🔀 ${sym}: режим ${m.mode === "mm" ? "маркет-мейкінг" : "мікро"} → ${mode === "mm" ? "маркет-мейкінг" : "мікро"}`, "mute");
      }
      if (m.mode !== mode) m.modeAt = now();
      m.mode = mode;
      const active = P.symbols.includes(sym) || (a && a.q);
      if (!active) { for (const o of ordersOf(sym)) await cancel(o.id); return; }
      if (mode === "mm") await runMM(sym, f); else await runMicro(sym, f);
    } catch (e) { setErr(`${sym}: ${e.message}`); log("step", sym, e.stack || e.message); }
    finally { m.busy = false; }
  }
  setInterval(() => { for (const sym of managed()) step(sym); }, 100);
  setInterval(() => { stopFlush().catch(() => {}); }, 500);

  // ---------------- reconcile with the exchange every 5 s (and executions by REST when the private WS is down) ----------------
  async function reconcile() {
    if (!keys || !keys.key) return;
    try {
      if (!ST.privUp) {
        const r = await bx("/v5/execution/list", { query: { category: "linear", limit: "100", startTime: String(Math.max(ST.lastExecPoll || now() - 60000, now() - 600000)) }, auth: true });
        ST.lastExecPoll = now() - 2000;
        for (const x of (r.list || []).slice().reverse()) onExec(Object.assign({ category: "linear" }, x));
      }
      const [ordR, posR] = await Promise.all([
        bx("/v5/order/realtime", { query: { category: "linear", settleCoin: "USDT", limit: "50" }, auth: true }),
        bx("/v5/position/list", { query: { category: "linear", settleCoin: "USDT", limit: "200" }, auth: true }),
      ]);
      const live = new Set((ordR.list || []).map(o => o.orderLinkId));
      for (const id of Object.keys(ORD)) if (!live.has(id) && now() - ORD[id].at > 4000) delete ORD[id];
      for (const o of ordR.list || []) if (String(o.orderLinkId).startsWith("fx") && !ORD[o.orderLinkId]) {
        try { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: o.symbol, orderLinkId: o.orderLinkId } }); } catch (e) { /* gone */ }
      }
      // positions: the exchange is the truth for the size
      const syms = managed();
      for (const sym of syms) {
        const rows = (posR.list || []).filter(x => x.symbol === sym && Number(x.size) > 0);
        if (rows.length && Number(rows[0].positionIdx) > 0) ST.hedge[sym] = true;
        const q = rows.reduce((s, x) => s + Number(x.size) * (x.side === "Buy" ? 1 : -1), 0);
        const a = acc(sym);
        if (Math.abs(q - a.q) > 1e-9) {
          a.mis = (a.mis || 0) + 1;
          if (a.mis >= 2) {
            if (!q) { if (a.q) { a.how = a.how || "стоп"; a.q = 0; closeTrip(sym, M[sym] && M[sym].last); } }
            else { if (!a.q) { a.at = now(); a.real = 0; a.fees = 0; a.fills = 0; a.mode = (M[sym] && M[sym].mode) || "micro"; a.side = q > 0 ? "Buy" : "Sell"; } a.q = q; a.avg = Number(rows[0].avgPrice); stopDirty(sym); }
            a.mis = 0;
          }
        } else a.mis = 0;
      }
    } catch (e) { setErr("звірка: " + e.message); }
  }
  setInterval(reconcile, 5000);

  // ---------------- coins ----------------
  async function loadSpecs(list) {
    for (const sym of list) {
      const m = mstate(sym);
      if (m.tick) continue;
      try {
        const s = await spec(sym);
        m.tickStr = s.tick; m.tick = Number(s.tick); m.step = s.step; m.minQty = Number(s.minQty || 0); m.minNotional = Number(s.minNotional || 5); m.maxLev = Number(s.maxLev || 10);
        await hedgeOf(sym);
      } catch (e) { setErr(`${sym}: ${e.message}`); }
    }
  }
  async function pickCoins(force) {
    if (!P.autoCoins || (!force && now() - ST.pickedAt < P.pickEveryMin * 60000)) return;
    ST.pickedAt = now();
    try {
      const r = await bx("/v5/market/tickers", { query: { category: "linear" } });
      const rows = (r.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.turnover24h) >= P.minTurnoverM * 1e6 && Number(x.lastPrice) > 0)
        .map(x => ({ sym: x.symbol, price: Number(x.lastPrice), turnoverM: Math.round(Number(x.turnover24h) / 1e6), range24: +((Number(x.highPrice24h) - Number(x.lowPrice24h)) / Number(x.lastPrice) * 100).toFixed(2),
          spreadPct: Number(x.ask1Price) && Number(x.bid1Price) ? +((Number(x.ask1Price) - Number(x.bid1Price)) / Number(x.lastPrice) * 100).toFixed(4) : null }));
      // micro: the busiest tapes with real movement; MM: coins where one tick already pays the fees
      const micro = rows.slice().sort((a, b) => b.turnoverM * Math.min(b.range24, 20) - a.turnoverM * Math.min(a.range24, 20));
      const mm = rows.filter(x => x.spreadPct != null && x.spreadPct >= P.mmEdgePct && x.spreadPct <= 0.4).sort((a, b) => b.turnoverM - a.turnoverM);
      const n = P.coinCount, pick = [];
      const take = (list, k) => { for (const x of list) { if (pick.length >= n || k <= 0) break; if (!pick.includes(x.sym)) { pick.push(x.sym); k--; } } };
      if (P.mode === "mm") take(mm, n); else if (P.mode === "micro") take(micro, n); else { take(mm, Math.ceil(n / 2)); take(micro, n); }
      if (pick.length < n) take(micro, n);
      ST.vol = rows.filter(x => pick.includes(x.sym));
      // keep coins that still have a position
      for (const s of Object.keys(A)) if (A[s].q && !pick.includes(s)) pick.push(s);
      if (pick.length && pick.join() !== P.symbols.join()) { P.symbols = pick; save(); engineLog(`🪙 Монети: ${pick.join(", ")}`, "mute"); }
    } catch (e) { setErr("вибір монет: " + e.message); }
    await loadSpecs(P.symbols); resub();
  }
  setInterval(() => { if (P.on) pickCoins(false); }, 30000);

  // ---------------- control ----------------
  // positions that are already open on the exchange (after a restart) are taken over at once: stop + exit by their mode
  async function adopt() {
    if (!keys || !keys.key) return;
    try {
      const r = await bx("/v5/position/list", { query: { category: "linear", settleCoin: "USDT", limit: "200" }, auth: true });
      for (const x of r.list || []) {
        const sz = Number(x.size); if (!(sz > 0)) continue;
        const sym = x.symbol, q = sz * (x.side === "Buy" ? 1 : -1);
        if (!P.symbols.includes(sym) && !lsGet("fxOwn", []).includes(sym)) continue;      // not ours: leave someone else's position alone
        if (Number(x.positionIdx) > 0) ST.hedge[sym] = true;
        const a = acc(sym); a.q = q; a.avg = Number(x.avgPrice); a.at = now(); a.real = 0; a.fees = 0; a.fills = 0; a.side = x.side; a.mode = "micro"; a.how = "після перезапуску";
        await loadSpecs([sym]); stopDirty(sym);
        engineLog(`↺ ${sym}: підхопив відкриту позицію ${x.side === "Buy" ? "лонг" : "шорт"} ${sz} @ ${x.avgPrice} — стоп і вихід`, "mute");
      }
    } catch (e) { setErr("позиції: " + e.message); }
  }
  setInterval(() => lsSet("fxOwn", Object.keys(A).filter(s2 => A[s2].q)), 5000);
  async function boot() {
    await refreshWallet();
    await loadSpecs(P.symbols);
    await adopt();
    connectPub(); connectPriv();
    // clean start: our old orders off, open positions adopted (they get a stop and an exit)
    try {
      const r = await bx("/v5/order/realtime", { query: { category: "linear", settleCoin: "USDT", limit: "50" }, auth: true });
      for (const o of r.list || []) if (String(o.orderLinkId).startsWith("fx")) await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: o.symbol, orderLinkId: o.orderLinkId } }).catch(() => {});
    } catch (e) { /* no keys yet */ }
    if (P.on) pickCoins(true);
  }
  function setOn(on) {
    P.on = !!on; save();
    if (on) { pickCoins(true); engineLog(`🚀 Швидка торгівля увімкнена: режим ${P.mode === "auto" ? "авто (мікро + маркет-мейкінг)" : P.mode === "mm" ? "маркет-мейкінг" : "мікроструктура"}, монети ${P.symbols.join(", ")}`, "up"); }
    else engineLog("■ Швидка торгівля зупинена: нові входи й котирування знято, відкриті позиції закриваю лімітом біля ринку", "mute");
  }
  function setParams(o, caps = {}) {
    const ch = [];
    if (o && Array.isArray(o.symbols) && o.symbols.length && o.autoCoins === undefined && P.autoCoins) o = Object.assign({}, o, { autoCoins: false });
    for (const [k, v] of Object.entries(o || {})) {
      if (k === "on") continue;
      if (NUM[k]) {
        let n = Number(v); if (!Number.isFinite(n)) continue;
        let hi = NUM[k][1];
        if (k === "margin" && caps.margin) hi = Math.min(hi, caps.margin);
        if (k === "leverage" && caps.leverage) hi = Math.min(hi, caps.leverage);
        n = Math.max(NUM[k][0], Math.min(hi, n));
        if (["coinCount", "maxPositions", "leverage", "takeTicks", "maxSpreadTicks"].includes(k)) n = Math.round(n);
        if (P[k] !== n) { ch.push(`${k} ${P[k]}→${n}`); P[k] = n; }
      } else if (BOOL.includes(k) && typeof v === "boolean") { if (P[k] !== v) { ch.push(`${k} ${P[k]}→${v}`); P[k] = v; } }
      else if (k === "mode" && ["micro", "mm", "auto"].includes(v)) { if (P.mode !== v) { ch.push(`режим ${P.mode}→${v}`); P.mode = v; ST.pickedAt = 0; } }
      else if (k === "symbols" && Array.isArray(v)) {
        const list = [...new Set(v.map(x => String(x).toUpperCase()).filter(x => /^[A-Z0-9]{2,30}USDT$/.test(x)))].slice(0, 12);
        if (list.length && list.join() !== P.symbols.join()) { ch.push(`монети ${list.join(",")}`); P.symbols = list; loadSpecs(list).then(resub); }
      }
    }
    if (ch.some(x => x.startsWith("leverage") || x.startsWith("margin"))) { ST.lev = {}; for (const m of Object.values(M)) m.sizeK = 1; }
    if (ch.some(x => /^(autoCoins|coinCount|minTurnoverM|режим)/.test(x)) && P.autoCoins) pickCoins(true);
    save();
    return ch;
  }

  // ---------------- views ----------------
  function stats(list) {
    const w = list.filter(t => t.pnl > 0), gp = w.reduce((a, t) => a + t.pnl, 0), gl = -list.filter(t => t.pnl <= 0).reduce((a, t) => a + t.pnl, 0);
    return { n: list.length, win: list.length ? Math.round(w.length / list.length * 100) : null, pnl: +list.reduce((a, t) => a + t.pnl, 0).toFixed(4),
      fees: +list.reduce((a, t) => a + t.fees, 0).toFixed(4), pf: gl > 0 ? +(gp / gl).toFixed(2) : (gp > 0 ? 9.99 : null),
      avgHoldSec: list.length ? +(list.reduce((a, t) => a + t.holdMs, 0) / list.length / 1000).toFixed(1) : null };
  }
  function series() {
    let wins = 0, losses = 0, best = 0, cur = 0;
    for (let i = TR.length - 1; i >= 0; i--) { if (TR[i].pnl > 0) { if (losses) break; wins++; } else { if (wins) break; losses++; } }
    for (const x of TR) { cur = x.pnl > 0 ? cur + 1 : 0; best = Math.max(best, cur); }
    return { wins, losses, bestWins: best };
  }
  function view() {
    const day = TR.filter(t => t.closedAt >= now() - 86400e3);
    const coins = [...managed()].map(sym => {
      const f = features(sym), m = M[sym] || {}, a = A[sym] || {};
      const os = ordersOf(sym).map(o => ({ role: o.role, side: o.side, price: o.price, qty: o.qty }));
      return { sym, mode: m.mode, f, why: m.why || (m.ready ? "" : "підключаюсь"), thr: thrOf(sym), edgeMult: (P.learn && LEARN.edge[sym]) || 1, tickPct: m.tickPct ? +m.tickPct.toFixed(4) : null,
        pos: a.q ? { q: +a.q.toFixed(8), avg: +Number(a.avg).toPrecision(8), sec: Math.round((now() - a.at) / 1000), pnl: f ? +((f.mid - a.avg) * a.q - a.fees).toFixed(4) : null } : null, orders: os };
    });
    return { avail: ST.avail, params: P, ws: ST.pubUp && now() - ST.lastPub < 5000, priv: ST.privUp, err: now() - (ST.errAt || 0) < 120000 ? ST.err : "", lat: { wsLagMs: Math.round(LAT.wsLag), orderMs: Math.round(LAT.rtt), fillMs: Math.round(LAT.fillMs) },
      coins, stats: stats(TR), day: stats(day), byMode: { micro: stats(TR.filter(t => t.mode === "micro")), mm: stats(TR.filter(t => t.mode === "mm")) }, series: series(),
      learn: { thr: LEARN.thr, edge: LEARN.edge }, trades: TR.slice(-20).reverse(), vol: ST.vol, fees: { maker: MAKER, taker: TAKER } };
  }
  function aiView() {
    const by = (fn) => { const g = {}; for (const t of TR.slice(-400)) { const k = fn(t); (g[k] = g[k] || []).push(t); } return Object.fromEntries(Object.entries(g).map(([k, v]) => [k, stats(v)])); };
    const v = view();
    return { params: P, fees: v.fees, latency: v.lat, stats: v.stats, today: v.day, byMode: v.byMode, bySymbol: by(t => t.sym), bySymbolMode: by(t => t.sym + ":" + t.mode), byExit: by(t => t.how),
      series: v.series, learned: { microThresholdByCoin: LEARN.thr, mmEdgeMultiplierByCoin: LEARN.edge },
      coins: v.coins.map(c => ({ sym: c.sym, mode: c.mode, tickPct: c.tickPct != null ? +Number(c.tickPct).toFixed(4) : null, why: c.why, signal: c.f && { score: c.f.score, ofi: c.f.ofi, microprice: c.f.microN, imb5: c.f.imb, tape: c.f.tfi, spreadTicks: c.f.spreadTicks, volTicks: c.f.vol, activity: c.f.activity }, position: c.pos })),
      candidates: ST.vol, last: TR.slice(-25).map(t => ({ sym: t.sym, mode: t.mode, side: t.side, pnl: t.pnl, fees: t.fees, holdSec: +(t.holdMs / 1000).toFixed(1), how: t.how, sig: t.sig })) };
  }

  // tickPct of every coin from its spec and price
  boot();
  return { params: P, setOn, setParams, view, aiView, pickCoins, _test: { M, A, ORD, onBook, onTrades, onExec, features, closeTrip, TR, LEARN, ST } };
}

module.exports = { create, DEFAULTS };
