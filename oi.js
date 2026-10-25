// OI — торгівля за ростом відкритого інтересу (Open Interest) на Bybit USDT-перпетуалах.
// Щохвилини бот сканує ВСІ монети: як змінився відкритий інтерес і ціна за 5 і 15 хв, яка ставка фінансування.
// Два сценарії (AI і самонавчання вибирають, який працює зараз):
//  • trend   — OI різко росте разом із рухом ціни: у ринок заходять нові гроші в цей бік -> входимо туди ж.
//  • squeeze — OI різко росте, а ціна стоїть: натовп набирає позиції (видно з фінансування) -> входимо проти натовпу, чекаємо вибивання.
// Вхід PostOnly-лімітом на найкращій ціні, тейк лімітом ≥ 10× комісії, стоп на біржі, трейлінг стопу в плюсі,
// вихід, коли OI почав падати (рух вичерпався). PnL — з виконань біржі разом із комісіями.
"use strict";
const WebSocketNode = (() => { try { return require("ws"); } catch (e) { return null; } })();

const DEFAULTS = {
  on: false,
  strategy: "both",        // "trend" | "squeeze" | "both"
  sideMode: "sell",        // "sell" = shorts only (earn on the fall) | "buy" = longs only | "both"
  dcaLevels: 1,            // right after the entry: limit orders to add to the position further against it (short: higher), 0 = off
  dcaStepPct: 2,           // ... every dcaStepPct % from the first entry (short: +2%, +4% ...)
  dcaMult: 1,              // size of each added order × the first entry
  symbols: [],             // coins that are traded / watched right now (filled by the scanner)
  margin: 10,              // USDT margin per trade
  leverage: 5,
  maxPositions: 5,
  // signal
  oiMin: 3,                // OI growth over 15 min, %
  oiMin5: 0.8,             // ... and over the last 5 min, % (the growth is still going on)
  pxMin: 0.5,              // trend: price move over 15 min, %
  pxFlat: 0.4,             // squeeze: price move over 15 min at most, %
  fundMin: 0.01,           // squeeze: |funding rate| at least, % (shows which side the crowd is on)
  minTurnoverM: 20,        // 24 h turnover, million $
  useCvd: true,            // CVD (market buys minus market sells) must confirm: trend - aggressors push the same way; squeeze - the crowd's market orders are absorbed
  cvdMin: 0.05,            // |CVD| / volume over the window at least (0.05 = 5% more buying than selling)
  cvdExit: 0.3,            // CVD of the last 5 min turned this strongly against a position in profit -> leave
  cvdWatch: 25,            // coins with the fastest OI growth whose tape is followed live for the CVD
  maxSpreadPct: 0.1,
  // exits
  feeX: 10,                // take at least feeX × the round-trip fee
  takePct: 2,              // take, %
  stopPct: 1.5,            // stop, %
  trailStart: 1,           // profit at which the stop starts to follow the price, %
  trailGap: 0.6,           // distance of the trailing stop from the best price, %
  oiExit: 1,               // OI fell this much in 5 min while in profit -> leave (the move is over)
  maxHoldMin: 240,
  entryWaitSec: 45,        // unfilled entry limit is cancelled after this (it follows the best price meanwhile)
  chaseSec: 20,            // the limit did not fill in this time and the signal is still alive -> enter at market (a strong move rarely fills a passive order; 0 = never)
  cooldownMin: 30,         // after a trade the coin rests
  scanSec: 60,
  learn: true,             // a strategy that keeps losing is switched off by itself while the other one makes money
};
const NUM = { dcaLevels: [0, 5], dcaStepPct: [0.2, 20], dcaMult: [0.2, 5], margin: [1, 100000], leverage: [1, 50], maxPositions: [1, 20], oiMin: [0.5, 50], oiMin5: [0, 20], pxMin: [0, 20], pxFlat: [0.05, 5], fundMin: [0, 1],
  minTurnoverM: [1, 5000], maxSpreadPct: [0.01, 1], feeX: [1, 50], takePct: [0.2, 30], stopPct: [0.2, 20], trailStart: [0, 20], trailGap: [0.1, 10], oiExit: [0, 20],
  maxHoldMin: [5, 4320], entryWaitSec: [5, 600], chaseSec: [0, 600], cooldownMin: [0, 1440], scanSec: [20, 600], cvdMin: [0, 0.9], cvdExit: [0.05, 1], cvdWatch: [5, 60] };
const BOOL = ["learn", "useCvd"];

function create(host) {
  const { bx, spec, align, sign, log, engineLog, lsGet, lsSet, env, keys } = host;
  const WS = host.WebSocket || WebSocketNode;
  const MAKER = host.makerFee != null ? host.makerFee : 0.02, TAKER = host.takerFee != null ? host.takerFee : 0.055;
  const PUB = host.testWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/public/linear" : "wss://stream.bybit.com/v5/public/linear");
  const PRIV = host.testPrivWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/private" : "wss://stream.bybit.com/v5/private");
  const now = () => Date.now();

  const P = Object.assign({}, DEFAULTS, lsGet("oi", {}));
  const save = () => lsSet("oi", P);
  const TR = lsGet("oiTrades", []);
  const LEARN = Object.assign({ stat: {} }, lsGet("oiLearn", {}));
  const saveLearn = () => lsSet("oiLearn", LEARN);
  const H = {};      // history per symbol: [[t, oi, price]]
  const TK = {};     // last ticker per symbol (REST scan + WS)
  const M = {};      // spec + live state per traded symbol
  const A = {};      // own accounting per symbol (from executions)
  const ORD = {};    // our live orders
  const ST = { pubUp: false, privUp: false, err: "", errAt: 0, scanAt: 0, cands: [], hedge: {}, lev: {}, seenExec: new Set(), lastExecPoll: 0, avail: null, cool: {} };
  const setErr = (t) => { ST.err = t; ST.errAt = now(); };
  const lid = (role) => ("oi" + role + now().toString(36) + Math.random().toString(36).slice(2, 7)).slice(0, 36);
  const roleOf = (id) => String(id || "").slice(2, 3);   // e entry, t take, x exit
  const px = (sym, v, up) => align(v, M[sym].tickStr, up);
  const makerOf = (sym) => (M[sym] && M[sym].maker != null ? M[sym].maker : MAKER);
  const rtFee = (sym) => 2 * Math.max(0.01, makerOf(sym));
  const takeOf = (sym) => Math.max(P.takePct, P.feeX * rtFee(sym));

  // ---------------- open interest history ----------------
  function addPoint(sym, t, oi, price) {
    const h = H[sym] = H[sym] || [];
    if (h.length && t - h[h.length - 1][0] < 20000) h[h.length - 1] = [t, oi, price]; else h.push([t, oi, price]);
    while (h.length && t - h[0][0] > 3 * 3600e3) h.shift();
  }
  function at(sym, ago) {                       // the point closest to `ago` ms back
    const h = H[sym]; if (!h || h.length < 2) return null;
    const t = h[h.length - 1][0] - ago;
    let best = null; for (const p of h) { if (p[0] <= t + 60000) best = p; else break; }
    return best && h[h.length - 1][0] - best[0] >= ago * 0.6 ? best : null;
  }
  function metrics(sym) {
    const h = H[sym]; if (!h || h.length < 2) return null;
    const last = h[h.length - 1], p15 = at(sym, 15 * 60000), p5 = at(sym, 5 * 60000), p60 = at(sym, 60 * 60000);
    if (!p15) return null;
    const ch = (a, b) => (a / b - 1) * 100;
    const tk = TK[sym] || {};
    return { sym, oi15: +ch(last[1], p15[1]).toFixed(2), oi5: p5 ? +ch(last[1], p5[1]).toFixed(2) : null, oi60: p60 ? +ch(last[1], p60[1]).toFixed(2) : null,
      px15: +ch(last[2], p15[2]).toFixed(2), px5: p5 ? +ch(last[2], p5[2]).toFixed(2) : null, price: last[2], oiUsdM: +(last[1] * last[2] / 1e6).toFixed(1),
      funding: tk.funding != null ? +(tk.funding * 100).toFixed(4) : null, turnoverM: tk.turnoverM, spreadPct: tk.spreadPct,
      cvd5: (cvd(sym, 5) || {}).ratio ?? null, cvd15: (cvd(sym, 15) || {}).ratio ?? null, cvd15usd: (cvd(sym, 15) || {}).usd ?? null };
  }
  // ---------------- CVD: cumulative volume delta from the tape (market buys − market sells, in $) per minute ----------------
  const CV = {};   // sym -> { b: Map(minute -> [buy$, sell$]), from: ms since when the tape is complete }
  function cvAdd(sym, t, side, usd) {
    const c = CV[sym] = CV[sym] || { b: new Map(), from: t };
    const k = Math.floor(t / 60000), v = c.b.get(k) || [0, 0];
    if (side > 0) v[0] += usd; else v[1] += usd;
    c.b.set(k, v);
    if (c.b.size > 70) { const old = Math.floor(now() / 60000) - 65; for (const kk of c.b.keys()) if (kk < old) c.b.delete(kk); }
  }
  function cvd(sym, min) {                                        // {ratio -1..1, usd} over the last `min` minutes, null if the tape does not cover it yet
    const c = CV[sym]; if (!c) return null;
    if (now() - c.from < min * 60000 * 0.6) return null;
    const k0 = Math.floor((now() - min * 60000) / 60000);
    let b = 0, s2 = 0; for (const [k, v] of c.b) if (k >= k0) { b += v[0]; s2 += v[1]; }
    return b + s2 > 0 ? { ratio: +((b - s2) / (b + s2)).toFixed(3), usd: Math.round(b - s2) } : null;
  }
  // a coin that just entered the watch list gets its recent tape at once (last 1000 trades)
  async function cvBoot(sym) {
    try {
      const r = await bx("/v5/market/recent-trade", { query: { category: "linear", symbol: sym, limit: "1000" } });
      const list = (r.list || []).map(x => [Number(x.time), x.side === "Buy" ? 1 : -1, Number(x.size) * Number(x.price)]).filter(x => x[0] > 0);
      if (!list.length) return;
      const from = Math.min(...list.map(x => x[0])), c = CV[sym] = CV[sym] || { b: new Map(), from: now() };
      const liveFrom = c.from;
      for (const [t, sd, usd] of list) if (t < liveFrom) cvAdd(sym, t, sd, usd);
      c.from = Math.min(liveFrom, from);
    } catch (e) { /* the live tape fills it */ }
  }
  ST.watch = new Set();
  // the two scenarios; score = how strong
  function signalOf(m) {
    if (!m || m.oi5 == null) return null;
    const out = [];
    const cv = m.cvd15 != null ? m.cvd15 : m.cvd5;                 // CVD over 15 min, or 5 min while the tape history is short
    const cvTxt = cv != null ? `, CVD ${cv > 0 ? "+" : ""}${Math.round(cv * 100)}%` : "";
    // trend: OI and price up together AND market buyers push (CVD > 0) - real new money, not a short squeeze or a thin book
    if (P.strategy !== "squeeze" && m.oi15 >= P.oiMin && m.oi5 >= P.oiMin5 && Math.abs(m.px15) >= P.pxMin && m.px5 != null && Math.sign(m.px5) === Math.sign(m.px15)) {
      const dir = Math.sign(m.px15), okCvd = !P.useCvd || (cv != null && cv * dir >= P.cvdMin);
      if (okCvd) out.push({ strategy: "trend", side: dir > 0 ? "Buy" : "Sell", score: +(m.oi15 * Math.abs(m.px15) * (cv != null ? 1 + Math.abs(cv) * 2 : 1)).toFixed(2),
        why: `OI +${m.oi15}% за 15 хв, ціна ${m.px15 > 0 ? "+" : ""}${m.px15}%${cvTxt} — нові гроші заходять у ${dir > 0 ? "лонг" : "шорт"}` });
      else m.wait = P.useCvd && cv == null ? "чекаю CVD" : `CVD ${cv != null ? Math.round(cv * 100) + "%" : "—"} не підтверджує тренд`;
    }
    // squeeze: OI up, price flat, crowded funding AND the crowd's market orders are absorbed (CVD in the crowd's direction, yet the price does not move)
    if (P.strategy !== "trend" && m.oi15 >= P.oiMin && m.oi5 >= P.oiMin5 && Math.abs(m.px15) <= P.pxFlat && m.funding != null && Math.abs(m.funding) >= P.fundMin) {
      const crowd = Math.sign(m.funding), okCvd = !P.useCvd || (cv != null && cv * crowd >= P.cvdMin);
      if (okCvd) out.push({ strategy: "squeeze", side: crowd > 0 ? "Sell" : "Buy", score: +(m.oi15 * Math.abs(m.funding) * 100 * (cv != null ? 1 + Math.abs(cv) * 2 : 1)).toFixed(2),
        why: `OI +${m.oi15}% за 15 хв, ціна стоїть (${m.px15}%), фінансування ${m.funding}%${cvTxt} — ${crowd > 0 ? "покупці" : "продавці"} б'ють ринком, а ціна не йде: натовп у пастці` });
      else m.wait = P.useCvd && cv == null ? "чекаю CVD" : `CVD ${cv != null ? Math.round(cv * 100) + "%" : "—"} не підтверджує сквіз`;
    }
    // learning: a scenario that keeps losing while the other one makes money is skipped
    const ok = out.filter(s => !(P.learn && disabled(s.strategy)) && (P.sideMode === "both" || (P.sideMode === "sell" ? s.side === "Sell" : s.side === "Buy")));
    if (!ok.length && out.length && P.sideMode !== "both") m.wait = `сигнал ${out[0].side === "Buy" ? "лонг" : "шорт"}, а режим «тільки ${P.sideMode === "sell" ? "Sell" : "Buy"}»`;
    return ok.sort((a, b) => b.score - a.score)[0] || null;
  }
  function disabled(strategy) {
    const me = LEARN.stat[strategy], other = LEARN.stat[strategy === "trend" ? "squeeze" : "trend"];
    return P.strategy === "both" && me && me.n >= 8 && me.ew < 0 && other && other.n >= 8 && other.ew > 0;
  }

  // ---------------- scanner: every scanSec all USDT perps ----------------
  let bootDone = false;
  async function scan() {
    try {
      const r = await bx("/v5/market/tickers", { query: { category: "linear" } });
      const t = now();
      for (const x of r.list || []) {
        if (!/USDT$/.test(x.symbol)) continue;
        const price = Number(x.lastPrice), oi = Number(x.openInterest);
        if (!(price > 0) || !(oi > 0)) continue;
        TK[x.symbol] = { price, bid: Number(x.bid1Price), ask: Number(x.ask1Price), funding: Number(x.fundingRate || 0), turnoverM: Math.round(Number(x.turnover24h) / 1e6),
          spreadPct: Number(x.ask1Price) && Number(x.bid1Price) ? (Number(x.ask1Price) - Number(x.bid1Price)) / price * 100 : null, at: t };
        addPoint(x.symbol, t, oi, price);
      }
      if (!bootDone) { bootDone = true; await bootstrap(); }
      ST.scanAt = t;
      const ms = Object.keys(TK).filter(s => TK[s].turnoverM >= P.minTurnoverM).map(metrics).filter(Boolean);
      // follow the tape (for the CVD) of the coins whose OI grows fastest
      const watch = ms.slice().sort((a, b) => b.oi15 - a.oi15).slice(0, P.cvdWatch).map(m => m.sym);
      for (const s2 of watch) if (!ST.watch.has(s2)) { ST.watch.add(s2); cvBoot(s2); }
      for (const s2 of [...ST.watch]) if (!watch.includes(s2) && !traded().has(s2)) { ST.watch.delete(s2); delete CV[s2]; }
      for (const s2 of traded()) if (!ST.watch.has(s2)) { ST.watch.add(s2); cvBoot(s2); }
      resub();
      ST.cands = ms.map(m => Object.assign(m, { sig: signalOf(m) })).sort((a, b) => (b.sig ? b.sig.score : -1) - (a.sig ? a.sig.score : -1) || b.oi15 - a.oi15).slice(0, 25);
      await entries();
    } catch (e) { setErr("сканер: " + e.message); }
  }
  // history right away (no 15 min warm-up): 5-minute OI and candles of the 80 most traded coins
  async function bootstrap() {
    const top = Object.keys(TK).filter(s => TK[s].turnoverM >= P.minTurnoverM).sort((a, b) => TK[b].turnoverM - TK[a].turnoverM).slice(0, 80);
    let n = 0;
    for (let i = 0; i < top.length; i += 10) {
      await Promise.all(top.slice(i, i + 10).map(async (sym) => {
        try {
          const [o, k] = await Promise.all([
            bx("/v5/market/open-interest", { query: { category: "linear", symbol: sym, intervalTime: "5min", limit: "13" } }),
            bx("/v5/market/kline", { query: { category: "linear", symbol: sym, interval: "5", limit: "13" } }),
          ]);
          const kl = new Map((k.list || []).map(c => [Number(c[0]), Number(c[4])]));
          const pts = (o.list || []).map(x => [Number(x.timestamp), Number(x.openInterest), kl.get(Number(x.timestamp))]).filter(p => p[2] > 0).sort((a, b) => a[0] - b[0]);
          const cur = H[sym] || [];
          H[sym] = pts.filter(p => !cur.length || p[0] < cur[0][0] - 20000).concat(cur);
          n++;
        } catch (e) { /* skip */ }
      }));
    }
    engineLog(`📈 Історія відкритого інтересу завантажена для ${n} монет — сигнали рахуються одразу`, "mute");
  }
  setInterval(() => { if (now() - ST.scanAt >= P.scanSec * 1000 - 500) scan(); }, 5000);

  // ---------------- live prices of the coins we trade (tickers WS) ----------------
  let pub = null, pubPing = null, subs = new Set();
  function connectPub() {
    try { pub = new WS(PUB); } catch (e) { setTimeout(connectPub, 3000); return; }
    pub.on("open", () => { ST.pubUp = true; subs = new Set(); tsubs = new Set(); resub(); clearInterval(pubPing); pubPing = setInterval(() => { try { pub.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    pub.on("message", (raw) => {
      let j; try { j = JSON.parse(String(raw)); } catch (e) { return; }
      if (j.topic && j.topic.startsWith("publicTrade.") && Array.isArray(j.data)) { for (const x of j.data) cvAdd(x.s || j.topic.slice(12), Number(x.T) || now(), x.S === "Buy" ? 1 : -1, Number(x.v) * Number(x.p)); return; }
      if (!j.topic || !j.topic.startsWith("tickers.") || !j.data) return;
      const d = j.data, sym = d.symbol || j.topic.slice(8), tk = TK[sym] = TK[sym] || {};
      if (d.bid1Price) tk.bid = Number(d.bid1Price); if (d.ask1Price) tk.ask = Number(d.ask1Price);
      if (d.lastPrice) tk.price = Number(d.lastPrice); if (d.fundingRate) tk.funding = Number(d.fundingRate);
      if (d.openInterest) tk.oi = Number(d.openInterest);
      tk.live = now();
    });
    pub.on("close", () => { ST.pubUp = false; clearInterval(pubPing); setTimeout(connectPub, 2000); });
    pub.on("error", () => {});
  }
  const traded = () => new Set([...Object.keys(A).filter(s => A[s].q), ...Object.values(ORD).map(o => o.sym)]);
  // tickers for the coins we trade, the tape (CVD) for those and for the watch list
  let tsubs = new Set();
  function resub() {
    if (!pub || !ST.pubUp) return;
    const want = traded(), wantT = new Set([...want, ...(ST.watch || [])]);
    const add = [...want].filter(s => !subs.has(s)), del = [...subs].filter(s => !want.has(s));
    const addT = [...wantT].filter(s => !tsubs.has(s)), delT = [...tsubs].filter(s => !wantT.has(s));
    const sub = add.map(s => `tickers.${s}`).concat(addT.map(s => `publicTrade.${s}`)), uns = del.map(s => `tickers.${s}`).concat(delT.map(s => `publicTrade.${s}`));
    for (let i = 0; i < sub.length; i += 10) pub.send(JSON.stringify({ op: "subscribe", args: sub.slice(i, i + 10) }));
    for (let i = 0; i < uns.length; i += 10) pub.send(JSON.stringify({ op: "unsubscribe", args: uns.slice(i, i + 10) }));
    add.forEach(s => subs.add(s)); del.forEach(s => subs.delete(s)); addT.forEach(s => tsubs.add(s)); delT.forEach(s => tsubs.delete(s));
  }
  setInterval(resub, 2000);

  // ---------------- private: orders + executions ----------------
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
    if (["Filled", "Cancelled", "Rejected", "Deactivated", "PartiallyFilledCanceled"].includes(o.orderStatus)) delete ORD[id];
    else ORD[id].filled = Number(o.cumExecQty || 0);
  }
  function acc(sym) { return A[sym] = A[sym] || { q: 0, avg: 0, real: 0, fees: 0, at: 0, side: null, fills: 0, sig: null, how: "", peak: 0 }; }
  function onExec(x) {
    if (x.category && x.category !== "linear") return;
    if (ST.seenExec.has(x.execId)) return; ST.seenExec.add(x.execId);
    if (ST.seenExec.size > 5000) ST.seenExec = new Set([...ST.seenExec].slice(-2000));
    const sym = x.symbol, ours = String(x.orderLinkId || "").startsWith("oi");
    if (!ours && !(A[sym] && A[sym].q)) return;
    if (x.execType && x.execType !== "Trade") return;
    const qty = Number(x.execQty), p = Number(x.execPrice), fee = Number(x.execFee || 0), s = x.side === "Buy" ? 1 : -1;
    if (!(qty > 0)) return;
    const a = acc(sym), o = ORD[x.orderLinkId];
    if (!a.q) { Object.assign(a, { entry0: p, dcaPlaced: false, dcaFilled: 0, at: now(), real: 0, fees: 0, fills: 0, side: s > 0 ? "Buy" : "Sell", sig: (o && o.sig) || (M[sym] && M[sym].sig) || null, how: "", peak: 0, lockPx: null, slPx: null }); }
    if (ours && M[sym] && M[sym].sizeK < 1) M[sym].sizeK = Math.min(1, M[sym].sizeK * 1.25);
    a.fees += fee; a.fills++;
    if (!a.q || Math.sign(a.q) === s) {
      a.avg = (a.avg * Math.abs(a.q) + p * qty) / (Math.abs(a.q) + qty); a.q += s * qty;
      if (roleOf(x.orderLinkId) === "d" && (!o || o.filled === 0)) { a.dcaFilled = (a.dcaFilled || 0) + 1; engineLog(`➕ ${sym}: добрав ${a.side === "Sell" ? "шорт вище" : "лонг нижче"} @ ${p} — середня ${Number(a.avg).toPrecision(6)}, тейк перераховано`, "mute"); }
    }
    else {
      const close = Math.min(qty, Math.abs(a.q));
      a.real += close * (p - a.avg) * Math.sign(a.q); a.q += s * close;
      const r = roleOf(x.orderLinkId);
      a.how = !ours ? (a.lockPx ? "трейлінг-стоп" : "стоп") : r === "t" ? "тейк" : (a.how || "вихід");
      if (Math.abs(a.q) < 1e-12) { a.q = 0; closeTrip(sym, p); }
    }
    stopDirty(sym);
  }
  function closeTrip(sym, exitPx) {
    const a = A[sym];
    const pnl = +(a.real - a.fees).toFixed(5);
    const t = { sym, side: a.side, strategy: a.sig ? a.sig.strategy : "?", pnl, gross: +a.real.toFixed(5), fees: +a.fees.toFixed(5), exit: exitPx, holdMs: now() - a.at, how: a.how || "вихід",
      peak: +Number(a.peak || 0).toFixed(2), closedAt: now(), sig: a.sig };
    TR.push(t); if (TR.length > 3000) TR.splice(0, TR.length - 3000);
    lsSet("oiTrades", TR);
    ST.cool[sym] = now() + P.cooldownMin * 60000;
    learnFrom(t);
    engineLog(`${t.strategy === "squeeze" ? "🧲" : "📈"} ${sym}: ${a.side === "Buy" ? "лонг" : "шорт"} закрито (${t.how}) ${pnl >= 0 ? "+" : ""}${pnl} USDT · комісії ${t.fees} · ${Math.round(t.holdMs / 60000)} хв`, pnl >= 0 ? "up" : "down");
  }
  function learnFrom(t) {
    const s = LEARN.stat[t.strategy] = LEARN.stat[t.strategy] || { n: 0, pnl: 0, ew: 0 };
    s.n++; s.pnl = +(s.pnl + t.pnl).toFixed(5); s.ew = +(s.n === 1 ? t.pnl : s.ew * 0.85 + t.pnl * 0.15).toFixed(6);
    saveLearn();
    if (P.learn && disabled(t.strategy)) engineLog(`🧪 Сценарій «${t.strategy === "trend" ? "за трендом" : "сквіз"}» збитковий (${s.n} угод), а інший у плюсі — тимчасово торгую тільки інший`, "gold-t");
  }

  // ---------------- orders ----------------
  async function spec2(sym) {
    const m = M[sym] = M[sym] || { sizeK: 1 };
    if (m.tick) return m;
    const s = await spec(sym);
    Object.assign(m, { tickStr: s.tick, tick: Number(s.tick), step: s.step, minQty: Number(s.minQty || 0), minNotional: Number(s.minNotional || 5), maxLev: Number(s.maxLev || 10) });
    try { const r = await bx("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true }); ST.hedge[sym] = (r.list || []).some(x => Number(x.positionIdx) > 0); } catch (e) { ST.hedge[sym] = false; }
    try { const fr = ((await bx("/v5/account/fee-rate", { query: { category: "linear", symbol: sym }, auth: true })).list || [])[0]; if (fr) { m.maker = Number(fr.makerFeeRate) * 100; m.taker = Number(fr.takerFeeRate) * 100; } } catch (e) { /* defaults */ }
    return m;
  }
  function idxFor(sym, side, reduce) {
    if (!ST.hedge[sym]) return { positionIdx: 0, reduceOnly: !!reduce };
    const q = (A[sym] && A[sym].q) || 0;
    if (side === "Sell" && q > 0) return { positionIdx: 1, reduceOnly: true };
    if (side === "Buy" && q < 0) return { positionIdx: 2, reduceOnly: true };
    return { positionIdx: side === "Buy" ? 1 : 2, reduceOnly: false };
  }
  async function refreshWallet() {
    if (!keys || !keys.key) return;
    try { const a = ((await bx("/v5/account/wallet-balance", { query: { accountType: "UNIFIED" }, auth: true })).list || [])[0] || {}; ST.avail = Number(a.totalAvailableBalance || 0); } catch (e) { /* keep */ }
  }
  setInterval(() => { if (P.on) refreshWallet(); }, 5000);
  // size from the money that is free; leverage up for the exchange minimum, but never so high that liquidation comes before the stop
  function sizing(sym, price) {
    const m = M[sym], safe = Math.max(1, Math.floor(80 / Math.max(P.stopPct + (P.dcaLevels > 0 ? P.dcaStepPct * P.dcaLevels : 0), 0.1)));
    const maxLev = Math.min(Math.floor(m.maxLev || P.leverage), safe), base = Math.min(P.leverage, maxLev);
    const minNotional = Math.max(m.minNotional * 1.05, m.minQty * price);
    const free = ST.avail == null ? Infinity : Math.max(0, ST.avail * 0.9);
    const margin = Math.min(P.margin, free) * (m.sizeK || 1);
    let lev = base;
    if (margin * lev < minNotional) lev = Math.min(maxLev, Math.ceil(minNotional * 1.1 / Math.max(margin, 1e-9)));
    return { margin, lev, ok: margin * lev >= minNotional, minNotional, free, maxLev };
  }
  const qtyFor = (sym, price) => {
    const m = M[sym], z = sizing(sym, price); if (!z.ok) return 0;
    const minQ = Math.max(m.minQty, Number(align(z.minNotional / price, m.step, true)));
    return Math.max(minQ, Number(align(z.margin * z.lev / price, m.step, false)));
  };
  async function setLev(sym, lev) {
    if (ST.lev[sym] === lev) return;
    try { await bx("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); }
    catch (e) { if (e.code !== 110043) { log("leverage", sym, e.message); if (e.code === 110013 || e.code === 110012) return; } }
    ST.lev[sym] = lev;
  }
  async function place(sym, side, price, qty, role, { reduce = false, market = false, sig = null } = {}) {
    const id = lid(role), ix = idxFor(sym, side, reduce);
    ORD[id] = { id, sym, side, price: Number(price), qty: Number(qty), role, at: now(), sig, filled: 0 };
    const body = { category: "linear", symbol: sym, side, orderType: market ? "Market" : "Limit", qty: String(qty), orderLinkId: id,
      timeInForce: market ? "IOC" : "PostOnly", positionIdx: ix.positionIdx, reduceOnly: ix.reduceOnly };
    if (!market) body.price = String(price);
    try { await bx("/v5/order/create", { method: "POST", auth: true, body }); return id; }
    catch (e) {
      delete ORD[id];
      if (e.code === 10001) ST.hedge[sym] = !ST.hedge[sym];
      else if ([110007, 110004, 110012, 110044, 110045].includes(e.code) || /not enough|insufficient/i.test(e.message)) {
        M[sym].sizeK = Math.max(0.1, (M[sym].sizeK || 1) * 0.5); refreshWallet();
        engineLog(`💰 ${sym}: біржа каже «мало коштів» — наступний ордер менший`, "mute");
      } else if (e.code !== 110017 && e.code !== 110025) setErr(`${sym}: ${e.message}`);
      return null;
    }
  }
  async function cancel(id) {
    const o = ORD[id]; if (!o) return;
    try { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: o.sym, orderLinkId: id } }); }
    catch (e) { if (e.code === 110001) delete ORD[id]; }
  }
  async function amend(id, price, qty) {
    const o = ORD[id]; if (!o) return;
    const body = { category: "linear", symbol: o.sym, orderLinkId: id };
    if (price != null) body.price = String(price); if (qty != null) body.qty = String(qty);
    try { await bx("/v5/order/amend", { method: "POST", auth: true, body }); if (price != null) o.price = Number(price); if (qty != null) o.qty = Number(qty); }
    catch (e) { if (e.code === 110001) delete ORD[id]; }
  }
  const ordersOf = (sym, role) => Object.values(ORD).filter(o => o.sym === sym && (!role || o.role === role));

  // exchange stop (and the trailing lock) on every position
  const stopQ = new Set();
  const stopDirty = (sym) => stopQ.add(sym);
  async function stopFlush() {
    for (const sym of [...stopQ]) {
      stopQ.delete(sym);
      const a = A[sym]; if (!a || !a.q || !M[sym] || !M[sym].tick) continue;
      const long = a.q > 0;
      const base = P.dcaLevels > 0 && a.entry0 ? a.entry0 : a.avg, dist = P.stopPct + (P.dcaLevels > 0 ? P.dcaStepPct * P.dcaLevels : 0);   // with add-ons: the stop sits beyond the last one
      const sl = a.lockPx ? String(a.lockPx) : px(sym, base * (1 - (long ? 1 : -1) * dist / 100), !long);
      if (a.slPx === sl && a.slQ === a.q) continue;
      try {
        await bx("/v5/position/trading-stop", { method: "POST", auth: true, body: { category: "linear", symbol: sym, stopLoss: sl, slTriggerBy: "LastPrice", tpslMode: "Full", positionIdx: ST.hedge[sym] ? (long ? 1 : 2) : 0 } });
        a.slPx = sl; a.slQ = a.q;
      } catch (e) { if (e.code !== 34040) log("stop", sym, e.message); }
    }
  }
  setInterval(() => { stopFlush().catch(() => {}); }, 1000);

  // ---------------- entries (after every scan) ----------------
  const openCount = () => Object.values(A).filter(a => a.q).length + Object.values(ORD).filter(o => o.role === "e").length;
  async function entries() {
    if (!P.on) return;
    for (const c of ST.cands) {
      if (!c.sig) continue;
      if (openCount() >= P.maxPositions) break;
      const sym = c.sym;
      if ((A[sym] && A[sym].q) || ordersOf(sym).length || (ST.cool[sym] || 0) > now()) continue;
      if (c.spreadPct != null && c.spreadPct > P.maxSpreadPct) continue;
      try {
        const m = await spec2(sym), tk = TK[sym];
        const price = Number(px(sym, c.sig.side === "Buy" ? tk.bid : tk.ask, c.sig.side === "Sell"));
        const qty = qtyFor(sym, price);
        if (!qty) { c.why = `мало коштів (мінімальний ордер ${sizing(sym, price).minNotional.toFixed(2)} USDT)`; continue; }
        await setLev(sym, sizing(sym, price).lev);
        m.sig = Object.assign({}, c.sig, { oi15: c.oi15, oi5: c.oi5, px15: c.px15, funding: c.funding, cvd: c.cvd15 != null ? c.cvd15 : c.cvd5 });
        const id = await place(sym, c.sig.side, price, qty, "e", { sig: m.sig });
        if (id) engineLog(`${c.sig.strategy === "squeeze" ? "🧲" : "📈"} ${sym}: ${c.sig.side === "Buy" ? "лонг" : "шорт"} лімітом ${price} — ${c.sig.why}`, c.sig.side === "Buy" ? "up" : "down");
      } catch (e) { setErr(`${sym}: ${e.message}`); }
    }
  }

  // ---------------- positions and pending entries: every second ----------------
  let busy = false;
  async function manage() {
    if (busy) return; busy = true;
    try {
      for (const sym of traded()) {
        const tk = TK[sym], m = M[sym], a = acc(sym);
        if (!tk || !m || !m.tick || !tk.bid) continue;
        const es = ordersOf(sym, "e"), ts = ordersOf(sym, "t"), xs = ordersOf(sym, "x"), ds = ordersOf(sym, "d");
        if (!a.q) {
          for (const o of [...ts, ...xs, ...ds]) await cancel(o.id);
          for (const o of es) {
            const best = o.side === "Buy" ? tk.bid : tk.ask;
            const c = ST.cands.find(x => x.sym === sym);
            if (now() - o.at > P.entryWaitSec * 1000 || !P.on || (c && !c.sig)) { await cancel(o.id); continue; }
            if (P.chaseSec > 0 && now() - o.at > P.chaseSec * 1000 && !o.filled && c && c.sig && c.sig.side === o.side) {   // still a signal, the move goes on without us: market
              await cancel(o.id);
              if (!ordersOf(sym, "e").length && !(A[sym] && A[sym].q)) { await place(sym, o.side, 0, o.qty, "e", { market: true, sig: o.sig }); engineLog(`⚡ ${sym}: ліміт не виконався за ${P.chaseSec} с, сигнал живий — вхід ринком`, "mute"); }
              continue;
            }
            if (Math.abs(best - o.price) >= m.tick * 0.5 && now() - (o.movedAt || 0) > 3000) { o.movedAt = now(); await amend(o.id, px(sym, best, o.side === "Sell")); }   // follow the best price
          }
          continue;
        }
        for (const o of es) await cancel(o.id);
        const long = a.q > 0, size = Number(align(Math.abs(a.q), m.step, false)), exitSide = long ? "Sell" : "Buy";
        // add-on limits right after the entry: short -> higher (+dcaStepPct, +2×...), long -> lower; a better average price
        if (P.dcaLevels > 0 && !a.dcaPlaced && a.entry0 && P.on && !xs.length) {
          a.dcaPlaced = true;
          const q1 = Math.max(Number(align(Math.abs(a.q) * P.dcaMult, m.step, false)), m.minQty);
          for (let i = 1; i <= P.dcaLevels; i++) {
            const lvl = Number(px(sym, a.entry0 * (1 + (long ? -1 : 1) * P.dcaStepPct * i / 100), !long));
            await place(sym, long ? "Buy" : "Sell", lvl, q1, "d");
          }
          engineLog(`➕ ${sym}: лімітки на добір ${long ? "нижче" : "вище"} входу: ${Array.from({ length: P.dcaLevels }, (_, i) => `${long ? "−" : "+"}${P.dcaStepPct * (i + 1)}%`).join(", ")}`, "mute");
        }
        const price = tk.price || (tk.bid + tk.ask) / 2;
        const mv = (long ? price - a.avg : a.avg - price) / a.avg * 100;
        a.peak = Math.max(a.peak || 0, mv);
        // trailing stop: from trailStart on, the stop follows the best price at trailGap (never below the fees)
        if (P.trailStart > 0 && a.peak >= P.trailStart) {
          const lockPct = Math.max(rtFee(sym) + 0.02, a.peak - P.trailGap);
          const lp = Number(px(sym, a.avg * (1 + (long ? 1 : -1) * lockPct / 100), long));
          if (!a.lockPx) for (const o of ds) await cancel(o.id);   // in profit: no more adding
          if (!a.lockPx || (long ? lp > a.lockPx : lp < a.lockPx)) { if (!a.lockPx) engineLog(`🛡 ${sym}: +${mv.toFixed(2)}% — стоп іде за ціною (в плюсі)`, "mute"); a.lockPx = lp; stopDirty(sym); }
        }
        // the move is over: OI falls while we are in profit -> leave at the best price
        const mt = metrics(sym), held = (now() - a.at) / 60000;
        const leave = !P.on ? "зупинка"
          : mt && mt.oi5 != null && mt.oi5 <= -P.oiExit && mv >= rtFee(sym) * 2 ? `OI падає (${mt.oi5}% за 5 хв) — рух вичерпано`
          : P.useCvd && (!a.sig || a.sig.strategy !== "squeeze") && mt && mt.cvd5 != null && mt.cvd5 * (long ? -1 : 1) >= P.cvdExit && mv >= rtFee(sym) * 2 ? `CVD розвернувся (${Math.round(mt.cvd5 * 100)}% за 5 хв) — ринкові ${long ? "продавці" : "покупці"} перехопили`
          : held > P.maxHoldMin ? "час" : "";
        if (leave && !xs.length) {
          for (const o of [...ts, ...ds]) await cancel(o.id);
          a.how = "вихід: " + leave;
          await place(sym, exitSide, long ? tk.ask : tk.bid, size, "x", { reduce: true });
          continue;
        }
        if (xs.length) { const o = xs[0], touch = long ? tk.ask : tk.bid; if (Math.abs(o.price - touch) >= m.tick * 0.5 && now() - (o.movedAt || 0) > 3000) { o.movedAt = now(); await amend(o.id, touch); } continue; }
        const tp = Number(px(sym, a.avg * (1 + (long ? 1 : -1) * takeOf(sym) / 100), long));
        if (!ts.length) await place(sym, exitSide, tp, size, "t", { reduce: true });
        else if (ts[0].qty !== size || Math.abs(ts[0].price - tp) >= m.tick * 0.5) await amend(ts[0].id, tp, ts[0].qty !== size ? size : null);
      }
    } catch (e) { setErr("позиції: " + e.message); }
    finally { busy = false; }
  }
  setInterval(manage, 1000);

  // ---------------- reconcile with the exchange ----------------
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
      for (const id of Object.keys(ORD)) if (!live.has(id) && now() - ORD[id].at > 5000) delete ORD[id];
      for (const o of ordR.list || []) if (/^(oi|fx|b[xn])/.test(String(o.orderLinkId)) && !ORD[o.orderLinkId]) {   // stray orders: ours from before a restart, the old bots' ones
        try { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: o.symbol, orderLinkId: o.orderLinkId } }); } catch (e) { /* gone */ }
      }
      for (const sym of Object.keys(A)) {
        const rows = (posR.list || []).filter(x => x.symbol === sym && Number(x.size) > 0);
        const q = rows.reduce((s, x) => s + Number(x.size) * (x.side === "Buy" ? 1 : -1), 0), a = A[sym];
        if (Math.abs(q - a.q) > 1e-9) {
          a.mis = (a.mis || 0) + 1;
          if (a.mis >= 2) { if (!q && a.q) { a.how = a.how || "стоп"; a.q = 0; closeTrip(sym, TK[sym] && TK[sym].price); } else if (q) { a.q = q; a.avg = Number(rows[0].avgPrice); stopDirty(sym); } a.mis = 0; }
        } else a.mis = 0;
      }
      lsSet("oiOwn", Object.fromEntries(Object.keys(A).filter(s => A[s].q).map(s => [s, A[s].sig || null])));
    } catch (e) { setErr("звірка: " + e.message); }
  }
  setInterval(reconcile, 5000);
  // conditional (trigger) orders of the old breakout bot are removed
  async function sweepOld() {
    if (!keys || !keys.key) return;
    try {
      const r = await bx("/v5/order/realtime", { query: { category: "linear", settleCoin: "USDT", orderFilter: "StopOrder", limit: "50" }, auth: true });
      for (const o of r.list || []) if (/^b[xn]/.test(String(o.orderLinkId || ""))) { try { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: o.symbol, orderLinkId: o.orderLinkId, orderFilter: "StopOrder" } }); engineLog(`🧹 ${o.symbol}: знято тригерний ордер старого бота`, "mute"); } catch (e) { /* gone */ } }
    } catch (e) { /* ignore */ }
  }
  setInterval(sweepOld, 60000);

  // ---------------- control ----------------
  async function adopt() {
    if (!keys || !keys.key) return;
    try {
      const ownRaw = lsGet("oiOwn", {}), own = Array.isArray(ownRaw) ? Object.fromEntries(ownRaw.map(s => [s, null])) : ownRaw;
      for (const s2 of lsGet("fxOwn", [])) if (!(s2 in own)) own[s2] = { strategy: "старий стакан-бот" };   // positions left by the previous order-book engine
      const r = await bx("/v5/position/list", { query: { category: "linear", settleCoin: "USDT", limit: "200" }, auth: true });
      for (const x of r.list || []) {
        if (!(Number(x.size) > 0) || !(x.symbol in own)) continue;
        const sym = x.symbol; await spec2(sym);
        if (Number(x.positionIdx) > 0) ST.hedge[sym] = true;
        Object.assign(acc(sym), { q: Number(x.size) * (x.side === "Buy" ? 1 : -1), avg: Number(x.avgPrice), at: now(), real: 0, fees: 0, side: x.side, how: "", peak: 0, sig: own[sym] || null });
        stopDirty(sym);
        engineLog(`↺ ${sym}: підхопив свою відкриту позицію ${x.side === "Buy" ? "лонг" : "шорт"} ${x.size} @ ${x.avgPrice}`, "mute");
      }
    } catch (e) { setErr("позиції: " + e.message); }
  }
  async function boot() {
    await refreshWallet(); await adopt();
    connectPub(); connectPriv();
    scan(); sweepOld();
  }
  function setOn(on) {
    P.on = !!on; save();
    engineLog(on ? `🚀 Торгівля за відкритим інтересом увімкнена: ${P.strategy === "both" ? "обидва сценарії (тренд + сквіз)" : P.strategy === "trend" ? "за трендом" : "сквіз"}, до ${P.maxPositions} позицій, тейк ≥ ${P.takePct}%, стоп ${P.stopPct}%` : "■ Торгівлю зупинено: нові входи не відкриваються, відкриті позиції закриваються лімітом біля ринку", on ? "up" : "mute");
    if (on) scan();
  }
  function setParams(o, caps = {}) {
    const ch = [];
    for (const [k, v] of Object.entries(o || {})) {
      if (k === "on") continue;
      if (NUM[k]) {
        let n = Number(v); if (!Number.isFinite(n)) continue;
        let hi = NUM[k][1];
        if (k === "margin" && caps.margin) hi = Math.min(hi, caps.margin);
        if (k === "leverage" && caps.leverage) hi = Math.min(hi, caps.leverage);
        n = Math.max(NUM[k][0], Math.min(hi, n));
        if (["leverage", "maxPositions"].includes(k)) n = Math.round(n);
        if (P[k] !== n) { ch.push(`${k} ${P[k]}→${n}`); P[k] = n; }
      } else if (BOOL.includes(k) && typeof v === "boolean") { if (P[k] !== v) { ch.push(`${k} ${P[k]}→${v}`); P[k] = v; } }
      else if (k === "strategy" && ["trend", "squeeze", "both"].includes(v)) { if (P.strategy !== v) { ch.push(`сценарій ${P.strategy}→${v}`); P.strategy = v; } }
      else if (k === "sideMode" && ["sell", "buy", "both"].includes(v)) { if (P.sideMode !== v) { ch.push(`бік ${P.sideMode}→${v}`); P.sideMode = v; } }
    }
    if (ch.some(x => /^(leverage|margin|stopPct|dca)/.test(x))) { ST.lev = {}; for (const m of Object.values(M)) m.sizeK = 1; }
    save();
    return ch;
  }

  // ---------------- views ----------------
  function stats(list) {
    const w = list.filter(t => t.pnl > 0), gp = w.reduce((a, t) => a + t.pnl, 0), gl = -list.filter(t => t.pnl <= 0).reduce((a, t) => a + t.pnl, 0);
    return { n: list.length, win: list.length ? Math.round(w.length / list.length * 100) : null, pnl: +list.reduce((a, t) => a + t.pnl, 0).toFixed(4), gross: +list.reduce((a, t) => a + (t.gross || 0), 0).toFixed(4),
      fees: +list.reduce((a, t) => a + t.fees, 0).toFixed(4), pf: gl > 0 ? +(gp / gl).toFixed(2) : (gp > 0 ? 9.99 : null), avgHoldMin: list.length ? Math.round(list.reduce((a, t) => a + t.holdMs, 0) / list.length / 60000) : null };
  }
  function positions() {
    return Object.entries(A).filter(([, a]) => a.q).map(([sym, a]) => {
      const tk = TK[sym] || {}, price = tk.price || 0, long = a.q > 0, mv = price ? (long ? price - a.avg : a.avg - price) / a.avg * 100 : null;
      return { sym, side: long ? "Buy" : "Sell", q: Math.abs(a.q), avg: +Number(a.avg).toPrecision(8), price, mv: mv != null ? +mv.toFixed(2) : null, pnl: price ? +((price - a.avg) * a.q - a.fees).toFixed(4) : null,
        peak: +Number(a.peak || 0).toFixed(2), stop: a.slPx || null, trailing: !!a.lockPx, entry0: a.entry0 || null, dcaFilled: a.dcaFilled || 0, dca: ordersOf(sym, "d").map(o => o.price), min: Math.round((now() - a.at) / 60000), strategy: a.sig ? a.sig.strategy : null, oi: metrics(sym) };
    });
  }
  function view() {
    const day = TR.filter(t => t.closedAt >= now() - 86400e3);
    return { params: P, ws: ST.pubUp, priv: ST.privUp, err: now() - ST.errAt < 120000 ? ST.err : "", avail: ST.avail, scanAt: ST.scanAt, coinsScanned: Object.keys(TK).length,
      cands: ST.cands.slice(0, 15), positions: positions(), pending: Object.values(ORD).filter(o => o.role === "e").map(o => ({ sym: o.sym, side: o.side, price: o.price, sec: Math.round((now() - o.at) / 1000), sig: o.sig })),
      stats: stats(TR), day: stats(day), byStrategy: { trend: stats(TR.filter(t => t.strategy === "trend")), squeeze: stats(TR.filter(t => t.strategy === "squeeze")) },
      learn: { stat: LEARN.stat, off: ["trend", "squeeze"].filter(s => P.learn && disabled(s)) }, trades: TR.slice(-25).reverse(), fees: { maker: MAKER, taker: TAKER } };
  }
  function aiView() {
    const v = view(), by = (fn) => { const g = {}; for (const t of TR.slice(-300)) { const k = fn(t); (g[k] = g[k] || []).push(t); } return Object.fromEntries(Object.entries(g).map(([k, x]) => [k, stats(x)])); };
    return { params: P, stats: v.stats, today: v.day, byStrategy: v.byStrategy, byExit: by(t => t.how), bySide: by(t => t.strategy + ":" + t.side), learn: v.learn,
      topOiMovers: v.cands.slice(0, 12).map(c => ({ sym: c.sym, oi15: c.oi15, oi5: c.oi5, oi60: c.oi60, px15: c.px15, px5: c.px5, funding: c.funding, cvd5: c.cvd5, cvd15: c.cvd15, cvd15usd: c.cvd15usd, wait: c.wait, oiUsdM: c.oiUsdM, turnoverM: c.turnoverM, signal: c.sig && { strategy: c.sig.strategy, side: c.sig.side, score: c.sig.score } })),
      positions: v.positions.map(p => ({ sym: p.sym, side: p.side, mv: p.mv, peak: p.peak, min: p.min, strategy: p.strategy, oi5: p.oi && p.oi.oi5, cvd5: p.oi && p.oi.cvd5 })),
      last: TR.slice(-20).map(t => ({ sym: t.sym, strategy: t.strategy, side: t.side, pnl: t.pnl, fees: t.fees, how: t.how, peak: t.peak, min: Math.round(t.holdMs / 60000), sig: t.sig && { oi15: t.sig.oi15, px15: t.sig.px15, funding: t.sig.funding, cvd: t.sig.cvd } })) };
  }

  boot();
  return { params: P, setOn, setParams, view, aiView, _test: { H, TK, A, ORD, M, TR, LEARN, ST, metrics, signalOf, onExec, scan } };
}

module.exports = { create, DEFAULTS };
