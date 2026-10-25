// Super scalping by order flow: live order book (50 levels, 20 ms) and trade tape from Bybit WebSocket.
// Signals: book imbalance, tape delta and bursts, icebergs, absorption, spoofing (walls pulled before being traded), impulse.
// Entries and takes are LIMIT orders (PostOnly = maker). The stop sits on the exchange (stop-market, so it always executes).
// Every parameter can be set by the AI autopilot.
"use strict";

// on the server the "ws" package; in the browser the page passes its own WebSocket wrapper (opts.WebSocket)
const WebSocketNode = typeof require === "function" ? (() => { try { return require("ws"); } catch (e) { return null; } })() : null;

const DEFAULTS = {
  on: false,
  symbols: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "SUIUSDT"],
  margin: 5, leverage: 10,
  tpPct: 0.2,            // take, % from the entry (limit, maker)
  slPct: 0.3,            // stop, % from the entry (on the exchange)
  scoreIn: 0.5,          // signal strength needed to enter (0..1)
  bandPct: 0.15,         // book depth used for the imbalance, % from mid
  burstX: 2.5,           // tape burst: 5 s volume vs normal
  wallX: 6,              // a wall: level size vs median level
  spoofSec: 4,           // a wall pulled within this time without trades = spoof
  icebergX: 3,           // traded at a level vs its visible size = iceberg
  entryWaitSec: 8,       // an unfilled entry limit is cancelled after this
  holdMaxSec: 300,       // after this the take is pulled to the best price to get out
  maxPositions: 2,
  cooldownSec: 60,
  dailyLossPct: 0,       // 0 = no daily loss limit (owner's choice); >0 stops new entries for the day after this loss, % of equity
  ride: true,            // move the take further while the impulse goes on (old mode, used only when trail is off)
  trail: true,           // trailing take: reaching the take does not close the trade; the take runs ahead of the price, the stop follows it in profit,
                         // the exit comes only when the price pulls back by trailPct from its best point -> the whole impulse is taken
  trailPct: 0,           // pull-back from the best point that ends the trade, % (0 = auto: half of the take, at least 0.04%)
  useIceberg: true, useAbsorption: false, useSpoof: false,   // spoofing and absorption gave losses: switched off for good (owner's choice)
  streakFix: true,       // after streakN losing trades in a row something is changed (filter, mirror, style, exits, coins...); the bot learns which change brings a plus series
  streakN: 3,
  // protection against the usual scalping losses
  needAgree: true,       // book and tape must point the same way
  cancelOnFade: true,    // pull an unfilled entry when the signal fades (avoids fills only when the price runs against us)
  flipExit: true,        // the flow turned against the trade: leave by a limit at the best price before the stop
  timeStopSec: 90,       // no progress in this time: leave near break-even (limit)
  adaptive: true,        // take/stop follow the last minute's range
  autoPrune: true,       // pause losing coins, switch off losing signal types
  // coins: the most volatile liquid coins, re-picked every few minutes
  autoCoins: true,
  coinCount: 6,
  minTurnoverM: 200,     // 24 h turnover, million $: order-book scalping needs a deep book (thin coins have fake walls and jumps)
  pickEveryMin: 10,
  // inversion: when the statistics say the normal mode loses, trade the mirror (opposite side, take and stop swapped)
  invert: "auto",        // "off" | "on" | "auto"
  invertMin: 20,         // trades in a mode before judging it
  // learning from own trades
  learn: true,           // skip entries whose most similar past trades lost money (k nearest neighbours)
  learnMin: 40,          // trades with recorded signals needed before the filter acts
  optExits: true,        // re-tune take/stop on recorded price paths every optEvery trades
  optEvery: 15,
  style: "flow",         // "flow" = book + tape + icebergs/absorption/spoof; "pressure" = book pressure only, tiny frequent takes
  pressLevels: 5,        // levels per side counted for the pressure
};
// preset of the "book pressure" style: tiny takes just above the maker fees, quick exits when the pressure goes away
const PRESSURE = { style: "pressure", tpPct: 0.08, slPct: 0.25, scoreIn: 0.55, entryWaitSec: 5, timeStopSec: 45, holdMaxSec: 120,
  ride: false, trail: true, adaptive: false, flipExit: true, cancelOnFade: true, needAgree: false, maxPositions: 3, cooldownSec: 20 };

// host: functions of the environment. On the server they talk to the breakout engine (via ev); a standalone page passes its own.
//   bx(path, opts) -> Bybit REST result, spec(sym) -> {tick, step, minQty, minNotional, maxLev}, align(v, step, up) -> string,
//   reserve(symbols, amount), freeCoins() -> number, equity() -> number, busy() -> [symbols of other bots' positions]
function create({ ev, log, engineLog, lsGet, lsSet, env, testWs, host, WebSocket: WSImpl }) {
  const WebSocket = WSImpl || WebSocketNode;
  const H = Object.assign({
    bx: (path, opts) => ev(`bx(${JSON.stringify(path)}, ${JSON.stringify(opts)})`),
    spec: (sym) => ev(`spec(${JSON.stringify(sym)})`),
    align: (v, step, up) => ev(`align(${v}, ${JSON.stringify(String(step))}, ${up})`),
    reserve: (list, amount) => { try { ev(`window.__ssSyms = ${JSON.stringify(list)}; window.__ssReserve = ${amount}`); } catch (e) { /* ignore */ } },
    freeCoins: () => ev(`(async () => { let n = 0; for (const s of SETUPS.filter(x => x.state === "armed" && (window.__ssSyms || []).includes(x.symbol))) {
        await cancelAll(s); await cancelDip(s); SETUPS = SETUPS.filter(x => x !== s); n++; } if (n) saveSetups(); return n; })()`),
    equity: () => Number(ev("(window.__eq || 0)")) || 0,
    busy: () => JSON.parse(ev(`JSON.stringify(POS.map(p => p.symbol).concat(SETUPS.filter(s => s.state !== "armed").map(s => s.symbol)))`) || "[]"),
  }, host || {});
  let ST_PICK_NOW = false;
  const P = Object.assign({}, DEFAULTS, lsGet("ss", {}));
  if (!P.v) { if (P.scoreIn === 0.6 || P.scoreIn === 0.62) P.scoreIn = DEFAULTS.scoreIn; P.v = 2; }   // the first threshold was too strict for the live market
  if (P.v < 4) { P.dailyLossPct = 0; P.v = 4; }
  if (P.v < 5) { P.trail = true; P.v = 5; }
  if (P.v < 6) { P.useSpoof = false; P.useAbsorption = false; P.dailyLossPct = 0; P.streakFix = true; if (lsGet("ssPausedUntil", 0) > 0) { P.on = true; lsSet("ssPausedUntil", 0); } P.v = 6; }
  P.useSpoof = false; P.useAbsorption = false; P.dailyLossPct = 0;   // never: the owner removed them (losses) and any stop of trading                     // the owner wants the take trailed to take the whole move                 // the owner switched the daily limit off
  if (P.v < 3) { if (!P.minTurnoverM || P.minTurnoverM < DEFAULTS.minTurnoverM) P.minTurnoverM = DEFAULTS.minTurnoverM; P.v = 3; ST_PICK_NOW = true; }
  const ST = { hedge: !!lsGet("ssHedge", false), trades: lsGet("ssTrades", []), dayStart: 0, dayPnl: 0, stoppedForDay: false, wsUp: false, lastMsg: 0, err: "" };
  const M = {};           // per symbol market state
  const POSN = {};        // our live entries/positions per symbol
  const COOL = {};
  let ws = null, pingT = null, reconnectT = null;
  const save = () => { lsSet("ss", P); lsSet("ssTrades", ST.trades.slice(-400)); };

  // ---------------- market data ----------------
  function mk(sym) {
    return M[sym] || (M[sym] = { bids: new Map(), asks: new Map(), tape: [], walls: new Map(), traded: new Map(), shown: new Map(),
      spoofs: [], icebergs: [], prices: [], bid: 0, ask: 0, last: 0, tick: 0, ready: false });
  }
  function applyBook(sym, d, snapshot) {
    const m = mk(sym);
    if (snapshot) { m.bids.clear(); m.asks.clear(); m.walls.clear(); }   // a full book: no wall history to compare with
    const now = Date.now();
    for (const [side, book, key] of [["Buy", m.bids, "b"], ["Sell", m.asks, "a"]]) {
      for (const [ps, ss] of d[key] || []) {
        const size = Number(ss), price = Number(ps);
        const prev = book.get(price) || 0;
        if (size === 0) book.delete(price); else book.set(price, size);
        m.shown.set(side + price, Math.max(m.shown.get(side + price) || 0, size));
        // spoofing: a big wall that disappears quickly with almost nothing traded at that price
        const w = m.walls.get(side + price);
        if (w && (size === 0 || size < w.size * 0.3)) {
          const traded = m.traded.get(side + price) || 0;
          // the price went through the wall = it was eaten, not pulled
          const crossed = m.last && (side === "Buy" ? m.last <= price : m.last >= price);
          if (!snapshot && !crossed && now - w.t < P.spoofSec * 1000 && traded < w.size * 0.2) m.spoofs.push({ side, price, size: w.size, t: now });
          m.walls.delete(side + price);
        } else if (!w && !snapshot && size > 0 && size > prev * 3) {   // a level suddenly grew: maybe a wall
          // remember new big levels as walls (checked against the median below)
          m.walls.set(side + price, { size, t: now, pending: true });
        }
      }
    }
    const bb = Math.max(...m.bids.keys(), 0), ba = Math.min(...m.asks.keys(), Infinity);
    // book pressure: notional of the nearest levels on each side, and the queue at the best prices (sampled every 100 ms)
    if (bb > 0 && ba < Infinity && now - (m.pressAt || 0) >= 100) {
      m.pressAt = now;
      const nb = [...m.bids.entries()].sort((a, b) => b[0] - a[0]).slice(0, P.pressLevels);
      const na = [...m.asks.entries()].sort((a, b) => a[0] - b[0]).slice(0, P.pressLevels);
      const bs = nb.reduce((s, [p, q]) => s + p * q, 0), as = na.reduce((s, [p, q]) => s + p * q, 0);
      const qb = (m.bids.get(bb) || 0) * bb, qa = (m.asks.get(ba) || 0) * ba;
      (m.press = m.press || []).push([now, bs + as ? bs / (bs + as) : 0.5, qb + qa ? qb / (qb + qa) : 0.5]);
      m.press = m.press.filter(x => now - x[0] < 5000);
    }
    if (bb > 0 && ba < Infinity) { m.bid = bb; m.ask = ba; m.ready = true; }
    if (!m.tick && m.bids.size > 2) {
      const ps = [...m.bids.keys()].sort((a, b) => b - a);
      m.tick = Math.min(...ps.slice(0, 10).map((p, i, a) => i ? +(a[i - 1] - p).toFixed(10) : Infinity).filter(x => x > 0)) || 0;
    }
    // keep only real walls: size >= wallX x median level size
    const sizes = [...m.bids.values(), ...m.asks.values()].sort((a, b) => a - b);
    const med = sizes[Math.floor(sizes.length / 2)] || 0;
    for (const [k, w] of m.walls) {
      if (w.pending) { if (w.size >= P.wallX * med && med > 0) w.pending = false; else m.walls.delete(k); }
    }
    m.spoofs = m.spoofs.filter(x => now - x.t < 60000);
  }
  function applyTrades(sym, list) {
    const m = mk(sym), now = Date.now();
    for (const t of list) {
      const p = Number(t.p), v = Number(t.v);
      m.tape.push({ t: Number(t.T) || now, side: t.S, usd: p * v, p, v });
      const lvlSide = t.S === "Sell" ? "Buy" : "Sell";           // a market sell hits the bid, a market buy lifts the ask
      m.traded.set(lvlSide + p, (m.traded.get(lvlSide + p) || 0) + v);
      m.last = p;
      // iceberg: much more traded against a level than was ever shown there, and the level is still standing
      const shown = m.shown.get(lvlSide + p) || 0;
      const stillThere = (lvlSide === "Buy" ? m.bids : m.asks).has(p);
      const done = m.traded.get(lvlSide + p);
      if (shown > 0 && stillThere && done > P.icebergX * shown && !m.icebergs.some(x => x.price === p && now - x.t < 20000)) {
        m.icebergs.push({ side: lvlSide, price: p, traded: done, shown, t: now });
      }
    }
    m.prices.push([now, m.last]);
    const cut = now - 60000;
    m.tape = m.tape.filter(x => x.t >= cut);
    m.prices = m.prices.filter(x => x[0] >= cut);
    m.icebergs = m.icebergs.filter(x => now - x.t < 60000);
    if (m.traded.size > 4000) m.traded.clear();
  }

  // ---------------- signals ----------------
  function signals(sym) {
    const m = M[sym];
    if (!m || !m.ready || !m.last) return null;
    const now = Date.now(), mid = (m.bid + m.ask) / 2, band = mid * P.bandPct / 100;
    let bs = 0, as = 0;
    for (const [p, s] of m.bids) if (p >= mid - band) bs += s * p;
    for (const [p, s] of m.asks) if (p <= mid + band) as += s * p;
    const imb = bs + as > 0 ? bs / (bs + as) : 0.5;
    const win = (ms) => m.tape.filter(x => x.t >= now - ms);
    const sum = (a, side) => a.filter(x => !side || x.side === side).reduce((s, x) => s + x.usd, 0);
    const t5 = win(5000), t10 = win(10000);
    const buy5 = sum(t5, "Buy"), sell5 = sum(t5, "Sell"), vol5 = buy5 + sell5;
    // normal 5 s volume over the time we actually have (up to a minute); no signals until 20 s of tape
    const covered = m.tape.length ? Math.min(60000, now - m.tape[0].t) : 0;
    if (covered < 20000) return null;
    const base = sum(m.tape) / Math.max(1, covered / 5000) || 1;
    const burst = vol5 / base;
    const delta = vol5 ? (buy5 - sell5) / vol5 : 0;
    const p10 = (m.prices.find(x => x[0] >= now - 10000) || [0, m.last])[1];
    const move10 = p10 ? (m.last - p10) / p10 * 100 : 0;
    const buy10 = sum(t10, "Buy"), sell10 = sum(t10, "Sell");
    // absorption: heavy aggression one way but the price does not give in
    const heavy = (buy10 + sell10) / (base * 2) > P.burstX;
    const absBull = P.useAbsorption && heavy && sell10 > buy10 * 1.8 && move10 >= -0.02;
    const absBear = P.useAbsorption && heavy && buy10 > sell10 * 1.8 && move10 <= 0.02;
    const ice = P.useIceberg ? m.icebergs.filter(x => now - x.t < 20000 && Math.abs(x.price - mid) / mid < 0.003) : [];
    const iceBid = ice.some(x => x.side === "Buy"), iceAsk = ice.some(x => x.side === "Sell");
    const sp = P.useSpoof ? m.spoofs.filter(x => now - x.t < 20000) : [];
    const spoofBid = sp.filter(x => x.side === "Buy").length, spoofAsk = sp.filter(x => x.side === "Sell").length;
    const spreadTicks = m.tick ? Math.round((m.ask - m.bid) / m.tick) : 1;
    // scores 0..1
    const impulseUp = burst >= P.burstX && delta > 0.3 && move10 > 0;
    const impulseDn = burst >= P.burstX && delta < -0.3 && move10 < 0;
    let long = 0.30 * Math.max(0, (imb - 0.5) * 2) + 0.25 * Math.max(0, delta) + (impulseUp ? 0.2 : 0)
      + (iceBid ? 0.12 : 0) + (absBull ? 0.15 : 0) + (spoofAsk ? 0.08 : 0) - (spoofBid ? 0.15 : 0) - (iceAsk ? 0.1 : 0);
    let short = 0.30 * Math.max(0, (0.5 - imb) * 2) + 0.25 * Math.max(0, -delta) + (impulseDn ? 0.2 : 0)
      + (iceAsk ? 0.12 : 0) + (absBear ? 0.15 : 0) + (spoofBid ? 0.08 : 0) - (spoofAsk ? 0.15 : 0) - (iceBid ? 0.1 : 0);
    // "pressure" style: the side whose book is heavier for the last 2 s (and whose best-price queue is longer),
    // with the tape not pushing against it
    let press = null;
    const ph = (m.press || []).filter(x => now - x[0] < 2000);
    if (ph.length >= 5) {
      const lvl = ph.reduce((a, x) => a + x[1], 0) / ph.length, que = ph.reduce((a, x) => a + x[2], 0) / ph.length;
      const steady = ph.filter(x => (x[1] - 0.5) * (lvl - 0.5) > 0).length / ph.length;   // how much of the time it pointed the same way
      press = { lvl: +lvl.toFixed(2), que: +que.toFixed(2), steady: +steady.toFixed(2) };
      if (P.style === "pressure") {
        const tape = Math.max(-1, Math.min(1, delta * 2));
        long = 0.55 * Math.max(0, (lvl - 0.5) * 2) + 0.3 * Math.max(0, (que - 0.5) * 2) + 0.15 * steady * (lvl > 0.5 ? 1 : 0) + (tape > 0 ? 0.1 * tape : 0.25 * tape);
        short = 0.55 * Math.max(0, (0.5 - lvl) * 2) + 0.3 * Math.max(0, (0.5 - que) * 2) + 0.15 * steady * (lvl < 0.5 ? 1 : 0) + (tape < 0 ? -0.1 * tape : -0.25 * tape);
      }
    } else if (P.style === "pressure") { long = 0; short = 0; }
    long = Math.max(0, Math.min(1, long)); short = Math.max(0, Math.min(1, short));
    return {
      sym, bid: m.bid, ask: m.ask, last: m.last, spreadTicks, imb: +imb.toFixed(2), delta: +delta.toFixed(2), burst: +burst.toFixed(1),
      move10: +move10.toFixed(3), press, iceBid, iceAsk, absBull, absBear, spoofBid, spoofAsk, long: +long.toFixed(2), short: +short.toFixed(2),
    };
  }

  // ---------------- orders ----------------
  const bx = (path, opts) => H.bx(path, opts);
  const specOf = (sym) => H.spec(sym);
  const alignP = (v, step, up) => H.align(v, step, up);
  const lid = (sym, tag) => ("ss" + tag + sym.slice(0, 10) + Date.now().toString(36) + Math.floor(Math.random() * 99)).slice(0, 36);
  // position mode of the account: one-way (positionIdx 0) or hedge (1 = long side, 2 = short side); found out on the first order
  const idxFor = (posSide) => ST.hedge ? (posSide === "Buy" ? 1 : 2) : 0;
  const idxErr = (e) => /position idx|position mode/i.test(String(e && e.message || e));
  async function withIdx(fn) {
    try { return await fn(); }
    catch (e) {
      if (!idxErr(e)) throw e;
      ST.hedge = !ST.hedge; lsSet("ssHedge", ST.hedge);
      log(`superscalp: position mode → ${ST.hedge ? "hedge" : "one-way"}`);
      return fn();
    }
  }
  async function limit(sym, side, price, qty, reduce, tag, postOnly = true) {
    const id = lid(sym, tag);
    const posSide = reduce ? (side === "Buy" ? "Sell" : "Buy") : side;   // a reduce order closes the opposite side
    await withIdx(() => bx("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: sym, side, orderType: "Limit", qty: String(qty), price: String(price),
      timeInForce: postOnly ? "PostOnly" : "GTC", reduceOnly: !!reduce, orderLinkId: id, positionIdx: idxFor(posSide) } }));
    return id;
  }
  const setStop = (p, price) => withIdx(() => bx("/v5/position/trading-stop", { method: "POST", auth: true,
    body: { category: "linear", symbol: p.sym, stopLoss: String(price), slTriggerBy: "MarkPrice", tpslMode: "Full", positionIdx: idxFor(p.side) } }));
  const cancel = (sym, id) => bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: sym, orderLinkId: id } }).catch(() => {});
  const amend = (sym, id, price) => bx("/v5/order/amend", { method: "POST", auth: true, body: { category: "linear", symbol: sym, orderLinkId: id, price: String(price) } });

  async function enter(sym, side, sig) {
    // enough free money for this entry? (a limit entry reserves its margin on Bybit right away)
    const w = await bx("/v5/account/wallet-balance", { query: { accountType: "UNIFIED" }, auth: true });
    const free = Number(((w.list || [])[0] || {}).totalAvailableBalance || 0);
    if (free < P.margin * 1.05) {
      ST.noMoneyUntil = Date.now() + 60000;
      ST.noMoney = `вільно ${free.toFixed(2)} USDT, на вхід треба ${(P.margin * 1.05).toFixed(2)}`;
      return;
    }
    ST.noMoney = "";
    const sp = await specOf(sym);
    const lev = Math.min(P.leverage, Math.floor(sp.maxLev));
    const price = side === "Buy" ? sig.bid : sig.ask;                // join the best price: maker
    const qty = await alignP(P.margin * lev / price, sp.step, false);
    if (Number(qty) < sp.minQty || Number(qty) * price < sp.minNotional) { ST.err = `${sym}: замала маржа для мін. ордера`; return; }
    try { await bx("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); } catch (e) { /* not modified */ }
    const id = await limit(sym, side, price, qty, false, "E");
    ST.lastEntryAt = Date.now();
    POSN[sym] = { sym, side, state: "entry", entryId: id, price, qty, at: Date.now(), sig, tick: sp.tick, step: sp.step, inv: inverted() };
    engineLog(`⚡⚡ ${sym}: ${side === "Buy" ? "лонг" : "шорт"} лімітом ${price} × ${qty} · стакан ${sig.imb} · дельта ${sig.delta} · сплеск ×${sig.burst}`
      + (sig.iceBid || sig.iceAsk ? " · айсберг" : "") + (sig.absBull || sig.absBear ? " · поглинання" : "") + (sig.spoofBid || sig.spoofAsk ? " · спуфінг" : ""), "gold-t");
  }

  // trailing take: runs every 0.5 s on the live price.
  //  - below the take level: nothing changes (the normal stop protects the trade)
  //  - the take level is reached: the stop jumps into profit (at least the fees), then follows the best point at a distance;
  //    the resting limit take is kept one take ahead of the best point, so a spike is still caught
  //  - the price pulls back by the gap from its best point: leave with a limit at the best price (maker); the stop backs it up
  async function trailStep(p, mv) {
    const long = p.side === "Buy";
    p.peak = Math.max(p.peak || 0, mv);
    if (!p.trailOn) {
      if (p.peak < p.tpPct) return;
      p.trailOn = true; p.rides = 0;
      engineLog(`🎯 ${p.sym}: тейк +${p.tpPct}% досягнуто — не закриваю, тягну трейлінгом (вихід на відкаті ${p.gap}% від найкращої ціни)`, "up");
    }
    const m = M[p.sym];
    if (mv <= p.peak - p.gap) {
      const best = long ? m.ask : m.bid;
      try {
        await amend(p.sym, p.tpId, best); p.tp = best; p.trailExit = true; p.leaving = "трейлінг";
        engineLog(`🎯 ${p.sym}: відкат від максимуму +${p.peak.toFixed(3)}% — забираю лімітом ${best} (≈ +${mv.toFixed(3)}%)`, "up");
      } catch (e) { /* the take may have just filled */ }
      return;
    }
    // stop: behind the best point by 1.5 gaps, never below the fees, only forward
    const lock = Math.max(LOCK_MIN, p.peak - p.gap * 1.5);
    if (p.lockPct == null || lock >= p.lockPct + Math.max(0.01, p.gap * 0.3)) {
      const st = await alignP(p.entry * (1 + (long ? 1 : -1) * lock / 100), p.tick, long);
      try { await setStop(p, st); p.sl = Number(st); p.lockPct = lock; p.beDone = true; } catch (e) { /* mark price already beyond: the limit exit handles it */ }
    }
    // keep the resting take one take ahead of the best point
    const want = p.peak + p.tpPct, have = (long ? p.tp - p.entry : p.entry - p.tp) / p.entry * 100;
    if (want > have + p.tpPct * 0.5) {
      const ntp = await alignP(p.entry * (1 + (long ? 1 : -1) * want / 100), p.tick, long);
      try { await amend(p.sym, p.tpId, ntp); p.tp = Number(ntp); p.rides = (p.rides || 0) + 1; } catch (e) { /* filled */ }
    }
  }

  async function onFilled(p, entryPx) {
    const long = p.side === "Buy";
    p.entry = entryPx; p.state = "open"; p.openedAt = Date.now();
    // adaptive: scale take and stop by the last minute's range (0.25% = normal); never a take below 0.1%
    let f = 1;
    const m = M[p.sym];
    if (P.adaptive && m && m.prices.length > 10) {
      const ps = m.prices.map(x => x[1]), rng = (Math.max(...ps) - Math.min(...ps)) / entryPx * 100;
      f = Math.max(0.6, Math.min(1.6, rng / 0.25));
    }
    // the mirror of the normal trade: take and stop swap places
    const tpBase = p.inv ? P.slPct : P.tpPct, slBase = p.inv ? P.tpPct : P.slPct;
    p.tpPct = Math.max(P.style === "pressure" ? 0.06 : 0.1, +(tpBase * f).toFixed(3)); p.slPct = +(slBase * f).toFixed(3); p.f = f;
    // trailing: the resting limit take sits one more take further (it only catches a sudden spike); the real exit is the trailing below
    p.trail = !!P.trail; p.peak = 0; p.gap = trailGap(p.tpPct);
    const tp = await alignP(entryPx * (1 + (long ? 1 : -1) * (p.trail ? p.tpPct * 2 : p.tpPct) / 100), p.tick, long);
    const sl = await alignP(entryPx * (1 - (long ? 1 : -1) * p.slPct / 100), p.tick, !long);
    p.tp = Number(tp); p.sl = Number(sl); p.lockPct = null;
    try { await setStop(p, sl); }
    catch (e) { engineLog(`⚡⚡ ${p.sym}: стоп не поставився: ${e.message}`, "down"); }
    try { p.tpId = await limit(p.sym, long ? "Sell" : "Buy", tp, p.qty, true, "T"); }
    catch (e) { p.tpId = await limit(p.sym, long ? "Sell" : "Buy", tp, p.qty, true, "T", false).catch(() => null); }
    engineLog(`⚡⚡ ${p.sym}: вхід виконано @ ${entryPx} · ${p.trail ? `трейлінг тейку від +${p.tpPct}% (відкат ${p.gap}%)` : `тейк лімітом ${tp}`} · стоп ${sl}`, long ? "up" : "down");
  }

  async function record(p, closed) {
    let pnl = 0, exitPx = null;
    try {
      const r = await bx("/v5/position/closed-pnl", { query: { category: "linear", symbol: p.sym, startTime: String(p.openedAt - 60000), limit: "20" }, auth: true });
      const rows = (r.list || []).filter(x => Number(x.createdTime) >= p.openedAt - 5000);
      pnl = rows.reduce((a, x) => a + Number(x.closedPnl || 0), 0);
      exitPx = rows.length ? Number(rows[0].avgExitPrice) : null;
    } catch (e) { /* keep 0 */ }
    const t = { sym: p.sym, side: p.side, entry: p.entry, exit: exitPx, pnl: +pnl.toFixed(4), roi: +(pnl / P.margin * 100).toFixed(2), how: closed,
      openedAt: p.openedAt, closedAt: Date.now(), holdSec: Math.round((Date.now() - p.openedAt) / 1000), rides: p.rides || 0,
      sig: p.sig && { imb: p.sig.imb, delta: p.sig.delta, burst: p.sig.burst, ice: p.sig.iceBid || p.sig.iceAsk, abs: p.sig.absBull || p.sig.absBear, spoof: !!(p.sig.spoofBid || p.sig.spoofAsk), score: Math.max(p.sig.long, p.sig.short) },
      inv: !!p.inv, f: p.f || 1, path: (p.path || []).slice(-600),
      mfe: p.path && p.path.length ? Math.max(...p.path) : null, mae: p.path && p.path.length ? Math.min(...p.path) : null,
      feat: p.sig ? feat(p.sig, p.inv ? (p.side === "Buy" ? "Sell" : "Buy") : p.side) : null,
      cfg: { tpPct: P.tpPct, slPct: P.slPct, scoreIn: P.scoreIn, burstX: P.burstX, ride: P.ride, trail: !!p.trail }, peak: p.peak != null ? +p.peak.toFixed(3) : null };
    if (p.leaving) t.how = closed === "тейк" ? `вихід: ${p.leaving}` : closed;
    t.cfg.adaptiveTp = p.tpPct || null;
    ST.trades.push(t); ST.dayPnl += t.pnl; save();
    if (P.autoPrune) prune(p.sym);
    judgeMode();
    streakCheck();
    if (P.optExits && ST.trades.filter(x => x.path && x.path.length).length % P.optEvery === 0) optimizeExits(false);
    engineLog(`⚡⚡ ${p.sym}: закрито (${closed}) PnL ${t.pnl >= 0 ? "+" : ""}${t.pnl} USDT за ${t.holdSec} с`, t.pnl >= 0 ? "up" : "down");
    COOL[p.sym] = Date.now() + (t.pnl < 0 ? P.cooldownSec * 2 : P.cooldownSec) * 1000;
    delete POSN[p.sym];
  }

  // the breakout bot keeps away from our coins; its waiting orders on them are removed
  const W_SET = (list) => H.reserve(list, P.on ? P.maxPositions * P.margin * 1.1 : 0);
  async function freeCoins() {
    try {
      const n = await H.freeCoins();
      if (n) engineLog(`⚡⚡ Суперскальпінг забрав свої монети: знято ${n} заявок пробою на них`, "mute");
    } catch (e) { /* next time */ }
  }

  // ---------------- learning ----------------
  // features of a signal seen from the side of the trade (0..1-ish ranges)
  function feat(sig, side) {
    const L = side === "Buy";
    return [
      L ? sig.imb : 1 - sig.imb,                                   // book on our side
      (L ? sig.delta : -sig.delta + 0) * 0.5 + 0.5,               // tape on our side
      Math.min(1, Math.log(1 + Math.max(0, sig.burst || 0)) / 2.5), // burst
      (L ? (sig.iceBid ? 1 : 0) - (sig.iceAsk ? 1 : 0) : (sig.iceAsk ? 1 : 0) - (sig.iceBid ? 1 : 0)) * 0.5 + 0.5,
      (L ? (sig.absBull ? 1 : 0) - (sig.absBear ? 1 : 0) : (sig.absBear ? 1 : 0) - (sig.absBull ? 1 : 0)) * 0.5 + 0.5,
      (L ? (sig.spoofAsk ? 1 : 0) - (sig.spoofBid ? 1 : 0) : (sig.spoofBid ? 1 : 0) - (sig.spoofAsk ? 1 : 0)) * 0.5 + 0.5,
      Math.max(0, Math.min(1, ((L ? sig.move10 : -sig.move10) || 0) / 0.6 + 0.5)),   // move of the last 10 s
      Math.min(1, (sig.spreadTicks || 1) / 4),
    ];
  }
  // expected result (% of the trade margin) of the k most similar past trades of the same mode
  function expect(sig, side, inv) {
    if (!P.learn) return null;
    const hist = ST.trades.filter(t => t.feat && !!t.inv === !!inv).slice(-400);
    if (hist.length < P.learnMin) return null;
    const f = feat(sig, side);
    const k = Math.min(20, Math.floor(hist.length / 2));
    const near = hist.map(t => ({ t, d: Math.sqrt(t.feat.reduce((a, x, i) => a + (x - f[i]) ** 2, 0)) })).sort((a, b) => a.d - b.d).slice(0, k);
    let w = 0, v = 0;
    for (const x of near) { const wt = 1 / (0.05 + x.d); w += wt; v += wt * (x.t.roi != null ? x.t.roi : 0); }
    return { value: v / w, n: near.length };
  }
  // replay recorded price paths with another take/stop; fees: maker in, maker take out, taker stop out
  const FEE_IN = 0.02, FEE_TP = 0.02, FEE_SL = 0.055 + 0.02;
  const LOCK_MIN = 0.06;                                          // the trailing stop never locks less than the fees
  function trailGap(tpPct) { return +(P.trailPct > 0 ? P.trailPct : Math.max(0.04, tpPct * 0.5)).toFixed(3); }   // the stop is a market order: taker fee + ~0.02% slippage
  function simulate(trades, tp, sl, adaptive) {
    let sum = 0, win = 0, gp = 0, gl = 0;
    for (const t of trades) {
      const k = adaptive ? (t.f || 1) : 1, T = Math.max(0.1, tp * k), S = sl * k;
      let r = null;
      if (P.trail) {
        // trailing: after the take level is reached, leave on a pull-back of the gap from the best point (limit, maker)
        const g = trailGap(T); let peak = -Infinity, on = false;
        for (const x of t.path) {
          if (!on && x <= -S) { r = -S - FEE_IN - FEE_SL; break; }
          if (x >= T) on = true;
          if (on) { peak = Math.max(peak, x); if (x <= peak - g) { r = Math.max(peak - g, LOCK_MIN) - FEE_IN - FEE_TP; break; } }
        }
      } else for (const x of t.path) { if (x >= T) { r = T - FEE_IN - FEE_TP; break; } if (x <= -S) { r = -S - FEE_IN - FEE_SL; break; } }
      if (r == null) r = t.path[t.path.length - 1] - FEE_IN - FEE_TP;   // time exit with the limit near the market
      sum += r; if (r > 0) { win++; gp += r; } else gl -= r;
    }
    const n = trades.length;
    return { n, avg: n ? sum / n : 0, win: n ? win / n * 100 : 0, pf: gl > 0 ? gp / gl : (gp > 0 ? 9.99 : 0) };
  }
  ST.opt = lsGet("ssOpt", null);
  function optimizeExits(force) {
    const inv = inverted();
    const trades = ST.trades.filter(t => t.path && t.path.length >= 3 && !!t.inv === inv).slice(-300);
    if (trades.length < 30 && !force) return null;
    if (!trades.length) return null;
    // in the mirror mode the take is P.slPct and the stop is P.tpPct
    const curTp = inv ? P.slPct : P.tpPct, curSl = inv ? P.tpPct : P.slPct;
    const cur = simulate(trades, curTp, curSl, P.adaptive);
    let best = null;
    for (const adaptive of [true, false]) {
      for (let tp = 0.08; tp <= 1.2001; tp += 0.02) {
        for (let sl = 0.1; sl <= 1.5001; sl += 0.05) {
          const r = simulate(trades, +tp.toFixed(2), +sl.toFixed(2), adaptive);
          if (r.pf < 1) continue;
          // goal: the most profit per trade; between close results the higher win rate and profit factor win
          const goal = r.avg + r.win * 0.0005 + Math.min(r.pf, 3) * 0.002;
          if (!best || goal > best.goal) best = Object.assign(r, { tp: +tp.toFixed(2), sl: +sl.toFixed(2), adaptive, goal });
        }
      }
    }
    const res = { at: Date.now(), n: trades.length, inv, cur: { tp: curTp, sl: curSl, adaptive: P.adaptive, avg: +cur.avg.toFixed(4), win: Math.round(cur.win), pf: +cur.pf.toFixed(2) },
      best: best ? { tp: best.tp, sl: best.sl, adaptive: best.adaptive, avg: +best.avg.toFixed(4), win: Math.round(best.win), pf: +best.pf.toFixed(2) } : null, applied: false };
    // apply only a clear improvement
    if (best && best.avg > cur.avg + 0.005 && trades.length >= 30) {
      if (inv) { P.slPct = best.tp; P.tpPct = best.sl; } else { P.tpPct = best.tp; P.slPct = best.sl; }
      P.adaptive = best.adaptive; save(); res.applied = true;
      engineLog(`⚡⚡ Оптимізатор (${trades.length} угод): тейк ${curTp}→${best.tp}%, стоп ${curSl}→${best.sl}%${best.adaptive !== res.cur.adaptive ? `, адаптивно ${best.adaptive ? "так" : "ні"}` : ""} · `
        + `на угоду ${res.cur.avg}%→${res.best.avg}% · win ${res.cur.win}%→${res.best.win}% · PF ${res.cur.pf}→${res.best.pf}`, "up");
    }
    ST.opt = res; lsSet("ssOpt", res);
    return res;
  }

  // ---------------- inversion ----------------
  // mode now: normal or inverted. In "auto" the bot judges the mode it trades by its last invertMin trades:
  // losing (PnL < 0 and win rate below the break-even of its take/stop) -> switch to the other mode;
  // if the other mode was already judged losing too -> super scalping pauses for 6 h (both directions lose = the costs eat it).
  ST.inv = !!lsGet("ssInv", false);
  ST.judged = lsGet("ssJudged", {});            // { normal: {pnl, win, at}, inverted: {...} }
  const inverted = () => P.invert === "on" ? true : P.invert === "off" ? false : ST.inv;
  function judgeMode() {
    if (P.invert !== "auto") return;
    const mode = ST.inv ? "inverted" : "normal";
    const since = (ST.judged[mode] && ST.judged[mode].resetAt) || 0;
    const ts = ST.trades.filter(t => !!t.inv === ST.inv && t.closedAt > since).slice(-P.invertMin * 2);
    if (ts.length < P.invertMin) return;
    const last = ts.slice(-P.invertMin);
    const pnl = last.reduce((a, t) => a + t.pnl, 0), win = last.filter(t => t.pnl > 0).length / last.length;
    if (pnl >= 0) return;
    ST.judged[mode] = { pnl: +pnl.toFixed(4), win: Math.round(win * 100), at: Date.now(), resetAt: Date.now() };
    const other = ST.inv ? "normal" : "inverted";
    const otherBad = ST.judged[other] && ST.judged[other].pnl < 0 && Date.now() - ST.judged[other].at < 7 * 86400e3;
    if (otherBad) {
      // both lost: no pause (trading never stops) - the series fixer changes filters/exits/coins; the modes are judged afresh
      ST.judged = {}; lsSet("ssJudged", ST.judged);
      engineLog(`⚡⚡ І звичайний режим, і інверсія були в мінусі — не зупиняюсь: міняю фільтри/виходи/монети, режими оцінюю заново`, "gold-t");
      streakFixNow("обидва режими в мінусі");
      return;
    }
    ST.inv = !ST.inv; lsSet("ssInv", ST.inv); lsSet("ssJudged", ST.judged);
    engineLog(`⚡⚡ ${mode === "normal" ? "Звичайний режим" : "Інверсія"} в мінусі (${pnl.toFixed(3)} USDT, ${Math.round(win * 100)}% у плюсі за ${P.invertMin} угод) — перемикаю на ${ST.inv ? "ІНВЕРСІЮ: торгую навпаки, тейк і стоп поміняні" : "звичайний режим"}`, "gold-t");
  }
  ST.pausedUntil = 0;

  // ---------------- series fixer: 3 losing trades in a row -> change something; learn which change gives a plus series ----------------
  // every change is scored by the next 5 trades (sum of PnL, wins); untried changes go first, then the ones that worked best
  const FIX = Object.assign({ active: null, stats: {}, log: [] }, lsGet("ssFix", {}));
  const saveFix = () => lsSet("ssFix", { active: FIX.active, stats: FIX.stats, log: FIX.log.slice(-40) });
  const baseScore = () => (P.style === "pressure" ? PRESSURE.scoreIn : DEFAULTS.scoreIn);
  const FIXES = {
    filter: { name: "суворіший вхід", run: () => { if (P.scoreIn >= 0.85) return null; const o = P.scoreIn; P.scoreIn = +Math.min(0.85, P.scoreIn + 0.05).toFixed(2); return `поріг сигналу ${o}→${P.scoreIn}`; } },
    mirror: { name: "навпаки", run: () => { if (P.invert === "auto") { ST.inv = !ST.inv; lsSet("ssInv", ST.inv); } else P.invert = P.invert === "on" ? "off" : "on"; return inverted() ? "торгую навпаки (Buy↔Sell)" : "торгую у звичайний бік"; } },
    style: { name: "інший стиль", run: () => { const to = P.style === "pressure" ? "flow" : "pressure"; setStyle(to); P.trail = true; return to === "pressure" ? "стиль → тиск стакану" : "стиль → потік ордерів"; } },
    exits: { name: "коротші виходи", run: () => { if (P.slPct <= 0.12 && P.tpPct <= 0.06) return null; const a = P.tpPct, b = P.slPct; P.tpPct = +Math.max(0.06, P.tpPct * 0.85).toFixed(3); P.slPct = +Math.max(0.12, P.slPct * 0.8).toFixed(3); return `тейк ${a}→${P.tpPct}%, стоп ${b}→${P.slPct}%`; } },
    wider: { name: "ширший стоп", run: () => { if (P.slPct >= 0.6) return null; const b = P.slPct; P.slPct = +Math.min(0.6, P.slPct * 1.3).toFixed(3); return `стоп ${b}→${P.slPct}% (менше вибиває шумом)`; } },
    coins: { name: "інші монети", run: (losers) => { for (const s2 of losers) COOL[s2] = Date.now() + 1800e3; if (P.autoCoins) pickCoins(true); return `пауза 30 хв для ${losers.join(", ")}` + (P.autoCoins ? ", беру інші монети" : ""); } },
    agree: { name: "стакан і стрічка разом", run: () => { P.needAgree = !P.needAgree; return P.needAgree ? "вхід лише коли стакан і стрічка згодні" : "вхід і без згоди стрічки"; } },
  };
  function chooseFix(exclude) {
    const keys = Object.keys(FIXES).filter(k => k !== exclude);
    const untried = keys.filter(k => !FIX.stats[k] || !FIX.stats[k].n);
    if (untried.length) return untried;
    // best average PnL per try first; a change that keeps failing goes to the end
    return keys.sort((a, b) => (FIX.stats[b].pnl / FIX.stats[b].n) - (FIX.stats[a].pnl / FIX.stats[a].n));
  }
  function streakFixNow(why) {
    if (!P.streakFix) return;
    const last = ST.trades.slice(-P.streakN);
    const losers = [...new Set(last.filter(t => t.pnl <= 0).map(t => t.sym))];
    if (FIX.active) scoreFix(true);                                  // the previous change did not help
    for (const k of chooseFix(FIX.last)) {
      const txt = FIXES[k].run(losers);
      if (!txt) continue;
      save();
      FIX.active = { key: k, at: Date.now(), from: ST.trades.length, txt };
      FIX.last = k;
      FIX.log.push({ at: Date.now(), key: k, txt, why });
      saveFix();
      engineLog(`🔁 ${why} — міняю (${FIXES[k].name}): ${txt}. Оцінка за наступні 5 угод.`, "gold-t");
      return;
    }
  }
  function scoreFix(failed) {
    const a = FIX.active; if (!a) return;
    const after = ST.trades.slice(a.from);
    const st = FIX.stats[a.key] = FIX.stats[a.key] || { n: 0, pnl: 0, wins: 0, trades: 0, good: 0 };
    st.n++; st.trades += after.length; st.pnl = +(st.pnl + after.reduce((x, t) => x + t.pnl, 0)).toFixed(4); st.wins += after.filter(t => t.pnl > 0).length;
    const good = !failed && after.reduce((x, t) => x + t.pnl, 0) > 0; if (good) st.good++;
    const L = FIX.log[FIX.log.length - 1]; if (L && L.at === a.at) { L.result = +after.reduce((x, t) => x + t.pnl, 0).toFixed(4); L.wins = after.filter(t => t.pnl > 0).length; L.n = after.length; }
    FIX.active = null; saveFix();
    if (good) engineLog(`🔁 Зміна «${FIXES[a.key].name}» спрацювала: ${after.filter(t => t.pnl > 0).length}/${after.length} у плюсі — залишаю`, "up");
  }
  // called after every closed trade
  function streakCheck() {
    if (!P.streakFix) return;
    const t = ST.trades, n = P.streakN;
    if (FIX.active && t.length - FIX.active.from >= 5) scoreFix(false);
    const tail = t.slice(-n);
    const sinceFix = FIX.active ? t.length - FIX.active.from : Infinity;
    if (tail.length === n && tail.every(x => x.pnl <= 0) && sinceFix >= n) streakFixNow(`${n} угоди поспіль у мінусі`);
  }
  // a stricter filter must not starve the bot: no entry for 15 min -> the threshold slowly goes back
  setInterval(() => {
    if (!P.on || P.scoreIn <= baseScore()) return;
    if (Date.now() - (ST.lastEntryAt || Date.now()) < 15 * 60000 || Object.keys(POSN).length) return;
    const o = P.scoreIn; P.scoreIn = +Math.max(baseScore(), P.scoreIn - 0.03).toFixed(2); ST.lastEntryAt = Date.now(); save();
    engineLog(`🔁 15 хв без входів — поріг сигналу ${o}→${P.scoreIn}, щоб торгівля не стояла`, "mute");
  }, 60000);
  function series() {
    const t = ST.trades; let wins = 0, losses = 0, best = 0, cur = 0;
    for (let i = t.length - 1; i >= 0; i--) { if (t[i].pnl > 0) { if (losses) break; wins++; } else { if (wins) break; losses++; } }
    for (const x of t) { cur = x.pnl > 0 ? cur + 1 : 0; best = Math.max(best, cur); }
    return { wins, losses, bestWins: best };
  }

  // ---------------- pick the most volatile coins ----------------
  // volatility = range of the last hour on 5-minute candles; only coins with enough turnover and a tight spread
  ST.vol = [];
  async function pickCoins(force) {
    if (!P.autoCoins || (!force && Date.now() - (ST.pickedAt || 0) < P.pickEveryMin * 60000)) return;
    ST.pickedAt = Date.now();
    try {
      const all = ((await bx("/v5/market/tickers", { query: { category: "linear" } })).list || [])
        .filter(t => /USDT$/.test(t.symbol) && Number(t.turnover24h) >= P.minTurnoverM * 1e6 && Number(t.lastPrice) > 0)
        .map(t => { const last = Number(t.lastPrice), bid = Number(t.bid1Price) || last, ask = Number(t.ask1Price) || last;
          return { sym: t.symbol, last, spreadPct: (ask - bid) / last * 100, range24: (Number(t.highPrice24h) - Number(t.lowPrice24h)) / last * 100, turnoverM: Math.round(Number(t.turnover24h) / 1e6) }; })
        .filter(t => t.spreadPct <= 0.05)
        .sort((a, b) => b.range24 - a.range24).slice(0, 30);   // the 30 most volatile today, then check the last hour
      const rows = [];
      for (const t of all) {
        try {
          const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: t.sym, interval: "5", limit: "12" } });
          const c = (k.list || []).map(r => ({ h: +r[2], l: +r[3], v: +r[6] || 0 }));
          if (c.length < 6) continue;
          const rng1h = (Math.max(...c.map(x => x.h)) - Math.min(...c.map(x => x.l))) / t.last * 100;
          const avg5 = c.reduce((a, x) => a + (x.h - x.l) / t.last * 100, 0) / c.length;
          rows.push(Object.assign(t, { rng1h: +rng1h.toFixed(2), avg5: +avg5.toFixed(3), score: +(rng1h * 0.6 + avg5 * 6 + t.range24 * 0.05).toFixed(2) }));
        } catch (e) { /* skip */ }
      }
      rows.sort((a, b) => b.score - a.score);
      ST.vol = rows.slice(0, 12);
      const now = Date.now();
      const keep = Object.keys(POSN);                                    // never drop a coin we are trading
      const fresh = rows.filter(r => (COOL[r.sym] || 0) < now && !keep.includes(r.sym)).map(r => r.sym);
      const list = [...new Set(keep.concat(fresh))].slice(0, Math.max(P.coinCount, keep.length));
      if (list.length && list.join() !== P.symbols.join()) {
        const before = P.symbols.slice();
        P.symbols = list; save(); resubscribe();
        engineLog(`⚡⚡ Топ волатильності: ${list.map(s => { const r = rows.find(x => x.sym === s); return r ? `${s} ${r.rng1h}%/год` : s; }).join(", ")}`
          + (before.length ? ` (було ${before.join(", ")})` : ""), "gold-t");
      }
    } catch (e) { ST.err = "вибір монет: " + (e && e.message || e); log("pickCoins:", e && (e.stack || e.message || e)); }
  }
  setInterval(() => { if (P.on) { pickCoins(ST_PICK_NOW); ST_PICK_NOW = false; } }, 30000);

  // drop what loses: a coin with a losing recent run pauses for an hour; a signal type that keeps losing is switched off
  function prune(sym) {
    const last = ST.trades.filter(x => x.sym === sym).slice(-8);
    const free = P.symbols.filter(x => x !== sym && (COOL[x] || 0) < Date.now()).length;
    if (free >= 2 && last.length >= 6 && last.reduce((a, x) => a + x.pnl, 0) < 0 && last.filter(x => x.pnl < 0).length >= 5) {
      COOL[sym] = Date.now() + 3600e3;
      engineLog(`⚡⚡ ${sym}: ${last.filter(x => x.pnl < 0).length} збиткових з ${last.length} — монета на паузі 1 год`, "down");
    }
    for (const [flag, key, name] of [["ice", "useIceberg", "айсберги"], ["abs", "useAbsorption", "поглинання"], ["spoof", "useSpoof", "спуфінг"]]) {
      const ts = ST.trades.slice(-200).filter(x => x.sig && x.sig[flag]).slice(-20);
      if (P[key] && ts.length >= 12 && ts.filter(x => x.pnl > 0).length / ts.length < 0.4 && ts.reduce((a, x) => a + x.pnl, 0) < 0) {
        P[key] = false; save();
        engineLog(`⚡⚡ Сигнал «${name}» збитковий (${ts.filter(x => x.pnl > 0).length}/${ts.length} у плюсі) — вимкнено`, "down");
      }
    }
  }

  // ---------------- the loop: entries every 0.5 s, our orders every 1.5 s ----------------
  let busy = false, lastPoll = 0;
  async function tick() {
    if (busy) return;
    busy = true;
    try {
      const now = Date.now();
      const day = new Date().toISOString().slice(0, 10);
      if (ST.day !== day) { ST.day = day; ST.dayPnl = 0; ST.stoppedForDay = false; }
      if (P.on && !ST.stoppedForDay) {
        const eq = Number(H.equity()) || 0;
        if (false && eq > 0 && ST.dayPnl <= -eq * P.dailyLossPct / 100) {   // no daily stop: the owner wants trading never to stop
          ST.stoppedForDay = true;
          engineLog(`⚡⚡ Суперскальпінг: денний збиток ${ST.dayPnl.toFixed(2)} USDT ≥ ${P.dailyLossPct}% капіталу — нові входи до завтра вимкнено`, "down");
        }
      }
      // price path of every open trade, every 0.5 s, in % from the entry in the trade's favour (for the exit optimiser)
      for (const p of Object.values(POSN)) {
        if (p.state !== "open" || !M[p.sym] || !M[p.sym].last) continue;
        const mv = (M[p.sym].last - p.entry) / p.entry * 100 * (p.side === "Buy" ? 1 : -1);
        (p.path = p.path || []).push(Math.round(mv * 1000) / 1000);
        if (p.path.length > 900) p.path = p.path.filter((_, i) => i % 2 === 0);   // keep it short for long trades
        if (p.trail && p.tpId && !p.trailExit) await trailStep(p, mv);
      }
      // manage our entries / positions
      if (Object.keys(POSN).length && now - lastPoll >= 1500) {
        lastPoll = now;
        const [ordR, posR] = await Promise.all([
          bx("/v5/order/realtime", { query: { category: "linear", settleCoin: "USDT", limit: "50" }, auth: true }),
          bx("/v5/position/list", { query: { category: "linear", settleCoin: "USDT", limit: "200" }, auth: true }),
        ]);
        const live = new Map((ordR.list || []).map(o => [o.orderLinkId, o]));
        const pos = (posR.list || []).filter(x => Number(x.size) > 0);
        for (const p of Object.values(POSN)) {
          const ps = pos.find(x => x.symbol === p.sym && x.side === p.side);   // in hedge mode both sides can exist
          const sig = signals(p.sym);
          if (p.state === "entry") {
            if (ps) { await onFilled(p, Number(ps.avgPrice)); continue; }
            if (!live.has(p.entryId)) { delete POSN[p.sym]; continue; }     // cancelled / rejected (PostOnly crossed)
            const mine = sig ? (p.side === "Buy" ? sig.long : sig.short) : 1;
            if (P.cancelOnFade && !p.inv && sig && mine < P.scoreIn * 0.7) {
              await cancel(p.sym, p.entryId);
              // the order may have filled at this very moment: then the position is picked up on the next check
              const again = await bx("/v5/position/list", { query: { category: "linear", symbol: p.sym }, auth: true });
              const filled = (again.list || []).find(x => x.side === p.side && Number(x.size) > 0);
              if (filled) { await onFilled(p, Number(filled.avgPrice)); continue; }
              delete POSN[p.sym]; COOL[p.sym] = now + 10000;
              engineLog(`⚡⚡ ${p.sym}: сигнал згас (${mine.toFixed(2)}) до виконання — вхід знято`, "mute");
              continue;
            }
            if (now - p.at > P.entryWaitSec * 1000) { await cancel(p.sym, p.entryId); delete POSN[p.sym]; engineLog(`⚡⚡ ${p.sym}: вхід не виконався за ${P.entryWaitSec} с — знято`, "mute"); }
            continue;
          }
          if (!ps) { await record(p, p.trailOn ? "трейлінг" : live.has(p.tpId) ? "стоп" : "тейк"); if (p.tpId) await cancel(p.sym, p.tpId); continue; }
          const long = p.side === "Buy", px = sig ? sig.last : Number(ps.markPrice);
          const gone = (long ? px - p.entry : p.entry - px) / p.entry * 100;
          // ride the impulse: the take moves further and the stop goes to a small profit while the tape still pushes our way
          const push = sig && (long ? sig.delta > 0.3 && sig.burst >= P.burstX * 0.7 : sig.delta < -0.3 && sig.burst >= P.burstX * 0.7);
          if (P.ride && !p.trail && !p.inv && p.tpId && !p.leaving && gone >= (p.tpPct || P.tpPct) * 0.7 && push) {
            const ntp = await alignP(p.tp * (1 + (long ? 1 : -1) * (p.tpPct || P.tpPct) / 200), p.tick, long);
            const be = await alignP(p.entry * (1 + (long ? 1 : -1) * 0.06 / 100), p.tick, long);
            try {
              await amend(p.sym, p.tpId, ntp); p.tp = Number(ntp); p.rides = (p.rides || 0) + 1;
              if (!p.beDone) { await setStop(p, be); p.beDone = true; p.sl = Number(be); }
              engineLog(`⚡⚡ ${p.sym}: імпульс триває — тейк → ${ntp}` + (p.beDone ? ", стоп у плюс" : ""), "up");
            } catch (e) { /* the take may have just filled */ }
          }
          // the flow turned against us: leave now with a limit at the best price on our side (maker), smaller loss than the stop
          const against = sig ? (long ? sig.short : sig.long) : 0;
          if (P.flipExit && !p.trailOn && !p.inv && !p.leaving && sig && against >= P.scoreIn && (long ? sig.delta < -0.2 : sig.delta > 0.2)) {
            const best = long ? sig.ask : sig.bid;
            try { await amend(p.sym, p.tpId, best); p.tp = best; p.leaving = "розворот"; engineLog(`⚡⚡ ${p.sym}: потік розвернувся проти (${against.toFixed(2)}) — виходжу лімітом ${best}`, "mute"); } catch (e) { /* filled */ }
          }
          // pressure style: the book stopped leaning our way -> leave at once with a limit at the best price
          if (P.style === "pressure" && !p.trailOn && !p.inv && !p.leaving && sig && sig.press && (long ? sig.press.lvl : 1 - sig.press.lvl) < 0.47) {
            const best = long ? sig.ask : sig.bid;
            try { await amend(p.sym, p.tpId, best); p.tp = best; p.leaving = "тиск зник"; engineLog(`📊 ${p.sym}: тиск стакану зник (${sig.press.lvl}) — виходжу лімітом ${best}`, "mute"); } catch (e) { /* filled */ }
          }
          // no progress for timeStopSec: leave near break-even (entry + fees) with the limit take
          const tpPctNow = p.tpPct || P.tpPct;
          if (!p.leaving && !p.trailOn && P.timeStopSec > 0 && now - p.openedAt > P.timeStopSec * 1000 && gone < tpPctNow * 0.3) {
            const beP = await alignP(p.entry * (1 + (long ? 1 : -1) * 0.05 / 100), p.tick, long);
            const best = sig ? (long ? sig.ask : sig.bid) : Number(beP);
            const target = long ? Math.max(Number(beP), best) : Math.min(Number(beP), best);
            try { await amend(p.sym, p.tpId, target); p.tp = target; p.leaving = "час"; engineLog(`⚡⚡ ${p.sym}: ${P.timeStopSec} с без руху — виходжу біля беззбитку ${target}`, "mute"); } catch (e) { /* filled */ }
          }
          // too long in the trade: pull the take to the best price (still a limit, maker) to get out
          if (now - p.openedAt > P.holdMaxSec * 1000 && sig && !p.pulled && !p.trailOn) {
            const best = long ? sig.ask : sig.bid;
            try { await amend(p.sym, p.tpId, best); p.tp = best; p.pulled = true; engineLog(`⚡⚡ ${p.sym}: тримаю довше ${P.holdMaxSec} с — тейк підтягнуто до ринку ${best} (лімітом)`, "mute"); } catch (e) { /* ignore */ }
          }
        }
      }
      // reserve our coins: the breakout bot does not use them while super scalping is on
      W_SET(P.on ? P.symbols : []);
      if (P.on && now - (ST.freedAt || 0) > 30000) { ST.freedAt = now; await freeCoins(); }
      // why there is no entry, per coin (shown on the page and given to the AI)
      for (const sym of P.symbols) {
        const s = signals(sym), m = M[sym] || {};
        const sc = s ? Math.max(s.long, s.short) : 0;
        m.peak = (m.peak || []).filter(x => now - x[0] < 600000); if (s) m.peak.push([now, sc]);
        m.why = !P.on ? "вимкнено" : ST.stoppedForDay ? "денний ліміт збитку" : !s ? "збираю стрічку (20 с)" : POSN[sym] ? "в угоді"
          : (COOL[sym] || 0) > now ? `пауза ${Math.round((COOL[sym] - now) / 1000)} с` : s.spreadTicks > 3 ? `широкий спред ${s.spreadTicks} тік.`
          : (ST.noMoneyUntil || 0) > now ? "мало вільних коштів" : sc < P.scoreIn ? `сигнал ${sc.toFixed(2)} < ${P.scoreIn}` : Object.keys(POSN).length >= P.maxPositions ? "ліміт угод" : "входжу";
      }
      // new entries
      if (P.on && !ST.stoppedForDay && (ST.noMoneyUntil || 0) < now && Object.keys(POSN).length < P.maxPositions) {
        const busySyms = new Set(H.busy() || []);
        for (const sym of P.symbols) if (busySyms.has(sym) && M[sym] && !POSN[sym]) M[sym].why = "позиція основного бота";
        const cands = P.symbols.map(signals).filter(Boolean)
          .filter(s => !POSN[s.sym] && !busySyms.has(s.sym) && (COOL[s.sym] || 0) < now && s.spreadTicks <= 3)
          .map(s => ({ s, side: s.long >= s.short ? "Buy" : "Sell", score: Math.max(s.long, s.short) }))
          .filter(c => c.score >= P.scoreIn)
          .filter(c => !P.needAgree || (c.side === "Buy" ? c.s.imb >= 0.55 && c.s.delta > 0.1 : c.s.imb <= 0.45 && c.s.delta < -0.1))
          .sort((a, b) => b.score - a.score);
        for (const c of cands) {
          // learned filter: what did the most similar past setups bring? (the signal side, before any inversion)
          const ex = expect(c.s, c.side, inverted());
          if (ex && ex.value <= 0) {
            ST.skipped = (ST.skipped || 0) + 1;
            if (M[c.s.sym]) M[c.s.sym].why = `схожі угоди в мінусі (${ex.value.toFixed(3)}%, ${ex.n})`;
            COOL[c.s.sym] = now + 5000;
            continue;
          }
          if (inverted()) { c.side = c.side === "Buy" ? "Sell" : "Buy"; }   // mirror: the opposite side
          if (Object.keys(POSN).length >= P.maxPositions) break;
          try { await enter(c.s.sym, c.side, c.s); }
          catch (e) {
            if (/110007|ab not enough|insufficient|not enough/i.test(e.message)) {
              ST.noMoneyUntil = now + 60000; ST.noMoney = "Bybit: недостатньо вільних коштів (маржу зайняли інші ордери/позиції)";
              break;
            }
            ST.err = `${c.s.sym}: ${e.message}`; COOL[c.s.sym] = now + 30000;
          }
          if ((ST.noMoneyUntil || 0) > Date.now()) break;
        }
      }
    } catch (e) { ST.err = String(e && e.message || e); }
    finally { busy = false; }
  }
  setInterval(tick, 500);

  // ---------------- websocket ----------------
  function connect() {
    clearTimeout(reconnectT); clearInterval(pingT);
    const url = testWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/public/linear" : "wss://stream.bybit.com/v5/public/linear");
    try { ws = new WebSocket(url); } catch (e) { ST.err = "WebSocket: " + e.message; reconnectT = setTimeout(connect, 5000); return; }
    ws.on("open", () => {
      ST.wsUp = true; ST.err = "";
      const args = [];
      for (const s of P.symbols) args.push(`orderbook.50.${s}`, `publicTrade.${s}`);
      for (let i = 0; i < args.length; i += 10) ws.send(JSON.stringify({ op: "subscribe", args: args.slice(i, i + 10) }));
      pingT = setInterval(() => { try { ws.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000);
      log(`superscalp ws up: ${P.symbols.join(",")}`);
    });
    ws.on("message", (raw) => {
      ST.lastMsg = Date.now();
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (!m.topic) return;
      const [kind, a, b] = m.topic.split(".");
      if (kind === "orderbook") applyBook(b, m.data || {}, m.type === "snapshot");
      else if (kind === "publicTrade") applyTrades(a, m.data || []);
    });
    ws.on("close", () => { ST.wsUp = false; clearInterval(pingT); reconnectT = setTimeout(connect, 3000); });
    ws.on("error", (e) => { ST.err = "WebSocket: " + (e && e.message || e); });
  }
  function resubscribe() { try { ws && ws.close(); } catch (e) { /* reconnects */ } for (const k of Object.keys(M)) if (!P.symbols.includes(k)) delete M[k]; }
  connect();

  // ---------------- control + views ----------------
  const NUM = { margin: [1, 100000], leverage: [1, 50], tpPct: [0.06, 2], slPct: [0.1, 3], scoreIn: [0.3, 0.95], bandPct: [0.05, 1], burstX: [1.2, 10],
    wallX: [2, 30], spoofSec: [1, 20], icebergX: [1.5, 20], entryWaitSec: [2, 60], holdMaxSec: [30, 3600], maxPositions: [1, 10], cooldownSec: [0, 3600], dailyLossPct: [0, 20], timeStopSec: [0, 3600], trailPct: [0, 2], streakN: [2, 10], pressLevels: [1, 25], coinCount: [1, 12], minTurnoverM: [5, 5000], pickEveryMin: [2, 120], invertMin: [10, 200], learnMin: [20, 400], optEvery: [5, 100] };
  function setParams(o, caps = {}) {
    const changed = [];
    // coins named explicitly win over the automatic volatility pick
    if (o && Array.isArray(o.symbols) && o.symbols.length && o.autoCoins === undefined && P.autoCoins) { o = Object.assign({}, o, { autoCoins: false }); }
    for (const [k, v] of Object.entries(o || {})) {
      if (k === "useSpoof" || k === "useAbsorption" || k === "dailyLossPct") continue;   // locked by the owner: spoof/absorption off, no daily stop
      if (NUM[k]) {
        let n = Number(v); if (!Number.isFinite(n)) continue;
        let hi = NUM[k][1];
        if (k === "margin" && caps.margin) hi = Math.min(hi, caps.margin);
        if (k === "leverage" && caps.leverage) hi = Math.min(hi, caps.leverage);
        n = Math.max(NUM[k][0], Math.min(hi, n));
        if (["maxPositions", "leverage", "entryWaitSec", "holdMaxSec", "cooldownSec", "spoofSec", "coinCount", "pickEveryMin"].includes(k)) n = Math.round(n);
        if (P[k] !== n) { changed.push(`${k} ${P[k]}→${n}`); P[k] = n; }
      } else if (["ride", "trail", "streakFix", "useIceberg", "on", "needAgree", "cancelOnFade", "flipExit", "adaptive", "autoPrune", "autoCoins", "learn", "optExits"].includes(k) && typeof v === "boolean") {
        if (P[k] !== v) { changed.push(`${k} ${P[k]}→${v}`); P[k] = v; }
      } else if (k === "style" && ["flow", "pressure"].includes(v)) {
        changed.push(...setStyle(v));
      } else if (k === "invert" && ["off", "on", "auto"].includes(v)) {
        if (P[k] !== v) { changed.push(`invert ${P[k]}→${v}`); P[k] = v; }
      } else if (k === "symbols" && Array.isArray(v)) {
        const list = [...new Set(v.map(x => String(x).toUpperCase()).filter(x => /^[A-Z0-9]{2,30}USDT$/.test(x)))].slice(0, 12);
        if (list.length && list.join() !== P.symbols.join()) { changed.push(`монети ${list.join(",")}`); P.symbols = list; resubscribe(); }
      }
    }
    save();
    return changed;
  }
  // switch the style; the flow settings are kept and come back when the pressure style is switched off
  function setStyle(style) {
    if (style === P.style) return [];
    if (style === "pressure") {
      lsSet("ssFlowPrev", Object.fromEntries(Object.keys(PRESSURE).map(k => [k, P[k]])));
      Object.assign(P, PRESSURE);
    } else {
      Object.assign(P, lsGet("ssFlowPrev", {}), { style: "flow" });
    }
    save();
    engineLog(style === "pressure" ? `📊 Скальпінг тиском стакану: тейк ${P.tpPct}% лімітом, стоп ${P.slPct}%, вихід одразу, коли тиск зник, до ${P.maxPositions} угод` : "⚡⚡ Повернено звичайний суперскальпінг (стакан + стрічка)", "gold-t");
    return [`стиль → ${style === "pressure" ? "тиск стакану" : "потік ордерів"}`];
  }
  function setOn(on) { P.on = !!on; save(); if (on) pickCoins(true); engineLog(on ? `⚡⚡ Суперскальпінг увімкнено: ${P.symbols.join(", ")} · тейк ${P.tpPct}% лімітом · стоп ${P.slPct}%` : "⚡⚡ Суперскальпінг вимкнено (відкриті угоди доведе до кінця)", on ? "up" : "mute"); }
  function stats() {
    const t = ST.trades, w = t.filter(x => x.pnl > 0);
    const day = t.filter(x => x.closedAt >= Date.now() - 86400e3);
    return { n: t.length, win: t.length ? Math.round(w.length / t.length * 100) : null, pnl: +t.reduce((a, x) => a + x.pnl, 0).toFixed(4),
      day: day.length, dayPnl: +day.reduce((a, x) => a + x.pnl, 0).toFixed(4), avgHold: t.length ? Math.round(t.reduce((a, x) => a + x.holdSec, 0) / t.length) : null };
  }
  function view() {
    const per = (inv) => { const a = ST.trades.filter(t => !!t.inv === inv); return { n: a.length, pnl: +a.reduce((s, t) => s + t.pnl, 0).toFixed(4), win: a.length ? Math.round(a.filter(t => t.pnl > 0).length / a.length * 100) : null }; };
    const withPath = ST.trades.filter(t => t.path && t.path.length >= 3).length, withFeat = ST.trades.filter(t => t.feat).length;
    const pfAll = (() => { const a = ST.trades.slice(-200), gp = a.filter(t => t.pnl > 0).reduce((s, t) => s + t.pnl, 0), gl = -a.filter(t => t.pnl < 0).reduce((s, t) => s + t.pnl, 0); return gl > 0 ? +(gp / gl).toFixed(2) : null; })();
    const fixView = { on: P.streakFix, n: P.streakN, series: series(), active: FIX.active, stats: FIX.stats, log: FIX.log.slice(-8).reverse(), names: Object.fromEntries(Object.entries(FIXES).map(([k, v]) => [k, v.name])) };
    return { fix: fixView, learn: { on: P.learn, ready: withFeat >= P.learnMin, have: withFeat, need: P.learnMin, skipped: ST.skipped || 0, pf: pfAll, opt: ST.opt, optOn: P.optExits, paths: withPath },
      mode: { inverted: inverted(), setting: P.invert, normal: per(false), inv: per(true), judged: ST.judged, pausedUntil: ST.pausedUntil || 0 },
      noMoney: (ST.noMoneyUntil || 0) > Date.now() ? ST.noMoney : "", vol: ST.vol, pickedAt: ST.pickedAt || 0, params: P, ws: ST.wsUp && Date.now() - ST.lastMsg < 15000, err: ST.err, stoppedForDay: ST.stoppedForDay, stats: stats(),
      live: P.symbols.map(sym => Object.assign({ sym }, signals(sym) || {}, { why: (M[sym] || {}).why || "підключаюсь", peak10: +Math.max(0, ...((M[sym] || {}).peak || []).map(x => x[1])).toFixed(2) })), open: Object.values(POSN).map(p => ({ sym: p.sym, side: p.side, state: p.state, entry: p.entry || p.price, tp: p.tp || null, sl: p.sl || null, rides: p.rides || 0, trail: !!p.trailOn, peak: p.peak != null ? +p.peak.toFixed(3) : null, lock: p.lockPct != null ? +p.lockPct.toFixed(3) : null, sec: Math.round((Date.now() - (p.openedAt || p.at)) / 1000) })),
      trades: ST.trades.slice(-15).reverse() };
  }
  function aiView() {
    // what the AI needs: parameters, results, live signals and the last trades with the signals they were taken on
    const by = (f) => { const g = {}; for (const t of ST.trades.slice(-300)) { const k = f(t); if (k == null) continue; (g[k] = g[k] || []).push(t); }
      return Object.entries(g).map(([k, a]) => ({ value: k, n: a.length, win: Math.round(a.filter(x => x.pnl > 0).length / a.length * 100), pnl: +a.reduce((s, x) => s + x.pnl, 0).toFixed(4) })); };
    return { seriesFixer: { on: P.streakFix, afterLosses: P.streakN, series: series(), active: FIX.active, resultsByChange: FIX.stats, recent: FIX.log.slice(-6) }, optimizer: ST.opt, learnedFilter: { on: P.learn, skipped: ST.skipped || 0 }, inverted: inverted(), byMode: by(t => t.inv ? "inverted" : "normal"), volatilityTop: ST.vol, params: P, stats: stats(), live: P.symbols.map(sym => Object.assign({ sym }, signals(sym) || {}, { why: (M[sym] || {}).why || "", peakScore10min: +Math.max(0, ...((M[sym] || {}).peak || []).map(x => x[1])).toFixed(2) })),
      bySymbol: by(t => t.sym), bySignal: [].concat(by(t => t.sig && t.sig.ice ? "iceberg" : null), by(t => t.sig && t.sig.abs ? "absorption" : null), by(t => t.sig && t.sig.spoof ? "spoof" : null)),
      byTp: by(t => t.cfg && t.cfg.tpPct), byExit: by(t => t.how), last: ST.trades.slice(-20) };
  }
  return { setStyle, optimizeNow: () => optimizeExits(true), view, aiView, setParams, setOn, params: P, signals, _test: { streakCheck, FIX, series, M, POSN, applyBook, applyTrades, judgeMode, ST, inverted, optimizeExits, expect, feat, simulate } };
}

module.exports = { create, DEFAULTS };
