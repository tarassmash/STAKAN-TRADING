// ПРОБІЙНА СІТКА — окремий бот: рівні кожні stepPct % без меж (сітка йде за ціною куди завгодно).
//  • Напрям = пробій/тренд на 5-хв свічках (EMA36 проти EMA96 з запасом band): вгору → лише лонги, вниз → лише шорти, без напряму → нових входів немає.
//  • Кожен перетин рівня (і за рухом, і на відкаті) відкриває «юніт» у бік пробою з мікро-тейком на наступному рівні.
//  • «Бігун»: один юніт без тейка зі стопом, що йде за найкращою ціною (trailSteps рівнів) — забирає весь трендовий рух.
//  • Стоп юніта — stopSteps рівнів назад; розворот напряму закриває все.
//  • Перед стартом: монета і крок вибираються перевіркою на історії (3 дні 5-хв свічок) за тими самими правилами.
"use strict";
const WebSocketNode = (() => { try { return require("ws"); } catch (e) { return null; } })();
const BT = require("./backtest");
const FLOWMOD = require("./flow");

const DEFAULTS = {
  on: false,
  autoCoin: true, symbol: "",
  investment: 20, leverage: 3,
  stepPct: 0.5,            // distance between levels, %
  maxUnits: 6,             // micro-take units open at once (+ the runner)
  stopSteps: 3,            // a unit's stop: this many levels back
  runner: true, trailSteps: 2,
  band: 0.2,               // EMA fast vs slow must differ by this % to set the direction
  feeX: 4,                 // the step is at least feeX × (taker in + maker out)
  autoTune: true, btDays: 3, btNeedPlus: true,
  minTurnoverM: 50,
  idleMin: 60,             // no direction and no units this long -> look for a better coin
  straddlePct: 1,          // "both sides": the triggers are this % above and below the price
  straddleRearm: true,     // after the breakout trades are closed the triggers are set again around the new price
  scalp: false,            // ⚡ super scalping: takes of scalpStep % on every micro move (the step is fixed, the coin is still chosen on history)
  scalpStep: 0.3,
  flip: false,             // 🔁 no stops, no EMA: the price sets the direction; a pullback of flipSteps levels from the best price closes all and turns around
  flipSteps: 0,            // 0 = chosen per coin on history (flipMin .. flipMin+2 levels)
  flipMin: 4,              // never turn on a smaller pullback than this many levels (2 levels = noise turns it back and forth)
  sawFlips: 4,             // this many turns within an hour = a saw -> leave the coin
  autoInv: false,          // the size = 75% of the balance (taken at every new start)
  ddCap: 25,               // flip: the leverage is the highest whose worst drawdown on history stays within this % of the balance
  learnMin: 10,             // flip: every this many minutes re-learn take / turn / leverage on the last learnHours of 1-minute candles
  learnHours: 6,
  brakePct: 8,             // flip: the result since the last change below -brakePct % of the money -> cut the risk at once
  limitEntry: true,        // entries only by fast PostOnly limits at the best price (maker fee), chased to the new best price; never market
  invert: false,           // 🔄 mirror: everything as is, but the account takes the opposite side of every trade
  autoOpt: true,
  useFlow: true,           // flip: the tape + the book + speed + indicators steer entries and turns
  flowGate: 0.35,          // flow against the direction stronger than this -> no new micro entries; at a turn -> close but wait for confirmation
  earlyFlip: 0.6,          // flow against stronger than this and the pullback already half way -> turn early (a smaller loss)           // flip: live self-tuning - losing reversals -> less leverage and a wider turn, a plus series -> leverage back up
};
const NUM = { flowGate: [0.05, 0.95], earlyFlip: [0.2, 1], learnMin: [2, 240], learnHours: [1, 24], brakePct: [2, 50], ddCap: [5, 90], flipMin: [1, 20], flipSteps: [0, 20], sawFlips: [2, 100], straddlePct: [0.1, 20], scalpStep: [0.1, 2], investment: [0.1, 1e6], leverage: [1, 50], stepPct: [0.1, 10], maxUnits: [1, 30], stopSteps: [1, 20], trailSteps: [1, 20], band: [0, 5], feeX: [1, 30], btDays: [1, 3.4], minTurnoverM: [1, 10000], idleMin: [5, 1440] };
const BOOL = ["limitEntry", "invert", "useFlow", "autoOpt", "flip", "autoInv", "straddleRearm", "scalp", "autoCoin", "runner", "autoTune", "btNeedPlus"];

function create(host) {
  const { bx, spec, align, sign, log, engineLog, lsGet, lsSet, env, keys } = host;
  const WS = host.WebSocket || WebSocketNode;
  const MAKER = host.makerFee != null ? host.makerFee : 0.02, TAKER = host.takerFee != null ? host.takerFee : 0.055;
  const PUB = host.testWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/public/linear" : "wss://stream.bybit.com/v5/public/linear");
  const PRIV = host.testPrivWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/private" : "wss://stream.bybit.com/v5/private");
  const now = () => Date.now();
  const P = Object.assign({}, DEFAULTS, lsGet("bg.p", {}));
  const save = () => lsSet("bg.p", P);
  let B = lsGet("bg.b", null);
  const saveB = () => { lsSet("bg.b", B); if (host.flush) host.flush(); };
  const TR = lsGet("bg.trades", []);
  const ST = { err: "", errAt: 0, price: 0, bid: 0, ask: 0, priceAt: 0, spec: null, hedge: false, busy: false, seen: new Set(), ex: {}, waitUntil: lsGet("bg.wait", 0), pubUp: false, privUp: false, dirAt: 0, mis: 0 };
  const setErr = (t) => { ST.err = t; ST.errAt = now(); log("bgrid", t); };
  const fmt = (v) => Number(v).toPrecision(6);
  const FLOW = FLOWMOD.create({ bx });

  // one queue for private requests (≤ 4/s; the grid bot has its own), a 10006 pauses and retries
  let qTail = Promise.resolve(), qLast = 0;
  function api(path, opts) {
    if (!opts || !opts.auth) return bx(path, opts);
    const run = async () => {
      const wait = Math.max(0, qLast + 250 - now(), (ST.pauseUntil || 0) - now()); if (wait) await new Promise(r => setTimeout(r, wait));
      qLast = now();
      try { return await bx(path, opts); }
      catch (e) { if (e.code === 10006 || /rate limit|too many visits/i.test(e.message)) { ST.pauseUntil = now() + 3000; await new Promise(r => setTimeout(r, 3000)); qLast = now(); return bx(path, opts); } throw e; }
    };
    const p2 = qTail.then(run, run); qTail = p2.catch(() => {}); return p2;
  }
  const px = (v, up) => Number(align(v, ST.spec.tickStr, up));
  const qa = (v, up) => Number(align(v, ST.spec.step, up));
  const r = () => 1 + B.stepPct / 100;
  const L = (j) => B.anchor * Math.pow(r(), j);
  const idxOf = (x) => Math.floor(Math.log(x / B.anchor) / Math.log(r()));
  const ix = (d) => (ST.hedge ? (d > 0 ? 1 : 2) : 0);

  // ---------------- market data ----------------
  let pub = null, pubPing = null, pubSym = "";
  function connectPub() {
    try { pub = new WS(PUB); } catch (e) { setTimeout(connectPub, 3000); return; }
    pub.on("open", () => { ST.pubUp = true; pubSym = ""; subPub(); clearInterval(pubPing); pubPing = setInterval(() => { try { pub.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    pub.on("message", (raw) => { let j; try { j = JSON.parse(String(raw)); } catch (e) { return; } if (!j.topic || !j.data) return;
      if (j.topic.startsWith("orderbook.")) return FLOW.onBook(j);
      if (j.topic.startsWith("publicTrade.")) return FLOW.onTrades(j.data);
      if (!j.topic.startsWith("tickers.")) return; const d = j.data;
      if (d.lastPrice) ST.price = Number(d.lastPrice); if (d.bid1Price) ST.bid = Number(d.bid1Price); if (d.ask1Price) ST.ask = Number(d.ask1Price); ST.priceAt = now(); });
    pub.on("close", () => { ST.pubUp = false; clearInterval(pubPing); setTimeout(connectPub, 2000); });
    pub.on("error", () => {});
  }
  const topics = (s2) => [`tickers.${s2}`, `orderbook.50.${s2}`, `publicTrade.${s2}`];
  function subPub() { if (!pub || !ST.pubUp || !B || pubSym === B.sym) return; if (pubSym) pub.send(JSON.stringify({ op: "unsubscribe", args: topics(pubSym) })); FLOW.reset(B.sym); pub.send(JSON.stringify({ op: "subscribe", args: topics(B.sym) })); pubSym = B.sym; }
  async function ticker(sym) {
    const x = ((await bx("/v5/market/tickers", { query: { category: "linear", symbol: sym } })).list || [])[0]; if (!x) throw new Error("немає " + sym);
    ST.price = Number(x.lastPrice); ST.bid = Number(x.bid1Price); ST.ask = Number(x.ask1Price); ST.priceAt = now(); return x;
  }
  async function candles5m(sym, n) {
    const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: sym, interval: "5", limit: String(n) } });
    return (k.list || []).map(x => ({ o: Number(x[1]), h: Number(x[2]), l: Number(x[3]), c: Number(x[4]) })).reverse();
  }
  // the direction of the breakout: EMA36 vs EMA96 on 5-minute candles with a band
  function dirOf(cs) {
    if (cs.length < 100) return { d: 0, why: "мало історії" };
    const cl = cs.map(k => k.c), f = BT.ema(cl, 36), s2 = BT.ema(cl, 96), n = cl.length - 1, diff = (f[n] / s2[n] - 1) * 100;
    const d = diff > P.band ? 1 : diff < -P.band ? -1 : 0;
    return { d, diff: +diff.toFixed(2), why: d > 0 ? `пробій вгору: EMA3г вище EMA8г на ${diff.toFixed(2)}%` : d < 0 ? `пробій вниз: EMA3г нижче EMA8г на ${Math.abs(diff).toFixed(2)}%` : `без напряму (різниця EMA ${diff.toFixed(2)}%)` };
  }

  // ---------------- private: fills ----------------
  let priv = null, privPing = null;
  function connectPriv() {
    if (!keys || !keys.key) return;
    try { priv = new WS(PRIV); } catch (e) { setTimeout(connectPriv, 5000); return; }
    priv.on("open", () => { const expires = now() + 10000; priv.send(JSON.stringify({ op: "auth", args: [keys.key, expires, sign("GET/realtime" + expires)] }));
      clearInterval(privPing); privPing = setInterval(() => { try { priv.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    priv.on("message", (raw) => { let j; try { j = JSON.parse(String(raw)); } catch (e) { return; }
      if (j.op === "auth") { if (j.success) { ST.privUp = true; priv.send(JSON.stringify({ op: "subscribe", args: ["execution", "order"] })); } return; }
      if (j.topic === "execution") for (const x of j.data || []) onExec(x);
      if (j.topic === "order") for (const o of j.data || []) onOrder(o); });
    priv.on("close", () => { ST.privUp = false; clearInterval(privPing); setTimeout(connectPriv, 3000); });
    priv.on("error", () => {});
  }
  // order link: bg<id>.<unit>.<o|t|s> (open / take / stop-close)
  const lid = (k, kind) => ("bg" + B.id + "." + k + "." + kind + (now() % 1e6).toString(36)).slice(0, 36);
  const parse = (id) => { const m = /^bg([a-z0-9]+)\.(\d+)\.([ots])/.exec(String(id || "")); return m && B && m[1] === B.id ? { k: Number(m[2]), kind: m[3] } : null; };
  const unit = (k) => B && B.units.find(u => u.k === k);
  function onExec(x) {
    if (!B || x.symbol !== B.sym || (x.execType && x.execType !== "Trade") || ST.seen.has(x.execId)) return;
    ST.seen.add(x.execId); if (ST.seen.size > 5000) ST.seen = new Set([...ST.seen].slice(-2000));
    const p = parse(x.orderLinkId); if (!p) return;
    const e = ST.ex[x.orderLinkId] = ST.ex[x.orderLinkId] || { q: 0, v: 0, fee: 0 };
    e.q += Number(x.execQty); e.v += Number(x.execQty) * Number(x.execPrice); e.fee += Number(x.execFee || 0);
    const u = unit(p.k); if (!u) return;
    if (e.q < u.qty - 1e-9) return;
    const avg = e.v / e.q;
    if (p.kind === "o" && u.state === "opening") {
      u.state = "open"; u.e = avg; u.fee = e.fee; u.at = now(); if (u.runner) u.pk = avg;
      if (u.tp) u.tp = u.d > 0 ? Math.max(u.tp, avg * r()) : Math.min(u.tp, avg / r());   // the take is at least one full step from the real entry
      saveB();
    }
    else if ((p.kind === "t" || p.kind === "s") && u.state !== "closed") finish(u, avg, e.fee, p.kind === "t" ? "тейк" : u.closeWhy || "стоп");
  }
  function onOrder(o) { const p = parse(o.orderLinkId); if (!p) return;
    { const u = unit(p.k); if (u && u.openOrd === o.orderLinkId && u.state === "opening" && u.lim && ["Cancelled", "Rejected", "Deactivated"].includes(o.orderStatus)) {
        if (acceptPartial(u)) saveB(); else if (!u.lim.quit) u.lim.redo = true; else { B.units = B.units.filter(z => z !== u); saveB(); } return; } }
    const u = unit(p.k); if (u && u.tpOrd === o.orderLinkId && ["Cancelled", "Rejected", "Deactivated"].includes(o.orderStatus)) { u.tpOrd = null; saveB(); } }
  function finish(u, x, fee, how) {
    const pnl = (x - u.e) * u.qty * real(u) - (u.fee || 0) - fee;
    B.units = B.units.filter(z => z !== u); B.profit = +((B.profit || 0) + pnl).toFixed(6); if (pnl > 0) B.wins = (B.wins || 0) + 1; else B.losses = (B.losses || 0) + 1;
    TR.push({ inv: !!u.inv, fee: +((u.fee || 0) + fee).toFixed(6), nt: +(u.e * u.qty).toFixed(4), at: now(), sym: B.sym, lev: B.lev, fs: B.flipSteps, st: B.stepPct, d: u.d, runner: !!u.runner, e: +Number(u.e).toPrecision(8), x: +Number(x).toPrecision(8), pnl: +pnl.toFixed(5), how, min: Math.round((now() - (u.at || now())) / 60000) });
    if (TR.length > 3000) TR.splice(0, TR.length - 3000); lsSet("bg.trades", TR);
    if (u.tpOrd) cancel(u.tpOrd);
    saveB();
    engineLog(`${u.runner ? "🏃" : "⚡"} ${B.sym}: ${u.runner ? "бігун" : "юніт"} ${real(u) > 0 ? "лонг" : "шорт"}${u.inv ? " 🔄" : ""} ${fmt(u.e)} → ${fmt(x)} (${how}) ${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} USDT`, pnl >= 0 ? "up" : "down");
  }
  async function cancel(id) { try { await api("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, orderLinkId: id } }); } catch (e) { /* gone */ } }
  const real = (u) => (u.inv ? -u.d : u.d);
  const touch = (side) => side === "Buy" ? px(ST.bid || ST.price, false) : px(ST.ask || ST.price, true);
  // ⚡ entry: a PostOnly limit at the best price of our side (maker); the loop moves it to the new best price until it fills
  async function enter(u) {
    const side = real(u) > 0 ? "Buy" : "Sell";
    if (!P.limitEntry) return market(u, "o", side, false);
    const id = lid(u.k, "o"), p0 = touch(side);
    u.openOrd = id; u.lim = Object.assign(u.lim || { n: 0, want: ST.price, t0: now() }, { at: now(), px: p0, redo: false });
    await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, side, orderType: "Limit", price: String(p0), qty: String(u.qty), timeInForce: "PostOnly", orderLinkId: id, positionIdx: ix(real(u)), reduceOnly: false } });
  }
  function acceptPartial(u) {   // an entry limit that stopped with part filled: the unit is that part
    const e = ST.ex[u.openOrd]; if (!e || !(e.q > 0)) return false;
    u.qty = +e.q.toFixed(8); u.state = "open"; u.e = e.v / e.q; u.fee = e.fee; u.at = now(); if (u.runner) u.pk = u.e;
    if (u.tp) u.tp = u.d > 0 ? Math.max(u.tp, u.e * r()) : Math.min(u.tp, u.e / r()); return true;
  }
  async function chase(x) {
    for (const u of B.units.slice()) {
      if (u.state !== "opening" || !u.lim) continue;
      const L2 = u.lim, side = real(u) > 0 ? "Buy" : "Sell", p0 = touch(side);
      if (L2.quit) { if (now() - L2.quitAt > 4000 && u.state === "opening") { if (!acceptPartial(u)) B.units = B.units.filter(z => z !== u); saveB(); } continue; }
      const far = Math.abs(x / L2.want - 1) * 100 > 2 * B.stepPct;
      if (far || L2.n >= 40 || (B.hold && !u.manualEntry)) {   // the price ran away: this entry is missed (no market chase)
        L2.quit = true; L2.quitAt = now(); saveB(); await cancel(u.openOrd);
        if (now() - (ST.missLogAt || 0) > 30000) { ST.missLogAt = now(); engineLog(`⏭ ${B.sym}: лімітний вхід не виконався — ціна пішла (${fmt(L2.want)} → ${fmt(x)}), вхід пропущено, ринком не доганяю`, "mute"); }
        continue;
      }
      if (L2.redo) { L2.n++; try { await enter(u); } catch (e) { setErr("лімітний вхід: " + e.message); } continue; }
      if (now() - L2.at > 1200 && p0 !== L2.px) {
        L2.n++; L2.at = now();
        try { await api("/v5/order/amend", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, orderLinkId: u.openOrd, price: String(p0) } }); L2.px = p0; }
        catch (e) { if (e.code === 110001 && !(ST.ex[u.openOrd] && ST.ex[u.openOrd].q > 0)) L2.redo = true; }
      }
    }
  }   // 🔄 mirror: the logic says long -> the account holds a short (and back)
  async function market(u, kind, side, reduce) {
    const id = lid(u.k, kind);
    if (kind === "o") u.openOrd = id; else u.closeOrd = id;
    await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, side, orderType: "Market", qty: String(u.qty), orderLinkId: id, positionIdx: ix(real(u)), reduceOnly: !!reduce, timeInForce: "IOC" } });
  }

  const FEES = {};
  async function feeOf(sym) {
    if (FEES[sym]) return FEES[sym];
    let maker = MAKER, taker = TAKER;
    try { const f = ((await api("/v5/account/fee-rate", { query: { category: "linear", symbol: sym }, auth: true })).list || [])[0]; if (f) { maker = Math.max(0, Number(f.makerFeeRate) * 100); taker = Number(f.takerFeeRate) * 100; } } catch (e) { /* defaults */ }
    return (FEES[sym] = { maker, taker });
  }
  // ---------------- choose coin and step ----------------
  async function plan(opt = {}) {
    let cands;
    if (opt.sym) cands = [{ sym: opt.sym }];
    else if (P.autoCoin) {
      const r2 = await bx("/v5/market/tickers", { query: { category: "linear" } });
      const busy = host.busySyms ? host.busySyms() : [];
      cands = (r2.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.turnover24h) >= P.minTurnoverM * 1e6 && Number(x.lastPrice) > 0 && !busy.includes(x.symbol) && !(ST.cool && ST.cool[x.symbol] > now()))
        .map(x => ({ sym: x.symbol, range: (Number(x.highPrice24h) - Number(x.lowPrice24h)) / Number(x.lastPrice) * 100, chg: Math.abs(Number(x.price24hPcnt || 0) * 100) }))
        .filter(x => x.range >= 3).sort((a, b) => (b.chg + b.range / 2) - (a.chg + a.range / 2)).slice(0, P.autoTune ? 6 : 1);   // movers: a breakout grid needs movement
    } else if (P.symbol) cands = [{ sym: P.symbol }];
    else throw new Error("вкажіть монету або увімкніть автовибір");
    const results = [];
    for (const cd of cands) {
      try {
        const s = await spec(cd.sym), fr = await feeOf(cd.sym), maker = fr.maker, taker = fr.taker;
        // every take must clearly beat the round trip (market entry + limit take): the step ≥ feeX × fees, and in scalping at least 2 × fees
        const entryFee = P.limitEntry ? maker : taker;
        const minStep = P.scalp ? 2 * (entryFee + maker) : P.feeX * (entryFee + maker);
        const cs = await candles5m(cd.sym, P.autoTune ? Math.min(1000, Math.round(P.btDays * 288)) : 120);
        const price = cs.length ? cs[cs.length - 1].c : 0, lev = Math.min(P.leverage, Math.floor(Number(s.maxLev || 10)));
        const minNotional = Math.max(Number(s.minNotional || 5) * 1.05, Number(s.minQty || 0) * price);
        if (P.autoInv && host.equity) { const eq = host.equity(); if (eq > 0) P.investment = +Math.max(0.1, eq * 0.75).toFixed(2);   /* 25% stays free: Bybit reserves margin for fees */ }
        const units = Math.min(P.maxUnits, Math.floor(P.investment * lev / minNotional) - 1);
        if (units < 1) { results.push({ sym: cd.sym, err: `замало коштів (мін. ордер ${minNotional.toFixed(2)} USDT)` }); continue; }
        const steps = P.scalp ? [+Math.max(P.scalpStep, minStep).toFixed(3)] : P.autoTune ? [...new Set([0.3, 0.5, 0.8, 1.2, P.stepPct].map(x => Math.max(x, minStep)).map(x => +x.toFixed(2)))] : [Math.max(P.stepPct, minStep)];
        let best = null;
        if (P.flip) {   // the take is fixed (scalpStep, at least 2 × fees); the turn distance is chosen on history
          const st = +Math.max(P.scalpStep, 2 * (entryFee + maker)).toFixed(3);
          // leverage: the highest whose worst drawdown on history stays within ddCap (and that still fits the exchange minimum)
          let levs = [...new Set(LADDER.map(l => Math.min(l, lev)))].filter(l => P.investment * l / minNotional >= 2);
          if (!levs.length) levs = [lev];
          const good = (x) => x && !x.liquidated && (x.maxDD == null || x.maxDD <= P.ddCap);
          for (const fs of P.flipSteps > 0 ? [P.flipSteps] : [P.flipMin, P.flipMin + 1, P.flipMin + 2]) for (const l of levs) {
            const un = Math.max(1, Math.min(P.maxUnits, Math.floor(P.investment * l / minNotional) - 1));
            const res = cs.length > 120 && P.autoTune ? BT.simulateFlip(cs, { invert: !!P.invert, stepPct: st, flipSteps: fs, maxUnits: un, investment: P.investment, lev: l, maker, taker, runner: P.runner, entryFee: P.limitEntry ? maker : taker }) : { roi: 0, wins: 0, losses: 0, flips: 0, maxDD: 0 };
            const c2 = Object.assign({ st, fs, lev: l, units: un }, res);
            if (!best || (good(c2) && !good(best)) || (good(c2) === good(best) && (good(c2) ? c2.roi > best.roi : c2.maxDD < best.maxDD))) best = c2;
          }
        }
        for (const st of P.flip ? [] : steps) {
          const res = cs.length > 120 && P.autoTune ? BT.simulateB2(cs, { stepPct: st, maxUnits: units, investment: P.investment, lev, maker, taker, stopSteps: P.stopSteps, runner: P.runner, trailSteps: P.trailSteps, band: P.band / 100 }) : { roi: 0, wins: 0, losses: 0, flips: 0 };
          if (!best || res.roi > best.roi) best = Object.assign({ st }, res);
        }
        results.push({ sym: cd.sym, fee: +(entryFee + maker).toFixed(4), step: best.st, fs: best.fs || 0, lev: best.lev || lev, maxDD: best.maxDD, units: best.units || units, roi: best.roi, wins: best.wins, losses: best.losses, flips: best.flips, dir: dirOf(cs), tested: cs.length });
      } catch (e) { results.push({ sym: cd.sym, err: e.message }); }
    }
    results.sort((a, b) => (b.roi ?? -1e9) - (a.roi ?? -1e9));
    ST.bt = { at: now(), days: P.btDays, results };
    const best = results.find(x => x.roi != null);
    if (!best) throw new Error(results.map(x => `${x.sym}: ${x.err}`).join("; ") || "немає кандидатів");
    if (!opt.force && P.autoCoin && P.autoTune && P.btNeedPlus && best.roi <= 0) throw Object.assign(new Error(`жодна монета не заробила б пробійною сіткою за ${P.btDays} дні (найкраща ${best.sym} ${best.roi}%) — чекаю 15 хв`), { wait: 15 });
    return best;
  }
  async function start(opt = {}) {
    const pl = await plan(opt);
    const s = await spec(pl.sym);
    ST.spec = { tickStr: s.tick, step: s.step, minQty: Number(s.minQty || 0), minNotional: Number(s.minNotional || 5), maxLev: Number(s.maxLev || 10) };
    try { ST.hedge = ((await api("/v5/position/list", { query: { category: "linear", symbol: pl.sym }, auth: true })).list || []).some(x => Number(x.positionIdx) > 0); } catch (e) { ST.hedge = false; }
    await ticker(pl.sym);
    const lev = Math.min(pl.lev || P.leverage, Math.floor(ST.spec.maxLev));
    try { await api("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: pl.sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); } catch (e) { /* not modified */ }
    const qty = Math.max(qa(P.investment * lev / (pl.units + 1) / ST.price, false), qa(Math.max(ST.spec.minNotional * 1.05, ST.spec.minQty * ST.price) / ST.price, true));
    B = { scalp: P.scalp, fee: pl.fee, id: now().toString(36).slice(-5), sym: pl.sym, stepPct: pl.step, maxUnits: pl.units, qty, lev, anchor: ST.price, last: 0, d: 0, units: [], nextK: 1, profit: 0, wins: 0, losses: 0, startedAt: now(), idleAt: now(), bt: { roi: pl.roi, wins: pl.wins, losses: pl.losses, flips: pl.flips, days: P.btDays, tested: pl.tested } };
    B.maxUnits = Math.max(1, Math.min(B.maxUnits, Math.floor(P.investment * lev / (qty * ST.price)) - 1));   // the minimum order rounds a unit up: only as many as the money really holds
    B.last = idxOf(ST.price);
    if (P.flip) {   // the first direction: the EMA if it has one, else the last hour's move
      const cs = await candles5m(pl.sym, 200), dd = dirOf(cs), h = cs.length > 12 ? cs[cs.length - 1].c - cs[cs.length - 13].c : 1;
      Object.assign(B, { inv: !!P.invert, levCap: lev, tuneAt: now(), flipsSince: 0, tunes: [], flip: true, flipSteps: Math.max(pl.fs || P.flipMin, P.flipMin), d: dd.d || (h < 0 ? -1 : 1), ext: ST.price, flipAt: [] });
    }
    saveB(); subPub();
    if (B.flip) {
      engineLog(`🔁 Пробій-розворот ${pl.sym}: плече x${lev} (на історії найгірша просадка ${pl.maxDD ?? "—"}% при межі ${P.ddCap}%), тейки ${pl.step}% (комісії ${pl.fee}% → чистими ≈ +${(pl.step - pl.fee).toFixed(3)}%), розворот при відкаті на ${B.flipSteps} рівні (${(B.flipSteps * pl.step).toFixed(2)}%), до ${B.maxUnits} юнітів по ${qty} + бігун, x${lev}${pl.tested ? ` · на історії за ${P.btDays} дні: ${pl.roi >= 0 ? "+" : ""}${pl.roi}%, тейків ${pl.wins}, розворотів ${pl.flips}` : ""} · старт ${B.d > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"}`, "up");
      await openAt(B.last, B.d, "старт"); return;
    }
    engineLog(`🚀 ${P.scalp ? "⚡ Суперскальпінг" : "Пробійна сітка"} ${pl.sym}: ${P.scalp ? `тейки ${pl.step}% (комісії за коло ${pl.fee}% → чистими ≈ +${(pl.step - pl.fee).toFixed(3)}% з тейка)` : `крок ${pl.step}%`}, до ${B.maxUnits} юнітів по ${qty} + бігун, x${lev}${pl.tested ? ` · на історії за ${P.btDays} дні: ${pl.roi >= 0 ? "+" : ""}${pl.roi}%, тейків ${pl.wins}, стопів ${pl.losses}` : ""}`, "up");
    await updateDir(true);
  }
  async function updateDir(force) {
    if (!B || B.flip || (!force && now() - ST.dirAt < 60000)) return;
    ST.dirAt = now();
    try {
      const dd = dirOf(await candles5m(B.sym, 200));
      B.dirInfo = dd;
      if (B.straddle) { saveB(); return; }                         // both-sides triggers armed: the breakout decides the direction, not the EMA
      if (B.manual && dd.d !== -B.d) { saveB(); return; }          // a direction set by hand holds until the trend turns clearly the other way
      if (dd.d !== B.d) {
        if (B.manual) { B.manual = false; engineLog(`🧭 ${B.sym}: тренд розвернувся проти ручного входу`, "gold-t"); }
        const old = B.d; B.d = dd.d; saveB();
        if (old !== 0 && B.units.length) { engineLog(`🔀 ${B.sym}: напрям змінився (${dd.why}) — закриваю всі юніти`, "gold-t"); for (const u of B.units.slice()) await closeUnit(u, "розворот"); }
        else engineLog(`🧭 ${B.sym}: ${dd.why}`, "mute");
      }
    } catch (e) { setErr("напрям: " + e.message); }
  }
  async function closeUnit(u, why) {
    if (u.state === "opening" || u.closing) return;
    u.closing = true; u.closeWhy = why;
    if (u.tpOrd) { await cancel(u.tpOrd); u.tpOrd = null; }
    try { await market(u, "s", real(u) > 0 ? "Sell" : "Buy", true); } catch (e) { u.closing = false; if (e.code === 110017) finish(u, ST.price, 0, why + " (позиції вже немає)"); else setErr("закриття юніта: " + e.message); }
    saveB();
  }

  async function openAt(j, way, why) {
    const out = [];
    if (B.flip && P.useFlow && !B.inv && !why && FLOW.sym() === B.sym) {
      const f = FLOW.snapshot();
      if (f.trades1m >= 5 && f.score * B.d < -P.flowGate) { B.gate = B.gate || {}; B.gate.skipped = (B.gate.skipped || 0) + 1; if (now() - (ST.gateLogAt || 0) > 60000) { ST.gateLogAt = now(); engineLog(`📊 ${B.sym}: потік проти ${B.d > 0 ? "лонгів" : "шортів"} (стрічка ${f.cvd1}, стакан ${f.imb}, сила ${f.score}) — нові входи пропускаю`, "mute"); } return out; }
    }
    if ((B.pausedUntil && now() < B.pausedUntil) || B.hold) return out;
    const microN = B.units.filter(u => !u.runner).length;
    if (microN < B.maxUnits && !B.units.some(u => !u.runner && u.j === j)) {
      const u = { inv: !!B.inv, k: B.nextK++, j, d: B.d, qty: B.qty, state: "opening", tp: L(j + B.d), sl: L(j - B.d * P.stopSteps), way };
      B.units.push(u); saveB();
      try { await enter(u); out.push("юніт"); engineLog(`⚡ ${B.sym}: ${why || (way === B.d ? "рух" : "відкат")} через ${fmt(L(j))} — ${real(u) > 0 ? "лонг" : "шорт"}${u.inv ? " 🔄" : ""} ${u.qty}, ${u.inv ? "вихід" : "тейк"} ${fmt(u.tp)}`, "mute"); }
      catch (e) { B.units = B.units.filter(z => z !== u); saveB(); setErr(e.code === 110007 ? "маржі на ще одну угоду не вистачає — нові відкриються, коли закриються поточні" : "вхід юніта: " + e.message); }
    }
    if (P.runner && way === B.d && !B.units.some(u => u.runner)) {
      const u = { inv: !!B.inv, k: B.nextK++, j, d: B.d, qty: B.qty, state: "opening", runner: true, sl: L(j - B.d * P.trailSteps), tp: null };
      B.units.push(u); saveB();
      try { await enter(u); out.push("бігун"); engineLog(`🏃 ${B.sym}: бігун ${real(u) > 0 ? "лонг" : "шорт"}${u.inv ? " 🔄" : ""} — трейлінг-стоп ${P.trailSteps} рівні за найкращою ціною`, "up"); }
      catch (e) { B.units = B.units.filter(z => z !== u); saveB(); }
    }
    return out;
  }
  // "enter now": the owner sets the direction; the breakout grid opens at once on the current/given coin (no waiting for a signal or for a plus on history)
  async function enterNow(dir, symIn) {
    if (!keys || !keys.key) return "Немає ключів Bybit";
    const sym = symIn ? (symIn.toUpperCase().endsWith("USDT") ? symIn.toUpperCase() : symIn.toUpperCase() + "USDT") : null;
    while (ST.busy) await new Promise(r2 => setTimeout(r2, 100));
    ST.busy = true;
    try {
      if (B && sym && B.sym !== sym) { if (B.units.length) return `Зараз відкриті юніти на ${B.sym} — спершу «Закрити все»`; B = null; }
      if (B && B.d === -dir && B.units.length) { for (const u of B.units.slice()) await closeUnit(u, "ручний розворот"); }
      if (!B) await start({ force: true, sym: sym || (P.autoCoin ? null : P.symbol || null) });
      if (!B) return "Не вдалося відкрити пробійну сітку: " + ST.err;
      P.on = true; save();
      if (!ST.price || now() - ST.priceAt > 5000) await ticker(B.sym);
      B.straddle = null; B.rearm = false; B.box = null;
      B.d = dir; B.manual = true; B.last = idxOf(ST.price); B.idleAt = now(); saveB();
      const opened = await openAt(B.last, dir, "ручний вхід");
      engineLog(`👆 ${B.sym}: ручний вхід ${dir > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"} зараз (${opened.join(" + ") || "уже відкрито"}) — далі сітка веде сама в цей бік, доки тренд явно не розвернеться`, dir > 0 ? "up" : "down");
      return `${B.sym}: ${dir > 0 ? "лонг" : "шорт"} відкрито (${opened.join(" + ") || "юніти вже є"})`;
    } catch (e) { return "Помилка: " + e.message; }
    finally { ST.busy = false; }
  }
  // "both sides": two triggers around the price; the side the price breaks first becomes the direction (the other trigger is dropped)
  function arm(pct) {
    const k = (pct || P.straddlePct) / 100, x = ST.price;
    B.straddle = { up: x * (1 + k), dn: x * (1 - k), pct: pct || P.straddlePct, at: now() };
    B.d = 0; B.manual = false; saveB();
    engineLog(`↕ ${B.sym}: чекаю пробою в будь-який бік — вище ${fmt(B.straddle.up)} лонг, нижче ${fmt(B.straddle.dn)} шорт`, "gold-t");
  }
  async function straddleNow(pct, symIn) {
    if (!keys || !keys.key) return "Немає ключів Bybit";
    const sym = symIn ? (symIn.toUpperCase().endsWith("USDT") ? symIn.toUpperCase() : symIn.toUpperCase() + "USDT") : null;
    while (ST.busy) await new Promise(r2 => setTimeout(r2, 100));
    ST.busy = true;
    try {
      if (B && sym && B.sym !== sym) { if (B.units.length) return `Зараз відкриті юніти на ${B.sym} — спершу «Закрити все»`; B = null; }
      if (B && B.units.length) return "Спершу закрийте поточні юніти («Закрити все») — межі ставляться на порожню сітку";
      if (!B) await start({ force: true, sym: sym || (P.autoCoin ? null : P.symbol || null) });
      if (!B) return "Не вдалося відкрити пробійну сітку: " + ST.err;
      P.on = true; save();
      if (!ST.price || now() - ST.priceAt > 5000) await ticker(B.sym);
      B.rearm = P.straddleRearm; arm(pct);
      return `${B.sym}: межі поставлено — вище ${fmt(B.straddle.up)} лонг, нижче ${fmt(B.straddle.dn)} шорт`;
    } catch (e) { return "Помилка: " + e.message; }
    finally { ST.busy = false; }
  }
  async function checkStraddle(x) {
    const t = B.straddle; if (!t) return;
    const dir = x >= t.up ? 1 : x <= t.dn ? -1 : 0; if (!dir) return;
    B.box = { up: t.up, dn: t.dn }; B.straddle = null; B.d = dir; B.manual = true; B.last = idxOf(x); B.idleAt = now(); saveB();
    engineLog(`💥 ${B.sym}: пробій ${dir > 0 ? "▲ ВГОРУ — лонг" : "▼ ВНИЗ — шорт"} (${fmt(x)}), друга межа знята`, dir > 0 ? "up" : "down");
    await openAt(B.last, dir, "пробій межі");
  }
  // ---------------- self-tuning (flip mode) ----------------
  const LADDER = [50, 30, 20, 10, 5, 3];
  function minLevNow() { const sp = ST.spec || {}, x = ST.price || 1; const need = Math.max((sp.minNotional || 5) * 1.05, (sp.minQty || 0) * x); return Math.max(1, Math.ceil(need * 2 / Math.max(0.01, P.investment))); }
  // change leverage / turn distance / take on the running coin (new units get the new size; open ones keep theirs)
  async function tune(o, why, who) {
    if (!B || !B.flip) return [];
    const ch = [], x = ST.price, maxL = Math.min(50, Math.floor((ST.spec && ST.spec.maxLev) || 50));
    if (o.lev != null && Number.isFinite(Number(o.lev))) {
      const l = Math.max(minLevNow(), Math.min(maxL, Math.round(Number(o.lev))));
      if (l !== B.lev) {
        ch.push(`плече x${B.lev}→x${l}`); B.lev = l; sizeUnits();
        try { await api("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, buyLeverage: String(l), sellLeverage: String(l) } }); } catch (e) { /* not modified / open position: the size above is what limits the risk */ }
      }
    }
    if (o.flipSteps != null && Number.isFinite(Number(o.flipSteps))) { const fs = Math.max(3, Math.min(10, Math.round(Number(o.flipSteps)))); if (fs !== B.flipSteps) { ch.push(`розворот ${B.flipSteps}→${fs} рівнів`); B.flipSteps = fs; } }
    if (o.takePct != null && Number.isFinite(Number(o.takePct))) {
      const tp = +Math.max(2 * (B.fee || 0.075), Math.min(1.5, Number(o.takePct))).toFixed(3);
      if (Math.abs(tp - B.stepPct) > 1e-9) { ch.push(`тейк ${B.stepPct}%→${tp}%`); B.stepPct = tp; B.anchor = x; B.last = idxOf(x); }
    }
    if (typeof o.useFlow === "boolean" && o.useFlow !== P.useFlow) { P.useFlow = o.useFlow; save(); ch.push(`потік ${o.useFlow ? "увімкнено" : "вимкнено"}`); }
    if (o.flowGate != null && Number.isFinite(Number(o.flowGate))) { const g = Math.max(0.05, Math.min(0.95, Number(o.flowGate))); if (Math.abs(g - P.flowGate) > 0.009) { ch.push(`поріг потоку ${P.flowGate}→${+g.toFixed(2)}`); P.flowGate = +g.toFixed(2); save(); } }
    if (o.maxUnits != null && Number.isFinite(Number(o.maxUnits))) { const m = Math.max(1, Math.min(10, Math.round(Number(o.maxUnits)))); if (m !== P.maxUnits) { ch.push(`юнітів ${P.maxUnits}→${m}`); P.maxUnits = m; save(); sizeUnits(); } }
    if (ch.length) {
      B.tuneAt = now(); B.flipsSince = 0; B.tunes = (B.tunes || []).concat({ at: now(), ch: ch.join(", "), why, who }).slice(-20); saveB();
      engineLog(`${who === "ai" ? "🤖 AI" : who === "learn" ? "⚡ Навчання" : "🧪 Самонастройка"} ${B.sym}: ${ch.join(", ")} — ${why}`, "gold-t");
    }
    return ch;
  }
  // unit size from the money now (balance × 75% when sizing from the balance) and the leverage now
  function sizeUnits() {
    const x = ST.price; if (!B || !ST.spec || !x) return;
    if (P.autoInv && host.equity) { const eq = host.equity(); if (eq > 0) P.investment = +Math.max(0.1, eq * 0.75).toFixed(2); }
    const minN = Math.max(ST.spec.minNotional * 1.05, ST.spec.minQty * x);
    const un = Math.max(1, Math.min(P.maxUnits, Math.floor(P.investment * B.lev / minN) - 1));
    B.qty = Math.max(qa(P.investment * B.lev / (un + 1) / x, false), qa(minN / x, true));
    B.maxUnits = Math.max(1, Math.min(un, Math.floor(P.investment * B.lev / (B.qty * x)) - 1));
  }
  // the bot's own rule on live results since the last change
  async function selfTune() {
    if (!B || !B.flip || B.leaving) return;
    const since = TR.filter(t => t.sym === B.sym && t.at >= (B.tuneAt || 0)), sum = since.reduce((a, t) => a + t.pnl, 0);
    if ((B.flipsSince || 0) >= 1 && sum < 0) {
      const lower = LADDER.find(l => l < B.lev && l >= minLevNow()), fs = Math.min(8, B.flipSteps + 1);
      if (lower == null && fs === B.flipSteps) {
        if (P.autoCoin) { engineLog(`🧪 ${B.sym}: і з найменшим ризиком у мінус (${sum.toFixed(4)} USDT) — міняю монету`, "gold-t"); B.leaving = true; ST.cool = ST.cool || {}; ST.cool[B.sym] = now() + 3 * 3600e3; saveB(); for (const u of B.units.slice()) await closeUnit(u, "зміна монети"); }
        return;
      }
      await tune({ lev: lower != null ? lower : B.lev, flipSteps: fs }, `за ${B.flipsSince} розворот(и) ${sum.toFixed(4)} USDT — зменшую ризик`, "self");
      if (host.onLoss) host.onLoss();
      setTimeout(() => learn("після мінусу").catch(() => {}), 3000);
    } else if ((B.flipsSince || 0) >= 4 && sum > 0 && B.lev < (B.levCap || B.lev)) {
      const up = LADDER.slice().reverse().find(l => l > B.lev && l <= B.levCap);
      if (up) await tune({ lev: up }, `за ${B.flipsSince} розворотів +${sum.toFixed(4)} USDT — повертаю плече`, "self");
    }
  }
  // ⚡ fast learning: the freshest hours of this coin on 1-minute candles -> the take, the turn and the leverage that earned most just now
  async function learn(why) {
    if (!B || !B.flip || B.leaving || ST.learning) return;
    ST.learning = true; ST.learnAt = now();
    try {
      const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: B.sym, interval: "1", limit: String(Math.min(1000, P.learnHours * 60)) } });
      const cs = (k.list || []).map(x => ({ o: Number(x[1]), h: Number(x[2]), l: Number(x[3]), c: Number(x[4]) })).reverse();
      if (cs.length < 60) return;
      const fr = await feeOf(B.sym), fee = (P.limitEntry ? fr.maker : fr.taker) + fr.maker, x = ST.price, minN = Math.max(ST.spec.minNotional * 1.05, ST.spec.minQty * x);
      const takes = [0.2, 0.3, 0.4, 0.5, 0.7, 1.0].filter(t => t >= 2 * fee), flipsC = [3, 4, 5, 6, 8], levs = LADDER.filter(l => l <= (B.levCap || B.lev) && l >= minLevNow());
      if (!levs.length) levs.push(B.lev);
      const sim = (t, fs, l) => { const un = Math.max(1, Math.min(P.maxUnits, Math.floor(P.investment * l / minN) - 1));
        return BT.simulateFlip(cs, { invert: !!B.inv, stepPct: t, flipSteps: fs, maxUnits: un, investment: P.investment, lev: l, maker: fr.maker, taker: fr.taker, runner: P.runner, entryFee: P.limitEntry ? fr.maker : fr.taker }); };
      const good = (r2) => !r2.liquidated && r2.maxDD <= P.ddCap;
      let best = null;
      for (const t of takes) for (const fs of flipsC) for (const l of levs) { const r2 = sim(t, fs, l); if (good(r2) && r2.roi > 0 && (!best || r2.roi > best.roi)) best = Object.assign({ t, fs, l }, r2); }
      const cur = sim(B.stepPct, B.flipSteps, B.lev);
      ST.learned = { at: now(), hours: +(cs.length / 60).toFixed(1), cur: cur.roi, best: best && { take: best.t, flipSteps: best.fs, lev: best.l, roi: best.roi, maxDD: best.maxDD } };
      if (!best) {   // nothing earned on this coin in the last hours
        if (P.autoCoin) { engineLog(`⚡ Навчання ${B.sym}: за останні ${ST.learned.hours} год жодні налаштування не дали плюсу — міняю монету`, "gold-t"); ST.cool = ST.cool || {}; ST.cool[B.sym] = now() + 3 * 3600e3; B.leaving = true; saveB(); for (const u of B.units.slice()) await closeUnit(u, "навчання: монета без плюсу"); }
        else await tune({ lev: minLevNow(), flipSteps: Math.max(B.flipSteps, 6) }, `за останні ${ST.learned.hours} год жодні налаштування не дали плюсу — мінімальний ризик`, "learn");
        return;
      }
      // switch only for a clear gain (otherwise the settings would jump on noise)
      if (best.roi > cur.roi + Math.max(2, Math.abs(cur.roi) * 0.2) && (best.t !== B.stepPct || best.fs !== B.flipSteps || best.l !== B.lev))
        await tune({ takePct: best.t, flipSteps: best.fs, lev: best.l }, `${why}: за останні ${ST.learned.hours} год так було б ${best.roi >= 0 ? "+" : ""}${best.roi}% (просадка ${best.maxDD}%) проти ${cur.roi >= 0 ? "+" : ""}${cur.roi}% із поточними`, "learn");
    } catch (e) { setErr("навчання: " + e.message); }
    finally { ST.learning = false; }
  }
  // emergency brake between reversals: the result since the last change (closed + open) below -brakePct % -> less risk at once
  async function brake() {
    if (!B || !B.flip || B.leaving || !P.autoOpt) return;
    const x = ST.price, closed = TR.filter(t => t.sym === B.sym && t.at >= (B.tuneAt || 0)).reduce((a, t) => a + t.pnl, 0);
    const open = B.units.filter(u => u.state === "open").reduce((a, u) => a + (x - u.e) * u.qty * real(u), 0);
    if (closed + open < -P.investment * P.brakePct / 100 && now() - (B.tuneAt || 0) > 60000) {
      const lower = LADDER.find(l => l < B.lev && l >= minLevNow());
      if (lower != null) { await tune({ lev: lower }, `гальмо: ${(closed + open).toFixed(4)} USDT (−${P.brakePct}% суми) ще до розвороту`, "self"); if (host.onLoss) host.onLoss(); }
      else B.tuneAt = now();
    }
  }
  setInterval(() => { if (B && B.flip && P.autoOpt && now() - (ST.learnAt || 0) >= P.learnMin * 60000) learn("навчання").catch(() => {}); brake().catch(() => {}); }, 20000);

  // what the AI sees
  function brief() {
    if (!B) return null;
    const since = TR.filter(t => t.sym === B.sym && t.at >= (B.tuneAt || 0)), last = TR.slice(-40);
    return { sym: B.sym, dir: B.d, price: ST.price, lev: B.lev, levCap: B.levCap, maxLev: ST.spec && ST.spec.maxLev, minLev: minLevNow(), takePct: B.stepPct, fee: B.fee, flipSteps: B.flipSteps, flips: B.flips || 0,
      flipsLastHour: (B.flipAt || []).filter(t => now() - t < 3600e3).length, openUnits: B.units.length, coinProfit: B.profit, manualCoin: !P.autoCoin,
      sinceLastChange: { min: Math.round((now() - (B.tuneAt || B.startedAt)) / 60000), trades: since.length, pnl: +since.reduce((a, t) => a + t.pnl, 0).toFixed(4), flips: B.flipsSince || 0 },
      lastTrades: last.map(t => ({ min: Math.round((now() - t.at) / 60000), sym: t.sym, d: t.d, pnl: t.pnl, how: t.how, lev: t.lev, fs: t.fs })),
      changes: (B.tunes || []).slice(-6).map(t => ({ min: Math.round((now() - t.at) / 60000), ch: t.ch, who: t.who })),
      flow: FLOW.sym() === B.sym ? FLOW.snapshot() : null, flowGate: P.flowGate, useFlow: P.useFlow, maxUnits: P.maxUnits, hold: !!B.hold, gate: B.gate || {}, learned: ST.learned || null,
      history: ST.bt ? ST.bt.results.filter(r2 => r2.roi != null).slice(0, 6).map(r2 => ({ sym: r2.sym, roi: r2.roi, maxDD: r2.maxDD, lev: r2.lev, fs: r2.fs, flips: r2.flips })) : [] };
  }
  // 🔄 the mirror button: close what is open and continue with the opposite side
  async function setInvert(on) {
    P.invert = !!on; save();
    if (!B || !B.flip) return on ? "Дзеркало увімкнено: наступні угоди будуть навпаки" : "Дзеркало вимкнено";
    if (!!B.inv === !!on) return on ? "Дзеркало вже увімкнене" : "Дзеркало вже вимкнене";
    for (const u of B.units.slice()) if (u.state === "open") await closeUnit(u, on ? "увімкнення дзеркала" : "вимкнення дзеркала");
    B.inv = !!on; B.hold = null; B.tuneAt = now(); B.flipsSince = 0; B.ext = ST.price; B.last = idxOf(ST.price); saveB();
    engineLog(on ? `🔄 ${B.sym}: ДЗЕРКАЛО — логіка та сама, але замість купівель продажі, замість продажів купівлі (вихід там, де був би тейк, — ринком; плюс там, де був би розворот)` : `🔄 ${B.sym}: дзеркало вимкнено — знову звичайна торгівля`, "gold-t");
    if (P.on) await openAt(B.last, B.d, on ? "дзеркало" : "звичайний режим");
    return on ? "Дзеркало увімкнено" : "Дзеркало вимкнено";
  }
  // "what if mirrored": the same trades with the opposite side (all exits by market)
  function mirrorEst(ms) {
    const t = TR.filter(x => x.at >= now() - ms && x.fee != null);
    let normal = 0, mirror = 0;
    for (const x of t) { const gross = x.pnl + x.fee, mf = x.nt * 2 * TAKER / 100; if (x.inv) { mirror += x.pnl; normal += -gross - x.nt * (TAKER + MAKER) / 100; } else { normal += x.pnl; mirror += -gross - mf; } }
    return { n: t.length, normal: +normal.toFixed(4), mirror: +mirror.toFixed(4) };
  }
  // AI: take a direction now / pause new entries
  async function forceDir(d, why) {
    if (!B || !B.flip || (d !== 1 && d !== -1)) return "";
    if (d !== B.d) { for (const u of B.units.slice()) await closeUnit(u, "напрям від AI"); B.d = d; }
    B.hold = null; B.ext = ST.price; B.last = idxOf(ST.price); saveB();
    engineLog(`🤖 AI ${B.sym}: ${d > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"} зараз — ${why}`, d > 0 ? "up" : "down");
    if (P.on) await openAt(B.last, d, "AI"); return "ok";
  }
  function pause(min, why) { if (!B) return ""; B.pausedUntil = now() + Math.max(1, Math.min(120, min)) * 60000; saveB(); engineLog(`🤖 AI ${B.sym}: пауза нових входів на ${Math.round(min)} хв — ${why}`, "gold-t"); return "ok"; }
  async function changeCoin(sym, why) {
    if (!B) return "";
    engineLog(`🤖 AI: міняю монету ${B.sym}${sym ? " → " + sym : ""} — ${why}`, "gold-t");
    ST.cool = ST.cool || {}; ST.cool[B.sym] = now() + 3 * 3600e3;
    if (sym) ST.nextSym = /USDT$/.test(sym) ? sym : sym + "USDT";   // a one-time hint: after it the bot picks coins itself again
    B.leaving = true; saveB(); for (const u of B.units.slice()) await closeUnit(u, "зміна монети (AI)");
    return "ok";
  }

  // ---------------- the loop: every 500 ms ----------------
  async function loop() {
    if (ST.busy) return; ST.busy = true;
    try {
      if (!keys || !keys.key) return;
      if (!B) { if (P.on && now() >= ST.waitUntil) { try { const hint = ST.nextSym; ST.nextSym = null; await start(hint ? { sym: hint, force: true } : {}); } catch (e) { setErr(e.message); ST.waitUntil = now() + (e.wait || 1) * 60000; lsSet("bg.wait", ST.waitUntil); if (e.wait) engineLog("⏸ " + e.message, "gold-t"); } } return; }
      if (!ST.spec) { const s = await spec(B.sym); ST.spec = { tickStr: s.tick, step: s.step, minQty: Number(s.minQty || 0), minNotional: Number(s.minNotional || 5), maxLev: Number(s.maxLev || 10) }; }
      if (!ST.price || now() - ST.priceAt > 10000) await ticker(B.sym);
      subPub();
      await updateDir(false);
      if (B.flip && P.useFlow) FLOW.refresh();
      if (P.limitEntry) await chase(ST.price);
      const x = ST.price;
      if (B.resync) { B.resync = false; B.last = idxOf(x); saveB(); }
      if (B.straddle) { await checkStraddle(x); if (B.straddle) { B.idleAt = now(); return; } }
      // a false breakout: after the break up the price fell through the lower trigger (or the other way) -> everything turns to the other side
      if (B.box && B.manual && !B.box.flipped && ((B.d > 0 && x <= B.box.dn) || (B.d < 0 && x >= B.box.up))) {
        B.box.flipped = true;                                        // one turn per pair of triggers: a saw does not keep flipping
        const nd = -B.d;
        engineLog(`🔁 ${B.sym}: хибний пробій — ціна пробила ${nd > 0 ? "верхню" : "нижню"} межу, перевертаюсь у ${nd > 0 ? "лонг" : "шорт"}`, "gold-t");
        for (const u of B.units.slice()) await closeUnit(u, "хибний пробій");
        B.d = nd; B.last = idxOf(x); B.flips = (B.flips || 0) + 1; saveB();
        await openAt(B.last, nd, "пробій іншої межі");
      }
      // both sides again: the breakout trades are done -> new triggers around the price
      if (B.rearm && B.manual && !B.units.length && B.wasBusy && P.on) { B.wasBusy = false; B.box = null; arm(); return; }
      if (B.units.length) B.wasBusy = true;   // after a restart: continue from the price now (no catch-up of missed levels)
      if (B.flip) {
        // units left from the old direction (their entry was still in flight at the turn) are closed
        for (const u of B.units.slice()) if (u.d !== B.d && u.state === "open" && !u.closing) await closeUnit(u, "розворот");
        B.ext = B.d > 0 ? Math.max(B.ext || x, x) : Math.min(B.ext || x, x);
        if (B.flipSteps < P.flipMin) { B.flipSteps = P.flipMin; saveB(); engineLog(`🔁 ${B.sym}: розворот тепер лише при відкаті на ${B.flipSteps} рівні (${(B.flipSteps * B.stepPct).toFixed(2)}%) — шум більше не розвертає`, "gold-t"); }
        const back = Math.pow(r(), B.flipSteps);
        const f = P.useFlow && !B.inv && FLOW.sym() === B.sym ? FLOW.snapshot() : null, live = f && f.trades1m >= 5;
        // a hold after a turn the flow did not confirm: wait for the flow, a level in the new direction, or a new extreme of the old one
        if (B.hold) {
          const h = B.hold, lvl = Math.pow(r(), 1);
          if ((h.old > 0 && x > h.ext) || (h.old < 0 && x < h.ext)) {   // the pullback was absorbed: the old direction goes on
            B.d = h.old; B.ext = x; B.last = idxOf(x); B.hold = null; B.gate = B.gate || {}; B.gate.saved = (B.gate.saved || 0) + 1; saveB();
            engineLog(`📊 ${B.sym}: відкат поглинули — продовжую ${B.d > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"} без розвороту (уникнув пилки)`, "up");
            if (P.on) await openAt(B.last, B.d, "продовження"); return;
          }
          const confirmed = (live && f.score * B.d > 0.1) || (B.d > 0 ? x >= h.px * lvl : x <= h.px / lvl) || now() - h.at > 15 * 60000;
          if (!confirmed) return;
          B.hold = null; B.ext = x; B.last = idxOf(x); saveB();
          engineLog(`📊 ${B.sym}: розворот підтверджено${live ? ` (потік ${f.score})` : ""} — входжу в ${B.d > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"}`, B.d > 0 ? "up" : "down");
          if (P.on) await openAt(B.last, B.d, "розворот"); return;
        }
        const full = (B.d > 0 && x <= B.ext / back) || (B.d < 0 && x >= B.ext * back);
        const half = (B.d > 0 && x <= B.ext / Math.sqrt(back)) || (B.d < 0 && x >= B.ext * Math.sqrt(back));
        const early = !full && half && live && f.score * B.d < -P.earlyFlip;
        if (full || early) {
          const nd = -B.d, was = B.ext;
          if (early) { B.gate = B.gate || {}; B.gate.early = (B.gate.early || 0) + 1; engineLog(`📊 ${B.sym}: сильний потік проти (сила ${f.score}, стрічка ${f.cvd1}, швидкість ×${f.speedX}) — розвертаюсь раніше, на півдорозі`, "gold-t"); }
          for (const u of B.units.slice()) { if (u.state === "opening" && u.lim && !u.lim.quit) { u.lim.quit = true; u.lim.quitAt = now(); await cancel(u.openOrd); } else await closeUnit(u, "розворот"); }
          B.d = nd; B.ext = x; B.last = idxOf(x); B.flips = (B.flips || 0) + 1; B.flipsSince = (B.flipsSince || 0) + 1;
          sizeUnits();   // the balance changed: the new direction's units fit the money now
          if (P.autoOpt) { const b0 = B; setTimeout(() => { if (B === b0) selfTune().catch(e => setErr("самонастройка: " + e.message)); }, 6000); }
          B.flipAt = (B.flipAt || []).filter(t => now() - t < 3600e3).concat(now()); saveB();
          engineLog(`🔁 ${B.sym}: відкат ${B.flipSteps} рівні від ${fmt(was)} — розворот у ${nd > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"} (${fmt(x)})`, nd > 0 ? "up" : "down");
          if (P.autoCoin && !B.inv && B.flipAt.length >= P.sawFlips) {   // a saw: the price twitches less than the turn distance -> another coin
            engineLog(`🪚 ${B.sym}: ${B.flipAt.length} розворотів за годину — це пилка, закриваю і шукаю іншу монету`, "gold-t");
            B.leaving = true; saveB(); ST.cool = ST.cool || {}; ST.cool[B.sym] = now() + 3 * 3600e3;
            return;
          }
          // the flow still pushes the old way -> positions closed, but the new direction waits for confirmation
          if (live && f.score * (-nd) > P.flowGate) { B.hold = { at: now(), px: x, ext: was, old: -nd }; B.gate = B.gate || {}; B.gate.holds = (B.gate.holds || 0) + 1; saveB();
            engineLog(`📊 ${B.sym}: потік ще за ${-nd > 0 ? "покупців" : "продавців"} (сила ${f.score}) — позиції закрив, у ${nd > 0 ? "лонг" : "шорт"} увійду після підтвердження`, "mute"); return; }
          if (P.on) await openAt(B.last, nd, "розворот");
          return;
        }
      }
      // stops and the runner's trailing stop (the flip mode has none: the turn replaces them)
      for (const u of B.flip ? [] : B.units.slice()) {
        if (u.state !== "open" || u.closing) continue;
        if (u.runner) {
          u.pk = u.d > 0 ? Math.max(u.pk || u.e, x) : Math.min(u.pk || u.e, x);
          const t = u.d > 0 ? u.pk / Math.pow(r(), P.trailSteps) : u.pk * Math.pow(r(), P.trailSteps);
          u.sl = u.d > 0 ? Math.max(u.sl, t) : Math.min(u.sl, t);
        }
        if ((u.d > 0 && x <= u.sl) || (u.d < 0 && x >= u.sl)) await closeUnit(u, u.runner ? "трейлінг бігуна" : "стоп");
      }
      // takes of open units
      for (const u of B.units.slice()) if (u.inv && u.state === "open" && !u.runner && !u.closing && u.tp && ((u.d > 0 && x >= u.tp) || (u.d < 0 && x <= u.tp))) await closeUnit(u, "дзеркало: вихід");
      for (const u of B.units) {
        if (u.state !== "open" || u.runner || u.tpOrd || u.closing || u.inv) continue;
        let tp = px(u.tp, u.d > 0);
        if (u.d > 0 && ST.bid && tp <= ST.bid) tp = px(ST.ask, true);
        if (u.d < 0 && ST.ask && tp >= ST.ask) tp = px(ST.bid, false);
        const id = lid(u.k, "t"); u.tpOrd = id;
        try { await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, side: u.d > 0 ? "Sell" : "Buy", orderType: "Limit", qty: String(u.qty), price: String(tp), timeInForce: "PostOnly", orderLinkId: id, positionIdx: ix(u.d), reduceOnly: true } }); saveB(); }
        catch (e) { u.tpOrd = null; if (e.code !== 110017) setErr("тейк юніта: " + e.message); }
      }
      // level crossings -> new units in the breakout direction
      const i = idxOf(x);
      if (i !== B.last) {
        const crossed = [];
        for (let j = B.last + 1; j <= i && j <= B.last + 3; j++) crossed.push([j, 1]);
        for (let j = B.last; j > i && j > B.last - 3; j--) crossed.push([j, -1]);
        B.last = i; saveB();
        if (P.on && B.d !== 0 && !B.leaving) for (const [j, way] of crossed) await openAt(j, way);
      }
      if ((B.pendingRestart || B.leaving) && !B.units.length) { B = null; saveB(); ST.waitUntil = 0; return; }   // the scalping switch waits for the units to close
      if (B.units.length || B.d !== 0) B.idleAt = now();
      // a coin without direction and without units for idleMin -> another coin
      if (P.autoCoin && !B.units.length && now() - B.idleAt > P.idleMin * 60000) { engineLog(`🔄 ${B.sym}: ${P.idleMin} хв без напряму — шукаю іншу монету`, "mute"); B = null; saveB(); }
    } catch (e) { setErr(e.message); }
    finally { ST.busy = false; }
  }
  setInterval(loop, 500);

  // reconcile: units whose order vanished, the position on the exchange vs the units
  async function reconcile() {
    if (!B || !keys || !keys.key) return;
    try {
      if (!ST.privUp) {
        const r2 = await api("/v5/execution/list", { query: { category: "linear", symbol: B.sym, limit: "100", startTime: String(now() - 120000) }, auth: true });
        for (const x of (r2.list || []).slice().reverse()) onExec(Object.assign({ symbol: B.sym }, x));
      }
      const live = new Set(((await api("/v5/order/realtime", { query: { category: "linear", symbol: B.sym, limit: "50" }, auth: true })).list || []).map(o => o.orderLinkId));
      for (const u of B.units) {
        if (u.tpOrd && !live.has(u.tpOrd)) {
          const h = ((await api("/v5/order/history", { query: { category: "linear", symbol: B.sym, orderLinkId: u.tpOrd }, auth: true })).list || [])[0];
          if (h && h.orderStatus === "Filled") finish(u, Number(h.avgPrice) || u.tp, Number(h.cumExecFee || 0), "тейк"); else u.tpOrd = null;
        }
      }
      const owned = new Set(B.units.flatMap(u => [u.tpOrd, u.state === "opening" ? u.openOrd : null]).filter(Boolean));
      for (const id of live) if (/^bg/.test(id) && !owned.has(id)) await cancel(id);
      const pos = ((await api("/v5/position/list", { query: { category: "linear", symbol: B.sym }, auth: true })).list || []).reduce((s2, z) => s2 + Number(z.size || 0) * (z.side === "Buy" ? 1 : z.side === "Sell" ? -1 : 0), 0);
      const mine = B.units.filter(u => u.state === "open").reduce((s2, u) => s2 + u.qty * real(u), 0);
      if (Math.abs(pos - mine) > B.qty * 0.5 && !B.units.some(u => u.state === "opening" || u.closing)) {
        if (++ST.mis >= 3) {
          ST.mis = 0;
          // units the exchange no longer holds are dropped (closed while the server was down); an extra position is closed
          if (Math.abs(pos) < Math.abs(mine) - B.qty * 0.5) { let extra = Math.round((Math.abs(mine) - Math.abs(pos)) / B.qty); for (const u of B.units.slice().reverse()) { if (!extra) break; if (u.state === "open") { finish(u, ST.price, 0, "закрито поза ботом"); extra--; } } }
          else if (Math.abs(pos) > Math.abs(mine) + B.qty * 0.5) {
            const d = pos > 0 ? 1 : -1, q = qa(Math.abs(pos) - Math.abs(mine), false);
            try { await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, side: d > 0 ? "Sell" : "Buy", orderType: "Market", qty: String(q), positionIdx: ix(d), reduceOnly: true, timeInForce: "IOC" } }); } catch (e) { /* next time */ }
          }
          engineLog(`🔧 ${B.sym}: позиція на біржі ${pos} ≠ юніти ${+mine.toFixed(8)} — вирівняно`, "mute");
        }
      } else ST.mis = 0;
    } catch (e) { setErr("звірка: " + e.message); }
  }
  setInterval(reconcile, 5000);

  async function boot() { connectPub(); connectPriv(); if (B) { B.resync = true; } if (B) engineLog(`↺ Пробійна сітка ${B.sym} продовжує роботу після перезапуску`, "mute"); }
  function setOn(on) { P.on = !!on; save(); engineLog(on ? "▶ Пробійна сітка запущена" : "■ Пробійна сітка зупинена: нових юнітів немає (відкриті доходять до тейку/стопу)", on ? "up" : "mute"); if (on) { ST.waitUntil = 0; lsSet("bg.wait", 0); } }
  async function closeAll() { if (!B) return "Немає пробійної сітки"; B.leaving = true; if (B.straddle || B.rearm || B.box) { B.straddle = null; B.rearm = false; B.box = null; saveB(); } for (const u of B.units.slice()) { if (u.state === "open") await closeUnit(u, "закрито власником"); } const sym = B.sym; setTimeout(() => { if (B && !B.units.length) { B = null; saveB(); } }, 6000); return `Юніти ${sym} закриваються ринком`; }
  async function restart() { if (B && B.units.length) return "Спершу закрийте юніти («Закрити все»)"; B = null; saveB(); ST.waitUntil = 0; return "Шукаю нову монету й крок"; }
  function setParams(o) {
    const ch = [];
    for (const [k, v] of Object.entries(o || {})) {
      if (NUM[k]) { let n = Number(v); if (!Number.isFinite(n)) continue; n = Math.max(NUM[k][0], Math.min(NUM[k][1], n)); if (["leverage", "maxUnits", "stopSteps", "trailSteps"].includes(k)) n = Math.round(n); if (P[k] !== n) { ch.push(`${k} ${P[k]}→${n}`); P[k] = n; } }
      else if (BOOL.includes(k) && typeof v === "boolean") { if (P[k] !== v) { ch.push(`${k} ${P[k]}→${v}`); P[k] = v; } }
      else if (k === "symbol" && typeof v === "string") { const s2 = v.trim().toUpperCase(); const sym = s2 ? (s2.endsWith("USDT") ? s2 : s2 + "USDT") : ""; if (P.symbol !== sym) { ch.push(`монета ${sym || "—"}`); P.symbol = sym; } }
    }
    save(); return ch;
  }
  function view() {
    const x = ST.price, day = TR.filter(t => t.at >= now() - 86400e3);
    const b = B && { inv: !!B.inv, mirror24: mirrorEst(86400e3), mirror6: mirrorEst(6 * 3600e3), flow: B.flip && FLOW.sym() === B.sym ? FLOW.snapshot() : null, hold: !!B.hold, gate: B.gate || {}, paused: B.pausedUntil > now() ? Math.ceil((B.pausedUntil - now()) / 60000) : 0, learned: ST.learned || null, levCap: B.levCap || null, tunes: (B.tunes || []).slice(-5).reverse(), flip: !!B.flip, flipSteps: B.flipSteps, flips: B.flips || 0, leaving: !!B.leaving, ext: B.ext ? +fmt(B.ext) : null, turnPx: B.flip && B.ext ? +fmt(B.d > 0 ? B.ext / Math.pow(1 + B.stepPct / 100, B.flipSteps) : B.ext * Math.pow(1 + B.stepPct / 100, B.flipSteps)) : null, box: B.box ? { up: +fmt(B.box.up), dn: +fmt(B.box.dn) } : null, straddle: B.straddle ? { up: +fmt(B.straddle.up), dn: +fmt(B.straddle.dn), pct: B.straddle.pct } : null, rearm: !!B.rearm, manual: !!B.manual, scalp: !!B.scalp, fee: B.fee, pending: !!B.pendingRestart, sym: B.sym, stepPct: B.stepPct, qty: B.qty, lev: B.lev, maxUnits: B.maxUnits, d: B.d, dir: B.dirInfo || null, profit: +(B.profit || 0).toFixed(4), wins: B.wins || 0, losses: B.losses || 0, bt: B.bt, startedAt: B.startedAt,
      levels: [-3, -2, -1, 0, 1, 2, 3].map(k => +fmt(L(B.last + k + (k > 0 ? 0 : 0)))),
      units: B.units.map(u => ({ d: u.d, runner: !!u.runner, state: u.state, e: u.e || null, tp: u.tp ? +fmt(u.tp) : null, sl: u.sl ? +fmt(u.sl) : null, inv: !!u.inv, pnl: u.e && x ? +((x - u.e) * u.qty * real(u)).toFixed(4) : null, way: u.way })),
      unreal: +B.units.filter(u => u.state === "open").reduce((s2, u) => s2 + (x - u.e) * u.qty * real(u), 0).toFixed(4) };
    return { params: P, price: x, err: now() - ST.errAt < 120000 ? ST.err : "", b, bt: ST.bt || null, waitIn: !B && P.on && ST.waitUntil > now() ? Math.round((ST.waitUntil - now()) / 1000) : null,
      stats: { n: TR.length, pnl: +TR.reduce((a, t) => a + t.pnl, 0).toFixed(4), win: TR.length ? Math.round(TR.filter(t => t.pnl > 0).length / TR.length * 100) : null, day: +day.reduce((a, t) => a + t.pnl, 0).toFixed(4), dayN: day.length,
        runner: +TR.filter(t => t.runner).reduce((a, t) => a + t.pnl, 0).toFixed(4) },
      trades: TR.slice(-30).reverse() };
  }
  boot();
  function setScalp(on) {
    P.scalp = !!on; save();
    engineLog(on ? `⚡ Суперскальпінг увімкнено: тейки ${P.scalpStep}% на кожному мікро-русі, завжди більші за комісії` : "⚡ Суперскальпінг вимкнено: крок знову підбирається на історії", "up");
    if (B && !B.units.length) { B = null; saveB(); ST.waitUntil = 0; return on ? "Суперскальпінг увімкнено — підбираю монету" : "Вимкнено — підбираю монету й крок"; }
    if (B) { B.pendingRestart = true; saveB(); return "Увімкнеться, щойно закриються поточні юніти"; }
    ST.waitUntil = 0; return on ? "Суперскальпінг увімкнено" : "Вимкнено";
  }
  return { tradesSince: (t) => TR.filter(x => x.at >= t), setInvert, flow: () => FLOW.snapshot(), forceDir, pause, learn, tune, brief, changeCoin, straddleNow, enterNow, params: P, setOn, setParams, setScalp, view, closeAll, restart, sym: () => (B ? B.sym : null) };
}
module.exports = { create, DEFAULTS };
