// ПРОБІЙНА СІТКА — окремий бот: рівні кожні stepPct % без меж (сітка йде за ціною куди завгодно).
//  • Напрям = пробій/тренд на 5-хв свічках (EMA36 проти EMA96 з запасом band): вгору → лише лонги, вниз → лише шорти, без напряму → нових входів немає.
//  • Кожен перетин рівня (і за рухом, і на відкаті) відкриває «юніт» у бік пробою з мікро-тейком на наступному рівні.
//  • «Бігун»: один юніт без тейка зі стопом, що йде за найкращою ціною (trailSteps рівнів) — забирає весь трендовий рух.
//  • Стоп юніта — stopSteps рівнів назад; розворот напряму закриває все.
//  • Перед стартом: монета і крок вибираються перевіркою на історії (3 дні 5-хв свічок) за тими самими правилами.
"use strict";
const WebSocketNode = (() => { try { return require("ws"); } catch (e) { return null; } })();
const BT = require("./backtest");

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
  scalp: false,            // ⚡ super scalping: takes of scalpStep % on every micro move (the step is fixed, the coin is still chosen on history)
  scalpStep: 0.3,
};
const NUM = { scalpStep: [0.1, 2], investment: [1, 1e6], leverage: [1, 50], stepPct: [0.1, 10], maxUnits: [1, 30], stopSteps: [1, 20], trailSteps: [1, 20], band: [0, 5], feeX: [1, 30], btDays: [1, 3.4], minTurnoverM: [1, 10000], idleMin: [5, 1440] };
const BOOL = ["scalp", "autoCoin", "runner", "autoTune", "btNeedPlus"];

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
    pub.on("message", (raw) => { let j; try { j = JSON.parse(String(raw)); } catch (e) { return; } if (!j.topic || !j.topic.startsWith("tickers.") || !j.data) return; const d = j.data;
      if (d.lastPrice) ST.price = Number(d.lastPrice); if (d.bid1Price) ST.bid = Number(d.bid1Price); if (d.ask1Price) ST.ask = Number(d.ask1Price); ST.priceAt = now(); });
    pub.on("close", () => { ST.pubUp = false; clearInterval(pubPing); setTimeout(connectPub, 2000); });
    pub.on("error", () => {});
  }
  function subPub() { if (!pub || !ST.pubUp || !B || pubSym === B.sym) return; if (pubSym) pub.send(JSON.stringify({ op: "unsubscribe", args: [`tickers.${pubSym}`] })); pub.send(JSON.stringify({ op: "subscribe", args: [`tickers.${B.sym}`] })); pubSym = B.sym; }
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
  function onOrder(o) { const p = parse(o.orderLinkId); if (!p) return; const u = unit(p.k); if (u && u.tpOrd === o.orderLinkId && ["Cancelled", "Rejected", "Deactivated"].includes(o.orderStatus)) { u.tpOrd = null; saveB(); } }
  function finish(u, x, fee, how) {
    const pnl = (x - u.e) * u.qty * u.d - (u.fee || 0) - fee;
    B.units = B.units.filter(z => z !== u); B.profit = +((B.profit || 0) + pnl).toFixed(6); if (pnl > 0) B.wins = (B.wins || 0) + 1; else B.losses = (B.losses || 0) + 1;
    TR.push({ at: now(), sym: B.sym, d: u.d, runner: !!u.runner, e: +Number(u.e).toPrecision(8), x: +Number(x).toPrecision(8), pnl: +pnl.toFixed(5), how, min: Math.round((now() - (u.at || now())) / 60000) });
    if (TR.length > 3000) TR.splice(0, TR.length - 3000); lsSet("bg.trades", TR);
    if (u.tpOrd) cancel(u.tpOrd);
    saveB();
    engineLog(`${u.runner ? "🏃" : "⚡"} ${B.sym}: ${u.runner ? "бігун" : "юніт"} ${u.d > 0 ? "лонг" : "шорт"} ${fmt(u.e)} → ${fmt(x)} (${how}) ${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} USDT`, pnl >= 0 ? "up" : "down");
  }
  async function cancel(id) { try { await api("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, orderLinkId: id } }); } catch (e) { /* gone */ } }
  async function market(u, kind, side, reduce) {
    const id = lid(u.k, kind);
    if (kind === "o") u.openOrd = id; else u.closeOrd = id;
    await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: B.sym, side, orderType: "Market", qty: String(u.qty), orderLinkId: id, positionIdx: ix(u.d), reduceOnly: !!reduce, timeInForce: "IOC" } });
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
      cands = (r2.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.turnover24h) >= P.minTurnoverM * 1e6 && Number(x.lastPrice) > 0 && !busy.includes(x.symbol))
        .map(x => ({ sym: x.symbol, range: (Number(x.highPrice24h) - Number(x.lowPrice24h)) / Number(x.lastPrice) * 100, chg: Math.abs(Number(x.price24hPcnt || 0) * 100) }))
        .filter(x => x.range >= 3).sort((a, b) => (b.chg + b.range / 2) - (a.chg + a.range / 2)).slice(0, P.autoTune ? 6 : 1);   // movers: a breakout grid needs movement
    } else if (P.symbol) cands = [{ sym: P.symbol }];
    else throw new Error("вкажіть монету або увімкніть автовибір");
    const results = [];
    for (const cd of cands) {
      try {
        const s = await spec(cd.sym), fr = await feeOf(cd.sym), maker = fr.maker, taker = fr.taker;
        // every take must clearly beat the round trip (market entry + limit take): the step ≥ feeX × fees, and in scalping at least 2 × fees
        const minStep = P.scalp ? 2 * (taker + maker) : P.feeX * (taker + maker);
        const cs = await candles5m(cd.sym, P.autoTune ? Math.min(1000, Math.round(P.btDays * 288)) : 120);
        const price = cs.length ? cs[cs.length - 1].c : 0, lev = Math.min(P.leverage, Math.floor(Number(s.maxLev || 10)));
        const minNotional = Math.max(Number(s.minNotional || 5) * 1.05, Number(s.minQty || 0) * price);
        const units = Math.min(P.maxUnits, Math.floor(P.investment * lev / minNotional) - 1);
        if (units < 1) { results.push({ sym: cd.sym, err: `замало коштів (мін. ордер ${minNotional.toFixed(2)} USDT)` }); continue; }
        const steps = P.scalp ? [+Math.max(P.scalpStep, minStep).toFixed(3)] : P.autoTune ? [...new Set([0.3, 0.5, 0.8, 1.2, P.stepPct].map(x => Math.max(x, minStep)).map(x => +x.toFixed(2)))] : [Math.max(P.stepPct, minStep)];
        let best = null;
        for (const st of steps) {
          const res = cs.length > 120 && P.autoTune ? BT.simulateB2(cs, { stepPct: st, maxUnits: units, investment: P.investment, lev, maker, taker, stopSteps: P.stopSteps, runner: P.runner, trailSteps: P.trailSteps, band: P.band / 100 }) : { roi: 0, wins: 0, losses: 0, flips: 0 };
          if (!best || res.roi > best.roi) best = Object.assign({ st }, res);
        }
        results.push({ sym: cd.sym, fee: +(taker + maker).toFixed(4), step: best.st, units, roi: best.roi, wins: best.wins, losses: best.losses, flips: best.flips, dir: dirOf(cs), tested: cs.length });
      } catch (e) { results.push({ sym: cd.sym, err: e.message }); }
    }
    results.sort((a, b) => (b.roi ?? -1e9) - (a.roi ?? -1e9));
    ST.bt = { at: now(), days: P.btDays, results };
    const best = results.find(x => x.roi != null);
    if (!best) throw new Error(results.map(x => `${x.sym}: ${x.err}`).join("; ") || "немає кандидатів");
    if (!opt.force && P.autoTune && P.btNeedPlus && best.roi <= 0) throw Object.assign(new Error(`жодна монета не заробила б пробійною сіткою за ${P.btDays} дні (найкраща ${best.sym} ${best.roi}%) — чекаю 15 хв`), { wait: 15 });
    return best;
  }
  async function start(opt = {}) {
    const pl = await plan(opt);
    const s = await spec(pl.sym);
    ST.spec = { tickStr: s.tick, step: s.step, minQty: Number(s.minQty || 0), minNotional: Number(s.minNotional || 5), maxLev: Number(s.maxLev || 10) };
    try { ST.hedge = ((await api("/v5/position/list", { query: { category: "linear", symbol: pl.sym }, auth: true })).list || []).some(x => Number(x.positionIdx) > 0); } catch (e) { ST.hedge = false; }
    await ticker(pl.sym);
    const lev = Math.min(P.leverage, Math.floor(ST.spec.maxLev));
    try { await api("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: pl.sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); } catch (e) { /* not modified */ }
    const qty = Math.max(qa(P.investment * lev / (pl.units + 1) / ST.price, false), qa(Math.max(ST.spec.minNotional * 1.05, ST.spec.minQty * ST.price) / ST.price, true));
    B = { scalp: P.scalp, fee: pl.fee, id: now().toString(36).slice(-5), sym: pl.sym, stepPct: pl.step, maxUnits: pl.units, qty, lev, anchor: ST.price, last: 0, d: 0, units: [], nextK: 1, profit: 0, wins: 0, losses: 0, startedAt: now(), idleAt: now(), bt: { roi: pl.roi, wins: pl.wins, losses: pl.losses, flips: pl.flips, days: P.btDays, tested: pl.tested } };
    B.last = idxOf(ST.price);
    saveB(); subPub();
    engineLog(`🚀 ${P.scalp ? "⚡ Суперскальпінг" : "Пробійна сітка"} ${pl.sym}: ${P.scalp ? `тейки ${pl.step}% (комісії за коло ${pl.fee}% → чистими ≈ +${(pl.step - pl.fee).toFixed(3)}% з тейка)` : `крок ${pl.step}%`}, до ${pl.units} юнітів по ${qty} + бігун, x${lev}${pl.tested ? ` · на історії за ${P.btDays} дні: ${pl.roi >= 0 ? "+" : ""}${pl.roi}%, тейків ${pl.wins}, стопів ${pl.losses}` : ""}`, "up");
    await updateDir(true);
  }
  async function updateDir(force) {
    if (!B || (!force && now() - ST.dirAt < 60000)) return;
    ST.dirAt = now();
    try {
      const dd = dirOf(await candles5m(B.sym, 200));
      B.dirInfo = dd;
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
    try { await market(u, "s", u.d > 0 ? "Sell" : "Buy", true); } catch (e) { u.closing = false; if (e.code === 110017) finish(u, ST.price, 0, why + " (позиції вже немає)"); else setErr("закриття юніта: " + e.message); }
    saveB();
  }

  async function openAt(j, way, why) {
    const out = [];
    const microN = B.units.filter(u => !u.runner).length;
    if (microN < B.maxUnits && !B.units.some(u => !u.runner && u.j === j)) {
      const u = { k: B.nextK++, j, d: B.d, qty: B.qty, state: "opening", tp: L(j + B.d), sl: L(j - B.d * P.stopSteps), way };
      B.units.push(u); saveB();
      try { await market(u, "o", u.d > 0 ? "Buy" : "Sell", false); out.push("юніт"); engineLog(`⚡ ${B.sym}: ${why || (way === B.d ? "рух" : "відкат")} через ${fmt(L(j))} — ${u.d > 0 ? "лонг" : "шорт"} ${u.qty}, тейк ${fmt(u.tp)}`, "mute"); }
      catch (e) { B.units = B.units.filter(z => z !== u); saveB(); setErr("вхід юніта: " + e.message); }
    }
    if (P.runner && way === B.d && !B.units.some(u => u.runner)) {
      const u = { k: B.nextK++, j, d: B.d, qty: B.qty, state: "opening", runner: true, sl: L(j - B.d * P.trailSteps), tp: null };
      B.units.push(u); saveB();
      try { await market(u, "o", u.d > 0 ? "Buy" : "Sell", false); out.push("бігун"); engineLog(`🏃 ${B.sym}: бігун ${u.d > 0 ? "лонг" : "шорт"} — трейлінг-стоп ${P.trailSteps} рівні за найкращою ціною`, "up"); }
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
      B.d = dir; B.manual = true; B.last = idxOf(ST.price); B.idleAt = now(); saveB();
      const opened = await openAt(B.last, dir, "ручний вхід");
      engineLog(`👆 ${B.sym}: ручний вхід ${dir > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"} зараз (${opened.join(" + ") || "уже відкрито"}) — далі сітка веде сама в цей бік, доки тренд явно не розвернеться`, dir > 0 ? "up" : "down");
      return `${B.sym}: ${dir > 0 ? "лонг" : "шорт"} відкрито (${opened.join(" + ") || "юніти вже є"})`;
    } catch (e) { return "Помилка: " + e.message; }
    finally { ST.busy = false; }
  }
  // ---------------- the loop: every 500 ms ----------------
  async function loop() {
    if (ST.busy) return; ST.busy = true;
    try {
      if (!keys || !keys.key) return;
      if (!B) { if (P.on && now() >= ST.waitUntil) { try { await start(); } catch (e) { setErr(e.message); ST.waitUntil = now() + (e.wait || 1) * 60000; lsSet("bg.wait", ST.waitUntil); if (e.wait) engineLog("⏸ " + e.message, "gold-t"); } } return; }
      if (!ST.spec) { const s = await spec(B.sym); ST.spec = { tickStr: s.tick, step: s.step, minQty: Number(s.minQty || 0), minNotional: Number(s.minNotional || 5), maxLev: Number(s.maxLev || 10) }; }
      if (!ST.price || now() - ST.priceAt > 10000) await ticker(B.sym);
      subPub();
      await updateDir(false);
      const x = ST.price;
      if (B.resync) { B.resync = false; B.last = idxOf(x); saveB(); }   // after a restart: continue from the price now (no catch-up of missed levels)
      // stops and the runner's trailing stop
      for (const u of B.units.slice()) {
        if (u.state !== "open" || u.closing) continue;
        if (u.runner) {
          u.pk = u.d > 0 ? Math.max(u.pk || u.e, x) : Math.min(u.pk || u.e, x);
          const t = u.d > 0 ? u.pk / Math.pow(r(), P.trailSteps) : u.pk * Math.pow(r(), P.trailSteps);
          u.sl = u.d > 0 ? Math.max(u.sl, t) : Math.min(u.sl, t);
        }
        if ((u.d > 0 && x <= u.sl) || (u.d < 0 && x >= u.sl)) await closeUnit(u, u.runner ? "трейлінг бігуна" : "стоп");
      }
      // takes of open units
      for (const u of B.units) {
        if (u.state !== "open" || u.runner || u.tpOrd || u.closing) continue;
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
        if (P.on && B.d !== 0) for (const [j, way] of crossed) await openAt(j, way);
      }
      if (B.pendingRestart && !B.units.length) { B = null; saveB(); ST.waitUntil = 0; return; }   // the scalping switch waits for the units to close
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
      const owned = new Set(B.units.map(u => u.tpOrd).filter(Boolean));
      for (const id of live) if (/^bg/.test(id) && !owned.has(id)) await cancel(id);
      const pos = ((await api("/v5/position/list", { query: { category: "linear", symbol: B.sym }, auth: true })).list || []).reduce((s2, z) => s2 + Number(z.size || 0) * (z.side === "Buy" ? 1 : z.side === "Sell" ? -1 : 0), 0);
      const mine = B.units.filter(u => u.state === "open").reduce((s2, u) => s2 + u.qty * u.d, 0);
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
  async function closeAll() { if (!B) return "Немає пробійної сітки"; for (const u of B.units.slice()) { if (u.state === "open") await closeUnit(u, "закрито власником"); } const sym = B.sym; setTimeout(() => { if (B && !B.units.length) { B = null; saveB(); } }, 6000); return `Юніти ${sym} закриваються ринком`; }
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
    const b = B && { manual: !!B.manual, scalp: !!B.scalp, fee: B.fee, pending: !!B.pendingRestart, sym: B.sym, stepPct: B.stepPct, qty: B.qty, lev: B.lev, maxUnits: B.maxUnits, d: B.d, dir: B.dirInfo || null, profit: +(B.profit || 0).toFixed(4), wins: B.wins || 0, losses: B.losses || 0, bt: B.bt, startedAt: B.startedAt,
      levels: [-3, -2, -1, 0, 1, 2, 3].map(k => +fmt(L(B.last + k + (k > 0 ? 0 : 0)))),
      units: B.units.map(u => ({ d: u.d, runner: !!u.runner, state: u.state, e: u.e || null, tp: u.tp ? +fmt(u.tp) : null, sl: u.sl ? +fmt(u.sl) : null, pnl: u.e && x ? +((x - u.e) * u.qty * u.d).toFixed(4) : null, way: u.way })),
      unreal: +B.units.filter(u => u.state === "open").reduce((s2, u) => s2 + (x - u.e) * u.qty * u.d, 0).toFixed(4) };
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
  return { enterNow, params: P, setOn, setParams, setScalp, view, closeAll, restart, sym: () => (B ? B.sym : null) };
}
module.exports = { create, DEFAULTS };
