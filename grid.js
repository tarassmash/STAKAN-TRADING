// GRID — простий ф'ючерсний грід-бот, як на Bybit, але з трейлінгом і автопідстановкою сіток.
//  • Сітка: діапазон [низ, верх] навколо ціни, N рівнів (геометрично — однаковий % між рівнями). Крок ≥ feeX × комісія за коло.
//  • Режими: long (купує на рівнях нижче, продає на рівень вище), short (дзеркально), neutral (нижче ціни — лонг-комірки, вище — шорт-комірки).
//  • Кожна комірка: ордер на відкриття → виконався → одразу ордер на закриття на сусідньому рівні → виконався → прибуток і знову ордер на відкриття.
//  • Трейлінг: ціна вийшла за верх/низ сітки — найдальша незадіяна комірка переїжджає на інший край, сітка йде за ціною.
//  • Стоп сітки: ціна пішла за межу на slPct — позиція закривається, ордери знімаються; з autoRestart через restartMin хв нова сітка навколо ціни.
"use strict";
const WebSocketNode = (() => { try { return require("ws"); } catch (e) { return null; } })();

const DEFAULTS = {
  on: false,
  autoCoin: true,          // the bot picks a coin that swings a lot but goes nowhere (best for a grid)
  symbol: "",              // the coin when autoCoin is off
  mode: "neutral",         // "long" | "short" | "neutral"
  investment: 20,          // USDT margin for the whole grid
  leverage: 3,
  grids: 20,               // number of grid cells
  autoRange: true,         // range from the coin's 24 h swing
  rangePct: 8,             // total width of the range, % (when autoRange is off and no manual low/high)
  low: 0, high: 0,         // manual range (both > 0 -> used)
  feeX: 10,                // the step between levels is at least feeX × the round-trip fee
  trailing: true,          // the grid follows the price out of its range
  slPct: 3,                // stop of the whole grid: this % beyond its edge
  autoRestart: true,       // after the stop (or when the coin goes dead) a new grid around the price
  restartMin: 2,
  minTurnoverM: 50,
};
const NUM = { investment: [1, 1e6], leverage: [1, 50], grids: [2, 200], rangePct: [0.5, 100], low: [0, 1e9], high: [0, 1e9], feeX: [1, 50], slPct: [0.2, 50], restartMin: [0, 1440], minTurnoverM: [1, 10000] };
const BOOL = ["autoCoin", "autoRange", "trailing", "autoRestart"];

function create(host) {
  const { bx, spec, align, sign, log, engineLog, lsGet, lsSet, env, keys } = host;
  const WS = host.WebSocket || WebSocketNode;
  const MAKER = host.makerFee != null ? host.makerFee : 0.02;
  const PUB = host.testWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/public/linear" : "wss://stream.bybit.com/v5/public/linear");
  const PRIV = host.testPrivWs || (env === "testnet" ? "wss://stream-testnet.bybit.com/v5/private" : "wss://stream.bybit.com/v5/private");
  const now = () => Date.now();

  const P = Object.assign({}, DEFAULTS, lsGet("grid.p", {}));
  const save = () => lsSet("grid.p", P);
  let G = lsGet("grid.g", null);          // the live grid
  const saveG = () => { lsSet("grid.g", G); if (host.flush) host.flush(); };   // written to disk at once: a crash must not forget an order
  const TR = lsGet("grid.trades", []);    // completed cells + grid stops
  const ST = { err: "", errAt: 0, pubUp: false, privUp: false, price: 0, bid: 0, ask: 0, hedge: null, spec: null, busy: false, seenExec: new Set(), lastExecPoll: 0, restartAt: lsGet("grid.restartAt", 0), cool: lsGet("grid.cool", {}), lastTrail: 0, ex: {} };
  const setErr = (t) => { ST.err = t; ST.errAt = now(); log("grid", t); };
  // Bybit allows ~10 order requests per second per symbol; everything private goes through one queue (≤ 7/s), a 10006 pauses the queue
  let qTail = Promise.resolve(), qLast = 0;
  function api(path, opts) {
    if (!opts || !opts.auth) return bx(path, opts);
    const run = async () => {
      const wait = Math.max(0, qLast + 145 - now(), (ST.pauseUntil || 0) - now());
      if (wait) await new Promise(r => setTimeout(r, wait));
      qLast = now();
      try { return await bx(path, opts); }
      catch (e) {
        if (e.code === 10006 || /rate limit|too many visits/i.test(e.message)) {
          ST.pauseUntil = now() + 3000; ST.rl = (ST.rl || 0) + 1;
          if (now() - (ST.rlLog || 0) > 60000) { ST.rlLog = now(); engineLog("⏳ Bybit: забагато запитів — пригальмовую на 3 с і повторюю", "mute"); }
          await new Promise(r => setTimeout(r, 3000)); qLast = now();
          return bx(path, opts);
        }
        throw e;
      }
    };
    const p2 = qTail.then(run, run);
    qTail = p2.catch(() => {});
    return p2;
  }

  const px = (v, up) => align(v, ST.spec.tickStr, up);
  const qa = (v, up) => Number(align(v, ST.spec.step, up));
  const rtFee = () => 2 * Math.max(0.01, ST.spec && ST.spec.maker != null ? ST.spec.maker : MAKER);
  const lid = (cell, kind) => ("gd" + G.id + "." + cell + kind + (now() % 1e7).toString(36)).slice(0, 36);
  const parseLid = (id) => { const m = /^gd([a-z0-9]+)\.(\d+)([oc])/.exec(String(id || "")); return m && G && m[1] === G.id ? { k: Number(m[2]), kind: m[3] } : null; };
  const cellOf = (k) => G && G.cells.find(c => c.k === k);

  // ---------------- market data ----------------
  let pub = null, pubPing = null, pubSym = "";
  function connectPub() {
    try { pub = new WS(PUB); } catch (e) { setTimeout(connectPub, 3000); return; }
    pub.on("open", () => { ST.pubUp = true; pubSym = ""; subPub(); clearInterval(pubPing); pubPing = setInterval(() => { try { pub.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    pub.on("message", (raw) => {
      let j; try { j = JSON.parse(String(raw)); } catch (e) { return; }
      if (!j.topic || !j.topic.startsWith("tickers.") || !j.data) return;
      const d = j.data;
      if (d.lastPrice) ST.price = Number(d.lastPrice); if (d.bid1Price) ST.bid = Number(d.bid1Price); if (d.ask1Price) ST.ask = Number(d.ask1Price);
      ST.priceAt = now();
    });
    pub.on("close", () => { ST.pubUp = false; clearInterval(pubPing); setTimeout(connectPub, 2000); });
    pub.on("error", () => {});
  }
  function subPub() {
    if (!pub || !ST.pubUp || !G || pubSym === G.sym) return;
    if (pubSym) pub.send(JSON.stringify({ op: "unsubscribe", args: [`tickers.${pubSym}`] }));
    pub.send(JSON.stringify({ op: "subscribe", args: [`tickers.${G.sym}`] })); pubSym = G.sym;
  }
  async function ticker(sym) {
    const r = await api("/v5/market/tickers", { query: { category: "linear", symbol: sym } });
    const x = (r.list || [])[0]; if (!x) throw new Error("немає " + sym);
    ST.price = Number(x.lastPrice); ST.bid = Number(x.bid1Price); ST.ask = Number(x.ask1Price); ST.priceAt = now();
    return x;
  }

  // ---------------- private: fills ----------------
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
      if (j.op === "auth") { if (j.success) { ST.privUp = true; priv.send(JSON.stringify({ op: "subscribe", args: ["execution", "order"] })); } else setErr("приватний WS: " + (j.ret_msg || "auth")); return; }
      if (j.topic === "execution") for (const x of j.data || []) onExec(x);
      if (j.topic === "order") for (const o of j.data || []) onOrder(o);
    });
    priv.on("close", () => { ST.privUp = false; clearInterval(privPing); setTimeout(connectPriv, 3000); });
    priv.on("error", () => {});
  }
  // executions are summed per order; the cell moves on when its order is fully filled
  function onExec(x) {
    if (!G || x.symbol !== G.sym || (x.execType && x.execType !== "Trade")) return;
    if (ST.seenExec.has(x.execId)) return; ST.seenExec.add(x.execId);
    if (ST.seenExec.size > 5000) ST.seenExec = new Set([...ST.seenExec].slice(-2000));
    const id = x.orderLinkId, e = ST.ex[id] = ST.ex[id] || { q: 0, v: 0, fee: 0 };
    e.q += Number(x.execQty); e.v += Number(x.execQty) * Number(x.execPrice); e.fee += Number(x.execFee || 0);
    G.fees = +((G.fees || 0) + Number(x.execFee || 0)).toFixed(6);
    const p = parseLid(id);
    if (p) { const c = cellOf(p.k); if (c && c.order === id && e.q >= c.qty - 1e-9) filled(c, p.kind, e.v / e.q, e.fee); }
    else if (String(id).startsWith("gi" + G.id)) initFilled(id, e);
  }
  function onOrder(o) {
    const p = parseLid(o.orderLinkId); if (!p || !G) return;
    const c = cellOf(p.k);
    if (c && c.order === o.orderLinkId && ["Cancelled", "Rejected", "Deactivated"].includes(o.orderStatus) && !c.moving) { c.order = null; c.fails = (c.fails || 0) + 1; c.retryAt = now() + Math.min(30000, 2000 * c.fails); saveG(); }   // e.g. PostOnly rejected -> re-placed next loop
  }

  // ---------------- grid ----------------
  // a cell: dir +1 long (open = buy at lo, close = sell at hi) / -1 short (open = sell at hi, close = buy at lo); state "open" (waiting to open) / "close" (holding)
  function cellOrder(c) {
    const long = c.dir > 0;
    if (c.state === "open") return { side: long ? "Buy" : "Sell", price: long ? c.lo : c.hi, kind: "o", reduce: false };
    return { side: long ? "Sell" : "Buy", price: long ? c.hi : c.lo, kind: "c", reduce: true };
  }
  function idx(dir) { return ST.hedge ? (dir > 0 ? 1 : 2) : 0; }
  async function placeCell(c) {
    if ((c.retryAt || 0) > now()) return false;
    const o = cellOrder(c);
    // PostOnly must not cross the book. An opening order beyond the price waits; a closing order beyond the price is already in profit -> close at the touch
    if (o.kind === "o" && ((o.side === "Buy" && ST.ask && o.price >= ST.ask) || (o.side === "Sell" && ST.bid && o.price <= ST.bid))) return false;
    if (o.kind === "c" && o.side === "Sell" && ST.bid && o.price <= ST.bid) o.price = Number(px(ST.ask, true));
    if (o.kind === "c" && o.side === "Buy" && ST.ask && o.price >= ST.ask) o.price = Number(px(ST.bid, false));
    const id = lid(c.k, o.kind);
    c.order = id; c.orderAt = now();
    try {
      await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: G.sym, side: o.side, orderType: "Limit", qty: String(c.qty), price: String(o.price),
        timeInForce: "PostOnly", orderLinkId: id, positionIdx: idx(c.dir), reduceOnly: ST.hedge || P.mode !== "neutral" ? o.reduce : false } });
      c.fails = 0; saveG(); return true;
    } catch (e) {
      c.order = null; c.fails = (c.fails || 0) + 1; c.retryAt = now() + Math.min(60000, 1000 * 2 ** Math.min(6, c.fails));   // a failing cell retries ever more slowly
      if (e.code === 10001) { ST.hedge = !ST.hedge; c.retryAt = 0; return true; }
      if (e.code === 110017 || e.code === 110025) { c.state = "open"; return true; }     // nothing to reduce: the holding is gone (stop) -> wait to open again
      if ([110007, 110004, 110012].includes(e.code)) { if (now() - (ST.nmAt || 0) > 60000) { ST.nmAt = now(); engineLog("💰 мало вільних коштів для ордера сітки — зменште суму або кількість сіток", "down"); } return true; }
      setErr(`ордер сітки: ${e.message}`); return true;
    }
  }
  async function cancelOrder(id) {
    try { await api("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: G.sym, orderLinkId: id } }); } catch (e) { /* gone */ }
  }
  function filled(c, kind, avg, fee) {
    c.order = null;
    if (kind === "o") { c.state = "close"; c.openPx = avg; c.openFee = fee; c.openAt = now(); }
    else {
      const profit = (avg - c.openPx) * c.qty * c.dir - (c.openFee || 0) - fee;
      G.profit = +((G.profit || 0) + profit).toFixed(6); G.cycles = (G.cycles || 0) + 1;
      TR.push({ at: now(), sym: G.sym, kind: "cell", dir: c.dir, open: c.openPx, close: avg, qty: c.qty, pnl: +profit.toFixed(5), min: Math.round((now() - (c.openAt || now())) / 60000) });
      if (TR.length > 3000) TR.splice(0, TR.length - 3000); lsSet("grid.trades", TR);
      c.state = "open"; c.openPx = null; c.openFee = 0;
      engineLog(`✅ ${G.sym}: сітка ${c.dir > 0 ? "купила" : "продала"} ${fmt(c.dir > 0 ? c.lo : c.hi)} → ${c.dir > 0 ? "продала" : "купила"} ${fmt(c.dir > 0 ? c.hi : c.lo)} · ${profit >= 0 ? "+" : ""}${profit.toFixed(4)} USDT`, profit >= 0 ? "up" : "down");
    }
    saveG();
  }
  const fmt = (v) => Number(v).toPrecision(6);

  // the coin for a grid: swings a lot within the day (range) but its 24 h change is small; enough turnover
  async function pickCoin() {
    const r = await api("/v5/market/tickers", { query: { category: "linear" } });
    const rows = (r.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.turnover24h) >= P.minTurnoverM * 1e6 && Number(x.lastPrice) > 0 && !((ST.cool[x.symbol] || 0) > now()))
      .map(x => { const range = (Number(x.highPrice24h) - Number(x.lowPrice24h)) / Number(x.lastPrice) * 100, chg = Math.abs(Number(x.price24hPcnt || 0) * 100);
        return { sym: x.symbol, range, chg, score: range / (chg + 2) * Math.log10(Number(x.turnover24h)) }; })
      .filter(x => x.range >= 3 && x.range <= 40).sort((a, b) => b.score - a.score);
    ST.picks = rows.slice(0, 8).map(x => ({ sym: x.sym, range: +x.range.toFixed(1), chg: +x.chg.toFixed(1) }));
    if (!rows.length) throw new Error("немає монети для сітки (усі занадто спокійні або трендові)");
    return rows[0];
  }
  async function loadSpec(sym) {
    const s = await spec(sym);
    ST.spec = { tickStr: s.tick, tick: Number(s.tick), step: s.step, minQty: Number(s.minQty || 0), minNotional: Number(s.minNotional || 5), maxLev: Number(s.maxLev || 10) };
    try { const fr = ((await api("/v5/account/fee-rate", { query: { category: "linear", symbol: sym }, auth: true })).list || [])[0]; if (fr) ST.spec.maker = Number(fr.makerFeeRate) * 100; } catch (e) { /* default */ }
    try { const r = await api("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true }); ST.hedge = (r.list || []).some(x => Number(x.positionIdx) > 0); } catch (e) { ST.hedge = false; }
  }
  async function build() {
    let sym = P.symbol, info = null;
    if (P.autoCoin) { info = await pickCoin(); sym = info.sym; }
    if (!sym) throw new Error("вкажіть монету або увімкніть автовибір");
    await loadSpec(sym);
    const t = await ticker(sym), price = Number(t.lastPrice);
    // range
    let low, high;
    if (!P.autoRange && P.low > 0 && P.high > P.low) { low = P.low; high = P.high; }
    else {
      const swing = (Number(t.highPrice24h) - Number(t.lowPrice24h)) / price * 100;
      const w = P.autoRange ? Math.max(3, Math.min(30, swing * 0.7)) : P.rangePct;
      low = price * (1 - w / 200); high = price * (1 + w / 200);
    }
    // cells: the step must pay feeX round-trip fees; the order must fit the exchange minimum
    let n = Math.round(P.grids);
    const minStep = P.feeX * rtFee();
    const maxByStep = Math.floor(Math.log(high / low) / Math.log(1 + minStep / 100));
    if (n > maxByStep) n = Math.max(2, maxByStep);
    const lev = Math.min(P.leverage, Math.floor(ST.spec.maxLev));
    const minNotional = Math.max(ST.spec.minNotional * 1.05, ST.spec.minQty * price);
    const maxByMoney = Math.floor(P.investment * lev / minNotional);
    if (n > maxByMoney) n = maxByMoney;
    if (n < 2) throw new Error(`замало коштів: на ${sym} мінімальний ордер ${minNotional.toFixed(2)} USDT, а ${P.investment} USDT × x${lev} вистачає на ${maxByMoney} — збільште суму або плече`);
    const r = Math.pow(high / low, 1 / n), L = Array.from({ length: n + 1 }, (_, i) => Number(px(low * Math.pow(r, i), false)));
    const qty = Math.max(qa(P.investment * lev / n / price, false), qa(minNotional / low, true));
    try { await api("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); }
    catch (e) { if (e.code !== 110043) log("leverage", e.message); }
    const id = now().toString(36).slice(-5);
    G = { id, sym, mode: P.mode, low: L[0], high: L[n], levels: L, stepPct: +((r - 1) * 100).toFixed(3), qty, lev, startedAt: now(), startPrice: price, profit: 0, cycles: 0, fees: 0, trails: 0, cells: [] };
    // cells: long below the price (and above it in a long grid, bought at start), short above (and below in a short grid, sold at start)
    let initBuy = 0, initSell = 0;
    for (let i = 0; i < n; i++) {
      const lo = L[i], hi = L[i + 1], below = hi <= price, above = lo >= price;
      let dir, state = "open";
      if (P.mode === "long") { dir = 1; if (!below) { state = "close"; initBuy += qty; } }
      else if (P.mode === "short") { dir = -1; if (!above) { state = "close"; initSell += qty; } }
      else { dir = below || (!above && price - lo > hi - price) ? 1 : -1; }   // neutral: no position at the start
      G.cells.push({ k: i, i, lo, hi, dir, state, qty, order: null, openPx: state === "close" ? price : null, openFee: 0 });
    }
    saveG(); subPub();
    engineLog(`🧱 Нова сітка ${sym}: ${P.mode === "long" ? "лонг" : P.mode === "short" ? "шорт" : "нейтральна"}, ${fmt(L[0])} – ${fmt(L[n])}, ${n} сіток по ${G.stepPct}% (≥ ${P.feeX}× комісії), ${qty} на сітку, x${lev}`
      + (info ? ` · монета за 24 год ходить ${info.range.toFixed(1)}%, змінилась на ${info.chg.toFixed(1)}%` : ""), "up");
    // a long grid buys the cells above the price at once (market), a short grid sells those below
    const init = initBuy || initSell;
    if (init) {
      try {
        await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: sym, side: initBuy ? "Buy" : "Sell", orderType: "Market", qty: String(qa(init, false)), orderLinkId: ("gi" + id + "." + (now() % 1e6).toString(36)).slice(0, 36), positionIdx: idx(initBuy ? 1 : -1), timeInForce: "IOC" } });
      } catch (e) { setErr("стартова позиція сітки: " + e.message); }
    }
  }
  function initFilled(id, e) {
    if (e.done) return; e.done = true;
    for (const c of G.cells) if (c.state === "close" && c.openPx === G.startPrice) { c.openPx = e.v / e.q; c.openFee = e.fee * c.qty / e.q; }
    saveG();
  }

  // trailing: the price left the grid -> the farthest unused cell moves to the other edge (the grid follows the price)
  async function trail() {
    if (!P.trailing || !G || now() - ST.lastTrail < 1000) return;
    const p = ST.price, r = 1 + G.stepPct / 100, n = G.cells.length;
    let moved = null;
    if (p > G.high * r) {
      // the lowest cell that holds nothing (long waiting to buy, or short waiting to sell) moves to the top
      const c = G.cells.filter(c => c.state === "open").sort((a, b) => a.lo - b.lo)[0];
      if (c && c.lo < G.low * r * r) {
        const lo = G.high, hi = Number(px(G.high * r, true));
        moved = await move(c, lo, hi, P.mode === "long" ? 1 : -1, "вгору");
      }
    } else if (p < G.low / r) {
      const c = G.cells.filter(c => c.state === "open").sort((a, b) => b.hi - a.hi)[0];
      if (c && c.hi > G.high / r / r) {
        const hi = G.low, lo = Number(px(G.low / r, false));
        moved = await move(c, lo, hi, P.mode === "short" ? -1 : 1, "вниз");
      }
    }
    if (moved) { ST.lastTrail = now(); G.cells.sort((a, b) => a.lo - b.lo).forEach((c, k) => c.i = k); G.low = G.cells[0].lo; G.high = G.cells[n - 1].hi; G.levels = G.cells.map(c => c.lo).concat([G.high]); G.trails = (G.trails || 0) + 1; saveG(); }
  }
  async function move(c, lo, hi, dir, where) {
    c.moving = true;
    if (c.order) { await cancelOrder(c.order); c.order = null; }
    c.lo = lo; c.hi = hi; c.dir = dir; c.state = "open"; c.openPx = null; c.openFee = 0; c.moving = false;
    // a long grid trailing up needs the coin for the new top cell: bought at market (like Bybit's trailing up); short grid trailing down: sold
    if ((P.mode === "long" && where === "вгору") || (P.mode === "short" && where === "вниз")) {
      try {
        await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: G.sym, side: dir > 0 ? "Buy" : "Sell", orderType: "Market", qty: String(c.qty), orderLinkId: ("gi" + G.id + ".t" + (now() % 1e6).toString(36)).slice(0, 36), positionIdx: idx(dir), timeInForce: "IOC" } });
        c.state = "close"; c.openPx = ST.price; c.openFee = c.qty * ST.price * 0.00055;
      } catch (e) { setErr("трейлінг: " + e.message); }
    }
    engineLog(`↕ ${G.sym}: ціна вийшла за сітку — сітка йде ${where}: нова сітка ${fmt(lo)} – ${fmt(hi)}`, "mute");
    return true;
  }

  // the whole grid's stop: the price went slPct beyond an edge (where the grid holds a position)
  async function checkStop() {
    if (!G || !ST.price) return;
    const p = ST.price, longs = G.cells.some(c => c.state === "close" && c.dir > 0), shorts = G.cells.some(c => c.state === "close" && c.dir < 0);
    const down = longs && p <= G.low * (1 - P.slPct / 100), up = shorts && p >= G.high * (1 + P.slPct / 100);
    if (down || up) await closeGrid(`стоп: ціна ${down ? "нижче" : "вище"} сітки на ${P.slPct}%`, true);
  }
  async function closeGrid(why, restart) {
    if (!G) return;
    const g = G; G = null; saveG();
    for (const c of g.cells) if (c.order) await cancelOrderSym(g.sym, c.order);
    // close the grid's position at market (the net of its holdings)
    const net = g.cells.filter(c => c.state === "close").reduce((s, c) => s + c.qty * c.dir, 0);
    let pnl = 0;
    if (Math.abs(net) > 1e-12) {
      try {
        const hedge = ST.hedge;
        if (hedge) {
          const lq = g.cells.filter(c => c.state === "close" && c.dir > 0).reduce((s, c) => s + c.qty, 0), sq = g.cells.filter(c => c.state === "close" && c.dir < 0).reduce((s, c) => s + c.qty, 0);
          if (lq) await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: g.sym, side: "Sell", orderType: "Market", qty: String(qa(lq, false)), reduceOnly: true, positionIdx: 1, timeInForce: "IOC" } });
          if (sq) await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: g.sym, side: "Buy", orderType: "Market", qty: String(qa(sq, false)), reduceOnly: true, positionIdx: 2, timeInForce: "IOC" } });
        } else await api("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: g.sym, side: net > 0 ? "Sell" : "Buy", orderType: "Market", qty: String(qa(Math.abs(net), false)), reduceOnly: true, positionIdx: 0, timeInForce: "IOC" } });
      } catch (e) { setErr("закриття сітки: " + e.message); }
      for (const c of g.cells) if (c.state === "close") pnl += (ST.price - c.openPx) * c.qty * c.dir - (c.openFee || 0) - c.qty * ST.price * 0.00055;
    }
    TR.push({ at: now(), sym: g.sym, kind: "grid", why, pnl: +pnl.toFixed(5), gridProfit: g.profit, cycles: g.cycles, min: Math.round((now() - g.startedAt) / 60000) });
    lsSet("grid.trades", TR);
    engineLog(`🛑 ${g.sym}: сітку закрито (${why}) · сітка заробила ${g.profit >= 0 ? "+" : ""}${(g.profit || 0).toFixed(4)} за ${g.cycles} кіл · закриття позиції ${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} USDT`, pnl + (g.profit || 0) >= 0 ? "up" : "down");
    if (why.startsWith("стоп")) { ST.cool[g.sym] = now() + 3 * 3600e3; lsSet("grid.cool", ST.cool); }
    if (restart && P.autoRestart && P.on) { ST.restartAt = now() + P.restartMin * 60000; lsSet("grid.restartAt", ST.restartAt); engineLog(`🔁 Нова сітка через ${P.restartMin} хв`, "mute"); }
  }
  async function cancelOrderSym(sym, id) { try { await api("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: sym, orderLinkId: id } }); } catch (e) { /* gone */ } }

  // ---------------- the loop ----------------
  async function loop() {
    if (ST.busy) return; ST.busy = true;
    try {
      if (!keys || !keys.key) return;
      if (!G) {
        if (P.on && now() >= (ST.restartAt || 0)) { try { await build(); } catch (e) { setErr(e.message); ST.restartAt = now() + 60000; } }
        return;
      }
      if (!ST.spec) await loadSpec(G.sym);
      if (!ST.price || now() - (ST.priceAt || 0) > 10000) await ticker(G.sym);
      subPub();
      if (!P.on) return;
      await checkStop(); if (!G) return;
      for (let k = 0; k < 3 && G; k++) { const before = G.trails || 0; ST.lastTrail = 0; await trail(); if ((G.trails || 0) === before) break; }   // a fast move: up to 3 cells per second
      // every cell has its order on the exchange
      let sent = 0;
      for (const c of G.cells.slice().sort((a, b) => Math.abs((a.lo + a.hi) / 2 - ST.price) - Math.abs((b.lo + b.hi) / 2 - ST.price))) {   // the cells nearest the price first
        if (sent >= 5 || !G) break;
        if (!c.order && await placeCell(c)) sent++;
      }
    } catch (e) { setErr(e.message); }
    finally { ST.busy = false; }
  }
  setInterval(loop, 1000);

  // reconcile: orders that vanished without a message (missed while restarting) -> history says filled or cancelled
  async function reconcile() {
    if (!G || !keys || !keys.key) return;
    try {
      if (!ST.privUp) {
        const r = await api("/v5/execution/list", { query: { category: "linear", symbol: G.sym, limit: "100", startTime: String(Math.max(ST.lastExecPoll || now() - 60000, now() - 600000)) }, auth: true });
        ST.lastExecPoll = now() - 2000;
        for (const x of (r.list || []).slice().reverse()) onExec(Object.assign({ symbol: G.sym }, x));
      }
      const live = new Set(((await api("/v5/order/realtime", { query: { category: "linear", symbol: G.sym, limit: "50" }, auth: true })).list || []).map(o => o.orderLinkId));
      let looks = 0;
      for (const c of G.cells) {
        if (!c.order || live.has(c.order) || now() - (c.orderAt || 0) < 5000) continue;
        if (++looks > 4) break;
        const id = c.order;
        try {
          const h = ((await api("/v5/order/history", { query: { category: "linear", symbol: G.sym, orderLinkId: id }, auth: true })).list || [])[0];
          if (h && h.orderStatus === "Filled") { const p = parseLid(id); if (p && c.order === id) filled(c, p.kind, Number(h.avgPrice) || (p.kind === "o" ? cellOrder(c).price : cellOrder(c).price), Number(h.cumExecFee || 0)); }
          else if (c.order === id) c.order = null;
        } catch (e) { /* next time */ }
      }
      // grid orders that no cell owns (a previous grid, or one placed just before a crash) are removed
      const owned = new Set(G.cells.map(c => c.order).filter(Boolean));
      for (const id of live) if (/^gd/.test(id) && !owned.has(id)) await cancelOrder(id);
      // the grid's holding must match the exchange position; a fill missed during a crash is caught here
      await syncPosition();
    } catch (e) { setErr("звірка: " + e.message); }
  }
  setInterval(reconcile, 5000);
  async function syncPosition() {
    const r = await api("/v5/position/list", { query: { category: "linear", symbol: G.sym }, auth: true });
    const ex = (r.list || []).reduce((s2, x) => s2 + Number(x.size || 0) * (x.side === "Buy" ? 1 : x.side === "Sell" ? -1 : 0), 0);
    const mine = G.cells.filter(c => c.state === "close").reduce((s2, c) => s2 + c.qty * c.dir, 0);
    const diff = ex - mine;
    if (Math.abs(diff) < G.qty * 0.5) { ST.mis = 0; return; }
    if (++ST.mis < 3) return;                                    // 3 checks in a row (15 s): not an in-flight fill
    ST.mis = 0;
    // the exchange holds more in some direction: the nearest cells of that direction are marked as holding; less: holding cells are released
    let left = Math.round(Math.abs(diff) / G.qty);
    const p = ST.price;
    const want = diff > 0 ? 1 : -1;
    const add = G.cells.filter(c => c.state === "open" && c.dir === want).sort((a, b) => Math.abs(a.lo - p) - Math.abs(b.lo - p));
    const rel = G.cells.filter(c => c.state === "close" && c.dir === -want).sort((a, b) => Math.abs(a.lo - p) - Math.abs(b.lo - p));
    for (const c of rel) { if (!left) break; if (c.order) { await cancelOrder(c.order); c.order = null; } c.state = "open"; c.openPx = null; left--; }
    for (const c of add) { if (!left) break; if (c.order) { await cancelOrder(c.order); c.order = null; } c.state = "close"; c.openPx = p; c.openFee = 0; left--; }
    saveG();
    engineLog(`🔧 ${G.sym}: позиція на біржі ${ex} ≠ сітка ${+mine.toFixed(8)} — сітку вирівняно під біржу`, "mute");
  }

  // ---------------- control ----------------
  async function boot() {
    connectPub(); connectPriv();
    if (G) { try { await loadSpec(G.sym); await ticker(G.sym); engineLog(`↺ Сітка ${G.sym} продовжує роботу після перезапуску`, "mute"); } catch (e) { setErr(e.message); } }
  }
  function setOn(on) {
    P.on = !!on; save();
    if (on) { ST.restartAt = 0; engineLog("▶ Грід-бот запущено", "up"); }
    else engineLog("■ Грід-бот зупинено: ордери сітки зняті (позиція лишається з її стопом, якщо є)", "mute");
    if (!on && G) { const g = G; for (const c of g.cells) if (c.order) { cancelOrderSym(g.sym, c.order); c.order = null; } saveG(); }
  }
  async function newGrid() { if (G) await closeGrid("нова сітка за командою", false); ST.restartAt = 0; if (P.on) { try { await build(); } catch (e) { setErr(e.message); } } return G ? "Нова сітка " + G.sym : ST.err; }
  async function closeNow() { if (!G) return "Сітки немає"; await closeGrid("закрито власником", false); return "Сітку закрито, позицію закрито ринком"; }
  function setParams(o) {
    const ch = [];
    for (const [k, v] of Object.entries(o || {})) {
      if (NUM[k]) { let n = Number(v); if (!Number.isFinite(n)) continue; n = Math.max(NUM[k][0], Math.min(NUM[k][1], n)); if (["grids", "leverage"].includes(k)) n = Math.round(n); if (P[k] !== n) { ch.push(`${k} ${P[k]}→${n}`); P[k] = n; } }
      else if (BOOL.includes(k) && typeof v === "boolean") { if (P[k] !== v) { ch.push(`${k} ${P[k]}→${v}`); P[k] = v; } }
      else if (k === "mode" && ["long", "short", "neutral"].includes(v)) { if (P.mode !== v) { ch.push(`режим ${P.mode}→${v}`); P.mode = v; } }
      else if (k === "symbol" && typeof v === "string") { const s2 = v.trim().toUpperCase(); const sym = s2 ? (s2.endsWith("USDT") ? s2 : s2 + "USDT") : ""; if (P.symbol !== sym) { ch.push(`монета ${sym || "—"}`); P.symbol = sym; } }
    }
    save();
    return ch;
  }

  // ---------------- view ----------------
  function view() {
    const g = G, p = ST.price;
    let unreal = 0, pos = 0;
    if (g) for (const c of g.cells) if (c.state === "close" && c.openPx) { unreal += (p - c.openPx) * c.qty * c.dir; pos += c.qty * c.dir; }
    const cells = TR.filter(t => t.kind === "cell"), day = cells.filter(t => t.at >= now() - 86400e3);
    return { params: P, ws: ST.pubUp, priv: ST.privUp, err: now() - ST.errAt < 120000 ? ST.err : "", price: p, picks: ST.picks || [], hedge: ST.hedge,
      restartIn: !g && P.on && ST.restartAt > now() ? Math.round((ST.restartAt - now()) / 1000) : null,
      grid: g && { sym: g.sym, mode: g.mode, low: g.low, high: g.high, stepPct: g.stepPct, qty: g.qty, lev: g.lev, n: g.cells.length, startedAt: g.startedAt, profit: +(g.profit || 0).toFixed(4), cycles: g.cycles || 0,
        trails: g.trails || 0, unreal: +unreal.toFixed(4), pos: +pos.toFixed(8), stopLow: +(g.low * (1 - P.slPct / 100)).toPrecision(6), stopHigh: +(g.high * (1 + P.slPct / 100)).toPrecision(6),
        cells: g.cells.map(c => ({ lo: c.lo, hi: c.hi, dir: c.dir, state: c.state, live: !!c.order, openPx: c.openPx })) },
      stats: { cycles: cells.length, profit: +cells.reduce((a, t) => a + t.pnl, 0).toFixed(4), day: +day.reduce((a, t) => a + t.pnl, 0).toFixed(4), dayCycles: day.length,
        stops: TR.filter(t => t.kind === "grid").length, stopPnl: +TR.filter(t => t.kind === "grid").reduce((a, t) => a + t.pnl, 0).toFixed(4) },
      trades: TR.slice(-30).reverse() };
  }

  boot();
  return { params: P, setOn, setParams, view, newGrid, closeNow, _test: { get G() { return G; }, ST, TR } };
}

module.exports = { create, DEFAULTS };
