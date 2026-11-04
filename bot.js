// SELL-сетка на самых волатильных за 1 мин монетах — серверный движок, торгует 24/7.
//  • раз в минуту: скан рынка USDT-перпов, самые волатильные по последней минутной свече (диапазон high-low);
//  • без позиции: SELL-сетка (levels заявок, шаг step %) от цены delay мин назад — едет за ценой с опозданием;
//  • шорт открыт: сетка входа снимается, доборы каждые addStep % выше последнего исполненного SELL (до maxPos заявок);
//  • выход ТОЛЬКО в плюс: одна лимитка BUY reduce-only на всю позицию на −take % от средней; стопа нет;
//  • только лимитные ордера; Live выкл — симуляция по живым ценам.
"use strict";

const DEF = { budgetPct: 90, maxCoins: 20, simEq: 1000, minTurn: 5, maxCand: 40, levels: 5, step: 1, usdt: 10, lev: 3, delay: 1, take: 0.7, addStep: 3, maxPos: 20, live: false };
const NUMS = ["budgetPct", "maxCoins", "simEq", "minTurn", "maxCand", "levels", "step", "usdt", "lev", "delay", "take", "addStep", "maxPos"];

function create(host) {
  const { bx, lsGet, lsSet, keys } = host;
  const now = () => Date.now(), sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const CFG = Object.assign({}, DEF, lsGet("vg.cfg", {}));
  const saveCfg = () => lsSet("vg.cfg", CFG);
  const STAT = Object.assign({ scans: 0, fills: 0, takes: 0, simPnl: 0 }, lsGet("vg.stat", {}));
  const saveStat = () => lsSet("vg.stat", STAT);
  const OURS = new Set(lsGet("vg.syms", []));          // coins this bot traded: their shorts get the take and the adds
  const saveOurs = () => lsSet("vg.syms", [...OURS]);
  const LAST_ADD = lsGet("vg.lastAdd", {});            // symbol -> the price of the last SELL fill (the base of the adds)
  const saveLastAdd = () => lsSet("vg.lastAdd", LAST_ADD);
  let running = !!lsGet("vg.running", false);
  const LOG = lsGet("vg.log", []);
  let logSaved = 0;
  function log(msg, cls) {
    LOG.unshift([now(), String(msg), cls || "info"]); LOG.length = Math.min(LOG.length, 400);
    if (now() - logSaved > 2000) { logSaved = now(); lsSet("vg.log", LOG); }
    host.log && host.log(msg);
  }

  // ---------------- Bybit ----------------
  const pub = (path, q) => bx(path, { query: q });
  async function priv(method, path, params) {
    const call = () => bx(path, { method, query: method === "GET" ? params : null, body: method === "POST" ? params : null, auth: true });
    try { return await call(); }
    catch (e) { if (e.code === 10006) { await sleep(1500); return call(); } throw e; }
  }
  let chain = Promise.resolve();   // orders one by one (~8/s)
  function enqueue(fn) { const p = chain.then(fn); chain = p.catch(() => {}).then(() => sleep(120)); return p; }
  const post = (path, params) => enqueue(() => priv("POST", path, params));

  const INST = {};
  async function inst(sym) {
    if (INST[sym]) return INST[sym];
    const r = await pub("/v5/market/instruments-info", { category: "linear", symbol: sym });
    const x = (r.list || [])[0]; if (!x) throw new Error("нет инструмента " + sym);
    return (INST[sym] = { tick: x.priceFilter.tickSize, step: x.lotSizeFilter.qtyStep, minQty: +x.lotSizeFilter.minOrderQty, minNot: +(x.lotSizeFilter.minNotionalValue || 5) });
  }
  const decs = (s) => (String(s).split(".")[1] || "").length;
  function rnd(v, step, mode) {
    const s = +step, d = decs(step), f = mode === "up" ? Math.ceil : mode === "down" ? Math.floor : Math.round;
    return (f(v / s - (mode === "up" ? 1e-9 : -1e-9)) * s).toFixed(d);
  }
  const lot = () => Math.max(CFG.usdt, (BUD && BUD.lot) || 0);   // USDT per order: at least the setting, more when the money would lie idle
  function qtyFor(i, px) {
    let q = +rnd(lot() / px, i.step, "down");
    if (q < i.minQty) q = i.minQty;
    if (q * px < i.minNot) q = +rnd(i.minNot * 1.02 / px, i.step, "up");
    return q;
  }
  const fmt = (v) => v == null || !Number.isFinite(+v) ? "—" : String(+(+v).toPrecision(6));

  // ---------------- state ----------------
  const C = {};
  let BUD = null, noMoneyAt = 0;   // the budget: 90 % of the deposit is always in the limits
  let ACTIVE = [], CAND = [], scanning = false, fastBusy = false, execFrom = 0, scanTimer = null, fastTimer = null, lastScanAt = 0;
  function coin(sym) { return C[sym] || (C[sym] = { sym, range: 0, last: 0, anchor: 0, grid: [], pos: null, takes: 0, lev: false, hedge: null, sim: { fills: [] }, stopKey: "", takeKey: "", lastAdd: LAST_ADD[sym] || 0 }); }
  const isLive = () => !!(CFG.live && keys());
  const hasShort = (s) => !!(C[s] && C[s].pos && C[s].pos.side === "Sell" && C[s].pos.size > 0);
  const gridCoins = () => [...new Set([...ACTIVE, ...Object.keys(C).filter(hasShort)])];

  // ---------------- the budget: budgetPct % of the deposit always works in the limits ----------------
  // one coin = `levels` orders × usdt (its entry grid, or its add ladder when a short is open); what the open shorts use is taken off
  async function budget() {
    const lev = Math.max(1, CFG.lev), perCoin = CFG.levels * CFG.usdt * 1.03 / lev;   // the margin of one coin's orders
    let eq = BUD ? BUD.eq : 0;
    if (isLive()) { try { const w = await priv("GET", "/v5/account/wallet-balance", { accountType: "UNIFIED" }); eq = +((w.list || [])[0] || {}).totalEquity || 0; } catch (e) { /* keep the last */ } }
    else eq = (CFG.simEq || 1000) + (STAT.simPnl || 0);
    const posIM = Object.values(C).filter(c => c.pos && c.pos.size > 0).reduce((a, c) => a + c.pos.size * c.pos.avg / lev, 0);
    const held = Object.keys(C).filter(hasShort).length;
    const cap = eq * CFG.budgetPct / 100, free = cap - posIM - held * perCoin;
    BUD = Object.assign(BUD || {}, { eq, cap, posIM, held, perCoinBase: perCoin, free, at: now() });
    return BUD;
  }

  // ---------------- 1) the scanner: the most volatile coins of the last minute ----------------
  async function scan() {
    if (scanning || !running) return; scanning = true;
    try {
      const t = await pub("/v5/market/tickers", { category: "linear" });
      const mv = (x) => Math.max(Math.abs(+x.price24hPcnt || 0) * 100 / 4, +x.prevPrice1h > 0 ? Math.abs(+x.lastPrice / +x.prevPrice1h - 1) * 100 : 0);
      const pool = (t.list || []).filter(x => x.symbol.endsWith("USDT") && +x.turnover24h >= CFG.minTurn * 1e6 && +x.lastPrice > 0)
        .sort((a, b) => mv(b) - mv(a)).slice(0, Math.max(10, Math.round(CFG.maxCand)));
      for (const s of gridCoins()) if (!pool.some(x => x.symbol === s)) { const x = (t.list || []).find(y => y.symbol === s); if (x) pool.push(x); }   // the shorts we hold are followed too
      const out = [];
      for (let i = 0; i < pool.length; i += 10) {
        const part = await Promise.all(pool.slice(i, i + 10).map(async (x) => {
          try {
            const k = await pub("/v5/market/kline", { category: "linear", symbol: x.symbol, interval: "1", limit: 2 + CFG.delay });
            const rows = (k.list || []).map(r => ({ o: +r[1], h: +r[2], l: +r[3], c: +r[4] }));   // newest first
            if (rows.length < 2) return null;
            const cur = rows[0], prev = rows[1], anchorRow = rows[Math.min(CFG.delay, rows.length - 1)];
            const rPrev = (prev.h - prev.l) / prev.c * 100, rCur = (cur.h - cur.l) / cur.c * 100, move = (cur.c / prev.o - 1) * 100;
            return { sym: x.symbol, rPrev, rCur, move, range: Math.max(rPrev, rCur), last: +x.lastPrice, anchor: anchorRow.c, turn: +x.turnover24h };
          } catch (e) { return null; }
        }));
        out.push(...part.filter(Boolean));
      }
      for (const x of out) x.held = hasShort(x.sym);
      out.sort((a, b) => b.range - a.range);
      // as many coins as the free money allows: 90 % of the deposit always in the limits (a coin stays while it is in the top K + 1)
      const B = await budget(), flat = out.filter(x => !x.held);
      // K coins: as many as the free money allows (up to maxCoins and the candidates found);
      // money left over when K hits that limit -> a bigger lot, so budgetPct % of the deposit is always in the limits
      const kMax = Math.min(Math.round(CFG.maxCoins), flat.length), K = Math.max(0, Math.min(kMax, Math.floor(B.free / B.perCoinBase)));
      const scale = K ? Math.max(1, B.free / (K * B.perCoinBase)) : 1;
      B.k = K; B.lot = +(CFG.usdt * scale).toFixed(2); B.perCoin = B.perCoinBase * scale;
      const keep = new Set(flat.slice(0, K + 1).map(x => x.sym));
      const next = flat.filter(x => ACTIVE.includes(x.sym) && keep.has(x.sym)).map(x => x.sym).slice(0, K);
      for (const x of flat.slice(0, K)) if (next.length < K && !next.includes(x.sym)) next.push(x.sym);
      if (!B.kLogged || B.k !== (BUD.prevK ?? -1) || Math.abs(B.lot / (BUD.prevLot || 1) - 1) > 0.1) log(`Бюджет: депозит ${B.eq.toFixed(2)} USDT · ${CFG.budgetPct}% = ${B.cap.toFixed(2)} · шорты ${B.posIM.toFixed(2)} + доборы ${(B.held * B.perCoin).toFixed(2)} · свободно ${Math.max(0, B.free).toFixed(2)} → сетки на ${K} монет по ${CFG.levels} заявок × ${B.lot} USDT${scale > 1.001 ? " (лот увеличен, чтобы занять " + CFG.budgetPct + "%)" : ""}`, "info");
      BUD.prevK = K; BUD.prevLot = B.lot; BUD.kLogged = true;
      const added = next.filter(s => !ACTIVE.includes(s)), removed = ACTIVE.filter(s => !next.includes(s));
      ACTIVE = next;
      CAND = out.filter(x => !x.held || ACTIVE.includes(x.sym)).slice(0, 15).map(x => ({ sym: x.sym, rPrev: x.rPrev, rCur: x.rCur, move: x.move, turn: x.turn, active: ACTIVE.includes(x.sym) }));
      for (const x of out) if (ACTIVE.includes(x.sym) || hasShort(x.sym)) { const c = coin(x.sym); c.range = x.range; c.last = x.last; c.anchor = x.anchor; }   // the grid anchor = the price delay min ago
      STAT.scans++; lastScanAt = now(); saveStat();
      if (added.length || removed.length) log(`Скан: ${ACTIVE.length} монет на ${CFG.budgetPct}% депозита → ${ACTIVE.map(s => s.replace(/USDT$/, "")).join(", ")}${added.length ? " · + " + added.join(", ") : ""}${removed.length ? " · − " + removed.join(", ") : ""}`, "ok");
      for (const s of removed) if (!hasShort(s)) await dropGrid(s);   // a coin with an open short keeps its grid (the adds)
      for (const s of gridCoins()) { try { await gridFor(s); } catch (e) { log(`${s}: сетка: ${e.message}`, "err"); } }
    } catch (e) { log("Скан: " + e.message, "err"); }
    finally { scanning = false; }
  }

  // ---------------- 2) flat: the SELL entry grid; a short: adds every addStep % above the last SELL fill ----------------
  async function gridFor(sym) {
    const c = coin(sym), i = await inst(sym);
    if (!c.anchor) return;
    const qty = qtyFor(i, c.anchor);
    c.grid = []; for (let k = 1; k <= CFG.levels; k++) c.grid.push(+rnd(c.anchor * (1 + CFG.step * k / 100), i.tick, "up"));
    if (!isLive()) return;   // the simulation fills the levels from the price
    OURS.add(sym); saveOurs();
    if (!c.lev) { try { await post("/v5/position/set-leverage", { category: "linear", symbol: sym, buyLeverage: String(CFG.lev), sellLeverage: String(CFG.lev) }); } catch (e) { if (e.code !== 110043) log(`${sym}: плечо: ${e.message}`, "err"); } c.lev = true; }
    if (c.hedge == null) { try { const r = await priv("GET", "/v5/position/list", { category: "linear", symbol: sym }); c.hedge = (r.list || []).some(p => +p.positionIdx > 0); } catch (e) { c.hedge = false; } }
    const posQty = c.pos && c.pos.side === "Sell" ? c.pos.size : 0, room = Math.max(0, CFG.maxPos - Math.round(posQty / qty));
    let want;
    if (posQty > 0) {
      const base = Math.max(c.lastAdd || 0, c.pos.avg);
      want = []; for (let k = 1; k <= Math.min(CFG.levels, room); k++) want.push(+rnd(base * (1 + CFG.addStep * k / 100), i.tick, "up"));
      c.grid = want;
    } else want = c.grid.slice(0, Math.min(CFG.levels, room));
    if (posQty && !room && !c.fullLogged) { c.fullLogged = true; log(`${sym}: позиция набрала ${CFG.maxPos} заявок — новых SELL нет, жду тейк`, "info"); } else if (room) c.fullLogged = false;
    const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_e")).sort((a, b) => +a.price - +b.price);
    const tick = +i.tick;
    for (let k = 0; k < Math.max(want.length, open.length); k++) {
      const o = open[k], px = want[k];
      if (o && px == null) { await cancelOrder(sym, o); continue; }
      if (!o) { await placeEntry(sym, px, qty, c); continue; }
      const qtyOff = Math.abs(+o.qty / qty - 1) > 0.1 && !(+o.cumExecQty > 0);   // the lot changed a lot -> the order follows it
      if (Math.abs(+o.price - px) >= tick / 2 || qtyOff) {
        try { await post("/v5/order/amend", Object.assign({ category: "linear", symbol: sym, orderId: o.orderId, price: String(px) }, qtyOff ? { qty: String(qty) } : {})); }
        catch (e) { await cancelOrder(sym, o); await placeEntry(sym, px, qty, c); }
      }
    }
  }
  async function placeEntry(sym, px, qty, c) {
    try { await post("/v5/order/create", { category: "linear", symbol: sym, side: "Sell", orderType: "Limit", qty: String(qty), price: String(px), timeInForce: "GTC", positionIdx: c.hedge ? 2 : 0, orderLinkId: "vg_e" + now() + Math.floor(Math.random() * 1e4) }); }
    catch (e) {
      if (/^(110004|110007|110012|110044|110045|110052)$/.test(String(e.code)) || /not enough|insufficient/i.test(e.message)) { if (now() - (noMoneyAt || 0) > 60000) { noMoneyAt = now(); log(`Маржи на новые заявки не хватает — ${CFG.budgetPct}% депозита уже в работе, жду`, "info"); } }
      else log(`${sym}: SELL ${qty} @ ${px}: ${e.message}`, "err");
    }
  }
  async function cancelOrder(sym, o) { try { await post("/v5/order/cancel", { category: "linear", symbol: sym, orderId: o.orderId }); } catch (e) { /* gone */ } }
  async function dropGrid(sym) {
    const c = coin(sym); c.grid = [];
    if (!isLive()) return;
    try {
      const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_e"));
      for (const o of open) await cancelOrder(sym, o);
      if (open.length) log(`${sym}: сетка снята (${open.length} заявок)`, "info");
    } catch (e) { log(`${sym}: снять сетку: ${e.message}`, "err"); }
  }

  // ---------------- 3) fills; ONE take for the whole short: BUY reduce-only at −take % from the average ----------------
  const SEEN = new Set();
  async function pollFills() {
    const touched = new Set();
    const r = await priv("GET", "/v5/execution/list", { category: "linear", limit: 100, startTime: Math.max(execFrom, now() - 120000) });
    for (const x of (r.list || []).slice().reverse()) {
      if (SEEN.has(x.execId)) continue; SEEN.add(x.execId);
      const id = x.orderLinkId || "";
      if (id.startsWith("vg_e") && x.side === "Sell") {
        const c = coin(x.symbol); STAT.fills++;
        c.lastAdd = Math.max(c.lastAdd || 0, +x.execPrice); LAST_ADD[x.symbol] = c.lastAdd; saveLastAdd(); touched.add(x.symbol);
        log(`${x.symbol}: SELL ${x.execQty} исполнен @ ${x.execPrice}${hasShort(x.symbol) ? " — добор" : ""} → следующий добор +${CFG.addStep}%`, "ok");
      } else if (id.startsWith("vg_t") && +x.leavesQty === 0) { STAT.takes++; coin(x.symbol).takes++; log(`${x.symbol}: ✅ тейк исполнен @ ${x.execPrice} — шорт закрыт в плюс`, "ok"); }
    }
    if (SEEN.size > 5000) { const a = [...SEEN].slice(-2000); SEEN.clear(); a.forEach(v => SEEN.add(v)); }
    saveStat();
    return touched;
  }
  async function syncTake(sym, c, i) {
    const px = rnd(c.pos.avg * (1 - CFG.take / 100), i.tick, "down"), qty = rnd(c.pos.size, i.step, "down"), key = px + "|" + qty;
    if (c.takeKey === key) return;
    const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_t"));
    for (const o of open.slice(1)) await cancelOrder(sym, o);
    const o = open[0];
    try {
      if (o) await post("/v5/order/amend", { category: "linear", symbol: sym, orderId: o.orderId, price: px, qty });
      else await post("/v5/order/create", { category: "linear", symbol: sym, side: "Buy", orderType: "Limit", qty, price: px, timeInForce: "GTC", reduceOnly: true, positionIdx: c.pos.idx, orderLinkId: "vg_t" + now() + Math.floor(Math.random() * 1e4) });
      c.takeKey = key; log(`${sym}: тейк на всю позицию: BUY ${qty} @ ${px} (−${CFG.take}% от средней ${fmt(c.pos.avg)})`, "info");
    } catch (e) {
      if (o) { await cancelOrder(sym, o); c.takeKey = ""; }
      log(`${sym}: тейк: ${e.message}`, "err");
    }
  }
  async function clearStops(sym, idx) {
    try { const t = (await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym, orderFilter: "tpslOrder" })).list || [];
      for (const o of t) if (/StopLoss|TrailingStop/i.test(o.stopOrderType || "")) await cancelOrder(sym, o); } catch (e) { /* none */ }
    try { await post("/v5/position/trading-stop", { category: "linear", symbol: sym, tpslMode: "Full", stopLoss: "0", trailingStop: "0", positionIdx: idx }); } catch (e) { /* none */ }
  }
  // ---------------- 4) the positions: the take always set, never a stop ----------------
  async function pollPositions() {
    const r = await priv("GET", "/v5/position/list", { category: "linear", settleCoin: "USDT" });
    const seen = new Set();
    for (const p of r.list || []) {
      if (!(+p.size > 0)) continue;
      const sym = p.symbol; seen.add(sym);
      const c = coin(sym); c.pos = { side: p.side, size: +p.size, avg: +p.avgPrice, upnl: +p.unrealisedPnl || 0, idx: +p.positionIdx || 0 }; c.last = +p.markPrice || c.last;
      if (p.side !== "Sell" || !OURS.has(sym)) continue;
      const i = await inst(sym);
      if (!c.anchor) c.anchor = c.pos.avg;
      await syncTake(sym, c, i);
      if (c.stopKey !== "none") { await clearStops(sym, c.pos.idx); c.stopKey = "none"; log(`${sym}: стопа нет — в минус не выхожу; на росте добор каждые ${CFG.addStep}%, выход только тейком в плюс`, "info"); }
    }
    for (const s of Object.keys(C)) if (!seen.has(s) && C[s].pos) {
      if (C[s].pos.side === "Sell" && C[s].pos.size > 0 && OURS.has(s)) log(`${s}: позиция закрыта`, "info");
      C[s].pos = null; C[s].stopKey = ""; C[s].takeKey = ""; C[s].lastAdd = 0; delete LAST_ADD[s]; saveLastAdd();
      if (!ACTIVE.includes(s)) await dropGrid(s);   // it was kept only for the adds
    }
  }

  // ---------------- the simulation (Live off): the same rules on the live prices ----------------
  async function simTick() {
    const t = await pub("/v5/market/tickers", { category: "linear" });
    const px = {}; for (const x of t.list || []) px[x.symbol] = +x.lastPrice;
    for (const s of Object.keys(C)) {
      const c = C[s], p = px[s]; if (!p) continue; c.last = p;
      const i = INST[s]; if (!i) continue;
      const fills = c.sim.fills;
      if (!fills.length) {
        if (ACTIVE.includes(s)) for (const lv of c.grid) if (p >= lv && fills.length < CFG.maxPos && !fills.some(o => Math.abs(o.px - lv) < +i.tick / 2)) { fills.push({ px: lv, qty: qtyFor(i, lv) }); STAT.fills++; log(`[СИМ] ${s}: SELL @ ${lv}`, "ok"); }
      } else {
        let top = Math.max(...fills.map(o => o.px));
        while (fills.length < CFG.maxPos && p >= top * (1 + CFG.addStep / 100)) { const lv = +rnd(top * (1 + CFG.addStep / 100), i.tick, "up"); fills.push({ px: lv, qty: qtyFor(i, lv) }); STAT.fills++; top = lv; log(`[СИМ] ${s}: добор №${fills.length - 1} SELL @ ${lv} (+${CFG.addStep}% от прошлого)`, "ok"); }
        c.grid = []; for (let k = 1; k <= Math.min(CFG.levels, CFG.maxPos - fills.length); k++) c.grid.push(+rnd(top * (1 + CFG.addStep * k / 100), i.tick, "up"));
      }
      if (fills.length) {
        const size = fills.reduce((a, o) => a + o.qty, 0), avg = fills.reduce((a, o) => a + o.px * o.qty, 0) / size, take = avg * (1 - CFG.take / 100);
        c.pos = { side: "Sell", size, avg, upnl: (avg - p) * size, sim: true };
        if (p <= take) { const pnl = (avg - take) * size - (avg + take) * size * 0.0004; STAT.simPnl += pnl; STAT.takes++; c.takes++; c.sim.fills = []; c.pos = null; log(`[СИМ] ${s}: ✅ тейк @ ${rnd(take, i.tick)} на всю позицию (${fills.length} заявок) PnL +${pnl.toFixed(4)}`, "ok"); }
      } else c.pos = null;
    }
    saveStat();
  }

  // ---------------- the loops ----------------
  async function fast() {
    if (!running || fastBusy) return; fastBusy = true;
    try {
      if (isLive()) { const touched = await pollFills(); await pollPositions(); for (const s of touched) { try { await gridFor(s); } catch (e) { log(`${s}: доборы: ${e.message}`, "err"); } } }
      else await simTick();
    } catch (e) { log("Опрос: " + e.message, "err"); }
    finally { fastBusy = false; }
  }
  function schedule() {
    clearInterval(scanTimer); clearInterval(fastTimer); scanTimer = fastTimer = null;
    if (!running) return;
    scanTimer = setInterval(() => { scan().catch(() => {}); }, Math.max(1, CFG.delay) * 60000);
    fastTimer = setInterval(() => { fast().catch(() => {}); }, 3000);
  }

  // ---------------- commands ----------------
  function setCfg(o) {
    const ch = [];
    for (const k of NUMS) if (o[k] != null && o[k] !== "" && Number.isFinite(Number(o[k])) && Number(o[k]) !== CFG[k]) { CFG[k] = Number(o[k]); ch.push(k); }
    CFG.budgetPct = Math.max(5, Math.min(100, CFG.budgetPct)); CFG.maxCoins = Math.max(1, Math.min(60, Math.round(CFG.maxCoins))); CFG.simEq = Math.max(10, CFG.simEq); CFG.levels = Math.max(1, Math.min(20, Math.round(CFG.levels))); CFG.delay = Math.max(1, Math.min(10, Math.round(CFG.delay)));
    CFG.addStep = Math.max(0.5, CFG.addStep || 3); CFG.maxPos = Math.max(1, Math.round(CFG.maxPos)); CFG.step = Math.max(0.1, CFG.step); CFG.take = Math.max(0.1, CFG.take);
    CFG.lev = Math.max(1, Math.min(50, Math.round(CFG.lev))); CFG.usdt = Math.max(1, CFG.usdt);
    if (typeof o.live === "boolean" && o.live !== CFG.live) { CFG.live = o.live; ch.push("live"); log(CFG.live ? "⚠ LIVE включён: реальные ордера" : "LIVE выключен: симуляция", CFG.live ? "err" : "info"); for (const c of Object.values(C)) { c.sim.fills = []; c.pos = null; c.lev = false; } }
    if (ch.includes("lev")) for (const c of Object.values(C)) c.lev = false;
    saveCfg();
    if (ch.includes("delay")) schedule();
    return ch;
  }
  async function start() {
    if (CFG.live && !keys()) return "Нет BYBIT_API_KEY / BYBIT_API_SECRET в Railway → Variables";
    running = true; lsSet("vg.running", true); execFrom = now() - 60000;
    log(`СТАРТ ${isLive() ? "LIVE" : "СИМУЛЯЦИЯ"} 24/7: лимитки на ${CFG.budgetPct}% депозита — сетки на столько самых волатильных за 1 мин монет, сколько помещается (до ${CFG.maxCoins}) · ${CFG.levels} SELL по ${CFG.usdt} USDT, шаг ${CFG.step}% · сетка за ценой с опозданием ${CFG.delay} мин · тейк −${CFG.take}% от средней · без стопа (только плюс) · добор каждые ${CFG.addStep}% до ${CFG.maxPos} заявок · плечо x${CFG.lev}`, "ok");
    schedule(); scan().catch(() => {});
    return "Бот запущен — торгует на сервере 24/7";
  }
  async function stop() {
    running = false; lsSet("vg.running", false); schedule();
    const list = ACTIVE.slice(); ACTIVE = [];
    for (const s of list) if (!hasShort(s)) await dropGrid(s);
    for (const s of Object.keys(C)) if (hasShort(s) && isLive()) await dropGrid(s);
    for (const c of Object.values(C)) if (!isLive()) { c.sim.fills = []; c.pos = null; }
    log("СТОП: сетки и доборы сняты. Открытые шорты остаются с тейком на бирже.", "info");
    return "Остановлен";
  }
  function resetStats() { Object.assign(STAT, { scans: 0, fills: 0, takes: 0, simPnl: 0 }); saveStat(); return "Статистика сброшена"; }
  // one-time: positions / orders of the previous bot of this project are taken over (its orders removed, its shorts get the take and the adds)
  async function adoptOld() {
    if (!keys() || lsGet("vg.adopted", false)) return;
    try {
      const orders = (await priv("GET", "/v5/order/realtime", { category: "linear", settleCoin: "USDT", limit: 50 })).list || [];
      let n = 0; for (const o of orders) if (/^sg_/.test(o.orderLinkId || "")) { await cancelOrder(o.symbol, o); n++; }
      const pos = (await priv("GET", "/v5/position/list", { category: "linear", settleCoin: "USDT" })).list || [];
      const shorts = pos.filter(p => +p.size > 0 && p.side === "Sell").map(p => p.symbol);
      for (const s of shorts) OURS.add(s); saveOurs();
      log(`Переход на новую стратегию: снято ${n} заявок прежнего бота${shorts.length ? `; шорты ${shorts.join(", ")} беру под тейк −${CFG.take}% и доборы` : ""}`, "ok");
    } catch (e) { log("Переход: " + e.message, "err"); }
    lsSet("vg.adopted", true);
  }

  function view() {
    const rows = [...new Set([...ACTIVE, ...Object.keys(C).filter(s => C[s].pos)])].map(s => {
      const c = coin(s);
      return { sym: s, active: ACTIVE.includes(s), held: hasShort(s), range: c.range, last: c.last, anchor: c.anchor, grid: c.grid, pos: c.pos, take: c.pos ? c.pos.avg * (1 - CFG.take / 100) : null, takes: c.takes };
    });
    const upnl = rows.reduce((a, r) => a + (r.pos ? r.pos.upnl : 0), 0);
    return { bud: BUD, running, live: isLive(), cfg: CFG, stat: STAT, rows, cand: CAND, upnl, nextScan: running && lastScanAt ? Math.max(0, Math.round((lastScanAt + CFG.delay * 60000 - now()) / 1000)) : null, log: LOG.slice(0, 200) };
  }

  (async () => { await adoptOld(); if (running) { log("Сервер перезапущен — бот продолжает торговать", "ok"); execFrom = now() - 5000; /* only new fills: the base of the adds is kept on the disk */ schedule(); scan().catch(() => {}); } })();
  return { start, stop, setCfg, resetStats, view };
}
module.exports = { create };
