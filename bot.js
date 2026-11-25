// SELL-сетка на самых волатильных за 1 мин монетах — серверный движок, торгует 24/7.
//  • раз в минуту: скан рынка USDT-перпов, самые волатильные по последней минутной свече (диапазон high-low);
//  • без позиции: SELL-сетка (levels заявок, шаг step %) от цены delay мин назад — едет за ценой с опозданием;
//  • шорт открыт: сетка входа снимается, доборы каждые addStep % выше последнего исполненного SELL (до maxPos заявок);
//  • выход в плюс: лимитка BUY reduce-only на −take % от средней; аварийный стоп +stopPct % от средней (маркет, на бирже, 0 = без стопа);
//  • только лимитные ордера; Live выкл — симуляция по живым ценам.
"use strict";
const fs = require("fs");
const path = require("path");

const DEF = { budgetPct: 45, fillPct: 90, maxCoins: 20, simEq: 1000, minTurn: 5, maxCand: 40, levels: 5, step: 1, usdt: 10, lev: 3, delay: 1, take: 1, btcOnly: false, trailOn: true, trailGap: 0.3, gridTrail: 0.3, addStep: 5, maxAdds: 3, maxPos: 20, stopPct: 5, addMult: 1.3, live: false, longOn: true, longBtc: true, oiOn: false, oiPct: 0, coolMin: 180, pumpPct: 8, resPct: 40, maxLossUsd: 3, turbo: false, turboSave: null, turboFrom: 0, usPause: true };
// ⚡ ТУРБО: быстрый оборот (близкие заявки, быстрый тейк) + умный риск (мало доборов, близкий стоп, авто-выкл при минусе)
const TURBO = { step: 0.4, levels: 2, delay: 1, gridTrail: 0.2, take: 0.5, trailGap: 0.5, addStep: 8, maxAdds: 2, addMult: 1.5, stopPct: 3 };
// 🔔 пауза на открытие биржи США: пн–пт 9:00–10:00 по Нью-Йорку (15:00–16:00 Мадрид, летнее/зимнее время учтено) — новые сетки не ставлю
function usOpenNow(t) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(new Date(t || Date.now())).map(x => [x.type, x.value]));
  if (p.weekday === "Sat" || p.weekday === "Sun") return false;
  const m = (+p.hour % 24) * 60 + +p.minute; return m >= 9 * 60 && m < 10 * 60;
}
// 🌙 ночная пауза каждый день 00:45–03:00 по Мадриду: новая дневная свеча крипты (00:00 UTC) + открытие Токио и Китая
function nightNow(t) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Madrid", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(new Date(t || Date.now())).map(x => [x.type, x.value]));
  const m = (+p.hour % 24) * 60 + +p.minute; return m >= 45 && m < 3 * 60;
}
// какая пауза сейчас: "us" / "night" / ""
function pauseNow(t) { return usOpenNow(t) ? "us" : nightNow(t) ? "night" : ""; }
const TURBO_LOSS_PCT = 3;   // турбо выключается само, если с момента включения закрыто в минус 3% депозита
const NUMS = ["budgetPct", "fillPct", "maxCoins", "simEq", "minTurn", "maxCand", "levels", "step", "usdt", "lev", "delay", "take", "trailGap", "gridTrail", "addStep", "maxAdds", "maxPos", "stopPct", "addMult", "oiPct", "coolMin", "pumpPct", "resPct", "maxLossUsd"];

function create(host) {
  const { bx, lsGet, lsSet, keys } = host;
  // ---------------- 📒 the detailed journal: one JSON line per event, one file per UTC day, on the volume ----------------
  const JDIR = path.join(host.dataDir || ".", "journal");
  try { fs.mkdirSync(JDIR, { recursive: true }); } catch (e) { /* read-only */ }
  const dayOf = (t) => new Date(t || Date.now()).toISOString().slice(0, 10);
  function jlog(type, data) {
    const line = JSON.stringify(Object.assign({ t: Date.now(), type, live: !!(CFG.live && keys()) }, data)) + "\n";
    fs.appendFile(path.join(JDIR, dayOf() + ".jsonl"), line, () => {});
  }
  const TRADES = lsGet("vg.trades", {});   // symbol -> the open trade being recorded
  const saveTrades = () => lsSet("vg.trades", TRADES);
  const now = () => Date.now(), sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const CFG = Object.assign({}, DEF, lsGet("vg.cfg", {}));
  const saveCfg = () => lsSet("vg.cfg", CFG);
  if (!lsGet("vg.adds35", false)) { if (CFG.addStep === 3) CFG.addStep = 5; if (CFG.maxAdds == null) CFG.maxAdds = 3; saveCfg(); lsSet("vg.adds35", true); }   // the default: 3 adds every 5 % (an own value stays)
  if (!lsGet("vg.lev3", false)) { CFG.lev = Math.min(3, CFG.lev || 3); saveCfg(); lsSet("vg.lev3", true); }   // leverage 3x at most: a margin reserve
  if (!lsGet("vg.strict45", false)) { CFG.budgetPct = 45; saveCfg(); lsSet("vg.strict45", true); }   // strictly 45 % / 45 % with gridbot-html
  if (!lsGet("vg.split45", false)) { CFG.budgetPct = 45; saveCfg(); lsSet("vg.split45", true); }   // the account is split 50/50 with gridbot-html: 45 % of the deposit here
  if (!lsGet("vg.long1", false)) { CFG.longOn = true; CFG.longBtc = true; saveCfg(); lsSet("vg.long1", true); }   // лонги на дампах включены
  if (!lsGet("vg.solo80", false)) { CFG.budgetPct = 80; saveCfg(); lsSet("vg.solo80", true); }   // gridbot-html выключен — Стакан один на счёте: 80 % депозита
  if (!lsGet("vg.geo13", false)) { CFG.addMult = 1.3; CFG.stopPct = 5; saveCfg(); lsSet("vg.geo13", true); }   // adds ×1.3 each; the stop 5 % above the last planned add
  if (!lsGet("vg.stop20b", false)) { CFG.stopPct = 20; saveCfg(); lsSet("vg.stop20b", true); }   // the emergency stop is on: 20 % against the short
  if (!lsGet("vg.take1", false)) { if (CFG.take === 0.7) CFG.take = 1; saveCfg(); lsSet("vg.take1", true); }   // the default take is 1 % now (an own value stays)
  if (CFG.resPct == null || !Number.isFinite(+CFG.resPct)) { CFG.resPct = 40; saveCfg(); }
  if (!lsGet("vg.maxLoss3", false)) { if (CFG.maxLossUsd === 1) CFG.maxLossUsd = 3; saveCfg(); lsSet("vg.maxLoss3", true); }   // макс. минус на позицию 3 USDT (было 1)
  if (!lsGet("vg.turboGap05", false)) { if (CFG.turbo && CFG.trailGap === 0.15) CFG.trailGap = 0.5; saveCfg(); lsSet("vg.turboGap05", true); }   // турбо: откат трейлинга 0.5 % (было 0.15 %)
  if (!lsGet("vg.turboAdd8", false)) { if (CFG.turbo && CFG.addStep === 3) { CFG.addStep = 8; } saveCfg(); lsSet("vg.turboAdd8", true); }   // турбо: добор против шорта каждые 8 % (было 3 %)
  const STAT = Object.assign({ scans: 0, fills: 0, takes: 0, simPnl: 0, since: 0, realized: 0, closed: 0, realDone: 0, doneUntil: 0 }, lsGet("vg.stat", {}));
  const saveStat = () => lsSet("vg.stat", STAT);
  const OURS = new Set(lsGet("vg.syms", []));          // coins this bot traded: their shorts get the take and the adds
  const saveOurs = () => lsSet("vg.syms", [...OURS]);
  const TRAIL = lsGet("vg.trail", {});                 // symbol -> the trailing take of the short { avg, on, best, level, orderId }
  const saveTrail = () => lsSet("vg.trail", TRAIL);
  const LAST_ADD = lsGet("vg.lastAdd", {});            // symbol -> the price of the last SELL fill (the base of the adds)
  const saveLastAdd = () => lsSet("vg.lastAdd", LAST_ADD);
  const COOL = lsGet("vg.cool", {});                  // symbol -> until when the coin is not traded after a loss (the stop)
  const saveCool = () => lsSet("vg.cool", COOL);
  const cooling = (sym) => COOL[sym] && COOL[sym] > now();
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
  const addK = (n) => Math.pow(Math.max(1, CFG.addMult || 1), Math.max(0, n));   // add №n = lot × addMult^n (geometric adds)
  function qtyFor(i, px, mult, base) {
    let q = +rnd((base || lot()) * (mult || 1) / px, i.step, "down");
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
  const hasPos = (s) => !!(C[s] && C[s].pos && C[s].pos.size > 0);   // шорт или лонг
  const sideOf = (c) => (c.pos && c.pos.size > 0 ? c.pos.side : (c.side || "Sell"));   // Sell = шорт (сетка сверху), Buy = лонг (сетка снизу)
  const sg = (side) => (side === "Buy" ? -1 : 1);
  const word = (side) => (side === "Buy" ? "лонг" : "шорт");
  const gridCoins = () => [...new Set([...ACTIVE, ...Object.keys(C).filter(hasPos)])];

  // ---------------- ₿ the local trend of Bitcoin: 5m EMA9 < EMA21 and the price below EMA21 = down ----------------
  let BTC = { down: null, at: 0, why: "—" };
  async function btcTrend() {
    if (now() - BTC.at < 50000 && BTC.down != null) return BTC;
    try {
      const k = await pub("/v5/market/kline", { category: "linear", symbol: "BTCUSDT", interval: "5", limit: 60 });
      const cl = (k.list || []).map(r => +r[4]).reverse();   // oldest first
      const ema = (n) => { const a = 2 / (n + 1); let e = cl[0]; for (const x of cl) e = x * a + e * (1 - a); return e; };
      const e9 = ema(9), e21 = ema(21), last = cl[cl.length - 1], ch30 = (last / cl[Math.max(0, cl.length - 7)] - 1) * 100;
      const down = e9 < e21 && last < e21;
      BTC = { down, e9: +e9.toFixed(1), e21: +e21.toFixed(1), last, ch30: +ch30.toFixed(2), at: now(),
        why: `BTC ${last} · EMA9 ${e9.toFixed(0)} ${e9 < e21 ? "<" : ">"} EMA21 ${e21.toFixed(0)} · 30 мин ${ch30 >= 0 ? "+" : ""}${ch30.toFixed(2)}% → ${down ? "тренд ВНИЗ" : "не вниз"}` };
    } catch (e) { BTC.why = "BTC: " + e.message; }
    return BTC;
  }

  // how many add orders a short may still place (the same limits as the add ladder in gridFor)
  function addsLeft(sym) {
    const done = Math.max(0, ((TRADES[sym] && TRADES[sym].fills) || 1) - 1);
    return Math.max(0, Math.min(CFG.levels, Math.round(CFG.maxAdds) - done));
  }
  // РЕЗЕРВ ПОД ДОБОРЫ: сколько денег (в лотах) нужно монете на ВСЮ сделку, а не только на сетку входа.
  // Худший случай по n = 1..levels: исполнилось n заявок входа, потом все оставшиеся доборы (×addMult каждый)
  // + запас на минус позиции, пока цена идёт до последнего добора. Деньги под это держатся с самого начала.
  function tradeUnits() {
    const lev = Math.max(1, CFG.lev), A = Math.round(CFG.maxAdds), m = Math.max(1, CFG.addMult || 1);
    let worst = CFG.levels * 1.03 / lev;   // одна сетка входа
    for (let n = 1; n <= CFG.levels; n++) {
      const f = []; let px = 1;
      for (let j = 0; j < n; j++) { px *= 1 + CFG.step / 100; f.push([1, px]); }
      for (let j = n, k = 0; j <= A && k < CFG.levels; j++, k++) { px *= 1 + CFG.addStep / 100; f.push([Math.pow(m, j), px]); }
      const units = f.reduce((a, x) => a + x[0], 0), loss = f.reduce((a, x) => a + x[0] * (px / x[1] - 1), 0);
      worst = Math.max(worst, units * 1.03 / lev + loss);
    }
    // 💰 запас под доборы и минус до последнего добора держу только на resPct % (остальное работает в заявках входа);
    // не хватит маржи на добор — бот снимет чужие заявки входа или закроет позицию около нуля
    const entry = CFG.levels * 1.03 / lev, R = Math.max(0, Math.min(100, CFG.resPct ?? 100)) / 100;
    return entry + Math.max(0, worst - entry) * R;
  }
  const tradeLot = (sym) => (TRADES[sym] && TRADES[sym].lot) || lot();   // доборы сделки — от её собственного лота (лот новых монет может меняться)
  // резерв уже открытой позиции: маржа оставшихся доборов + минус, если цена дойдёт до последнего добора
  function heldNeed(sym) {
    const c = C[sym]; if (!c || !c.pos) return 0;
    const lev = Math.max(1, CFG.lev), L = c.pos.side === "Buy", d = L ? -1 : 1, left = addsLeft(sym), l = tradeLot(sym);
    const done = Math.max(0, ((TRADES[sym] && TRADES[sym].fills) || 1) - 1);
    const p0 = c.lp || c.last || c.pos.avg, base = L ? Math.min(c.lastAdd || Infinity, c.pos.avg) : Math.max(c.lastAdd || 0, c.pos.avg);
    const lastPx = base * (1 + d * CFG.addStep * left / 100);
    let need = Math.max(0, d * (lastPx / p0 - 1)) * c.pos.size * p0;   // минус текущей позиции до последнего добора
    for (let k = 1; k <= left; k++) { const px = base * (1 + d * CFG.addStep * k / 100), u = l * addK(done + k); need += u * 1.03 / lev + u * Math.max(0, d * (lastPx / px - 1)); }
    if (CFG.maxLossUsd > 0) need = Math.min(need, CFG.maxLossUsd);   // стоп по лимиту минуса: больше не потеряем и доборов за ним нет
    return need * Math.max(0, Math.min(100, CFG.resPct ?? 100)) / 100;
  }

  // ---------------- the budget: budgetPct % of the deposit always works in the limits ----------------
  // one coin = `levels` orders × usdt (its entry grid, or its add ladder when a short is open); what the open shorts use is taken off
  let OTHER = new Set();   // coins the other bot (gridbot-html) is trading now: we never enter them — one coin = one position on Bybit
  async function budget() {
    const lev = Math.max(1, CFG.lev), perCoin = CFG.usdt * tradeUnits();   // одна монета = сетка входа + ВСЕ её доборы + запас на минус
    let eq = BUD ? BUD.eq : 0, im = null;
    if (isLive()) { try { const w = await priv("GET", "/v5/account/wallet-balance", { accountType: "UNIFIED" }); const a = (w.list || [])[0] || {}; eq = +a.totalEquity || 0; if (a.totalInitialMargin != null) im = +a.totalInitialMargin || 0; } catch (e) { /* keep the last */ } }
    else eq = (CFG.simEq || 1000) + (STAT.simPnl || 0);
    // only OUR shorts count (the other bot on the account has its own share of the deposit)
    const posIM = Object.values(C).filter(c => c.pos && c.pos.size > 0 && OURS.has(c.sym)).reduce((a, c) => a + c.pos.size * c.pos.avg / lev, 0);
    // what the OTHER bot really holds on the exchange = the account's margin minus ours (our shorts + our open SELL limits).
    // The other bot's neutral grids take much less margin than its own count says, so the share split left ~half of the
    // deposit idle: we fill the account up to fillPct % of the deposit with what the other bot does not use.
    let foreign = null;
    const otherSyms = new Set();
    if (im != null) {
      try {
        let ourOrd = 0, cursor = "";
        for (let n = 0; n < 10; n++) {
          const r = await priv("GET", "/v5/order/realtime", Object.assign({ category: "linear", settleCoin: "USDT", limit: 50 }, cursor ? { cursor } : {}));
          for (const o of r.list || []) {
            if ((o.orderLinkId || "").startsWith("vg_") && !o.reduceOnly) ourOrd += (+o.qty - (+o.cumExecQty || 0)) * +o.price / lev;
            if ((o.orderLinkId || "").startsWith("grid")) otherSyms.add(o.symbol);   // gridbot-html trades this coin
          }
          cursor = r.nextPageCursor || ""; if (!cursor || !(r.list || []).length) break;
        }
        foreign = Math.max(0, im - posIM - ourOrd);
        OTHER = otherSyms;
      } catch (e) { foreign = null; }
    }
    const heldSyms = Object.keys(C).filter(s => hasPos(s) && OURS.has(s)), held = heldSyms.length;
    // reserve only for the adds a short can still make (all adds done -> no orders, no reserve: the money works elsewhere)
    const addRes = heldSyms.reduce((a, s) => a + heldNeed(s), 0);
    const capOwn = eq * CFG.budgetPct / 100;
    const cap = capOwn, free = cap - posIM - addRes;   // strictly budgetPct % (45 %): the other half of the account is gridbot-html's
    BUD = Object.assign(BUD || {}, { eq, cap, capOwn, foreign, posIM, held, addRes, perCoinBase: perCoin, free, at: now() });
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
            let oi = null;   // открытый интерес: изменение за 15 мин (свечи OI по 5 мин), % ; null = нет данных
            if (CFG.oiOn) { try { const o = await pub("/v5/market/open-interest", { category: "linear", symbol: x.symbol, intervalTime: "5min", limit: 4 }); const l = (o.list || []).map(r => +r.openInterest); if (l.length >= 2 && l[l.length - 1] > 0) oi = (l[0] / l[l.length - 1] - 1) * 100; } catch (e) { /* нет данных */ } }
            const cur = rows[0], prev = rows[1], anchorRow = rows[Math.min(CFG.delay, rows.length - 1)];
            const rPrev = (prev.h - prev.l) / prev.c * 100, rCur = (cur.h - cur.l) / cur.c * 100, move = (cur.c / prev.o - 1) * 100;
            const ch1h = +x.prevPrice1h > 0 ? (+x.lastPrice / +x.prevPrice1h - 1) * 100 : 0;
            return { sym: x.symbol, rPrev, rCur, move, range: Math.max(rPrev, rCur), last: +x.lastPrice, anchor: anchorRow.c, turn: +x.turnover24h, oi, ch1h };
          } catch (e) { return null; }
        }));
        out.push(...part.filter(Boolean));
      }
      for (const x of out) x.held = hasPos(x.sym);
      out.sort((a, b) => b.range - a.range);
      // as many coins as the free money allows: 90 % of the deposit always in the limits (a coin stays while it is in the top K + 1)
      for (const s of Object.keys(COOL)) if (!cooling(s)) { delete COOL[s]; saveCool(); }
      const coolNow = out.filter(x => !x.held && cooling(x.sym)).map(x => x.sym.replace(/USDT$/, "")).sort().join(",");
      if (coolNow !== (BUD && BUD.coolLogged || "")) { if (BUD) BUD.coolLogged = coolNow; if (coolNow) log(`🧊 Кулдаун после стопа — не захожу: ${coolNow}`, "info"); }
      const B = await budget(), flat = out.filter(x => !x.held && !OTHER.has(x.sym) && !cooling(x.sym));
      turboGuard(B.eq);
      const busy = [...OTHER].filter(s => !hasShort(s)).map(s => s.replace(/USDT$/, "")).sort().join(",");
      if (busy !== (BUD.otherLogged ?? "")) { BUD.otherLogged = busy; if (busy) log(`Монеты gridbot-html — не захожу: ${busy}`, "info"); }
      // K coins: as many as the free money allows (up to maxCoins and the candidates found);
      // money left over when K hits that limit -> a bigger lot, so budgetPct % of the deposit is always in the limits
      const kMax = Math.min(Math.round(CFG.maxCoins), flat.length);
      let K = Math.max(0, Math.min(kMax, Math.floor(B.free / B.perCoinBase)));
      const btc = await btcTrend();
      // какие стороны можно открывать: SELL (шорт на пампе) и BUY (лонг на дампе)
      const pz = CFG.usPause ? pauseNow() : "", usP = !!pz;
      if (pz !== (BTC.pauseKind || "")) { BTC.pauseKind = pz; BTC.usPaused = usP; log(pz === "us" ? "🔔 Открытие биржи США (15:00–16:00 по Мадриду): снимаю все заявки и закрываю все позиции лимитками по лучшей цене (стоп остаётся до закрытия)" : pz === "night" ? "🌙 Ночная пауза (00:45–03:00 по Мадриду: новая дневная свеча + открытие Азии): снимаю все заявки и закрываю все позиции лимитками по лучшей цене" : "▶ Пауза закончилась — снова ставлю сетки", "info"); }
      const sellOK = !usP && !(CFG.btcOnly && !btc.down);
      const buyOK = !usP && !!CFG.longOn && isLive() && !(CFG.longBtc && btc.down);   // лонги не ловим, пока BTC в тренде вниз
      if (CFG.btcOnly && !btc.down) {
        if (BTC.blocked !== true) log(`₿ Фильтр BTC: ${btc.why} — новые SELL не ставлю${buyOK ? ", только лонги" : ""} (открытые позиции веду)`, "info");
        BTC.blocked = true;
      } else if (CFG.btcOnly && BTC.blocked !== false) { log(`₿ Фильтр BTC: ${btc.why} — ставлю SELL`, "ok"); BTC.blocked = false; }
      if (CFG.longOn && isLive() && BTC.longBlocked !== !buyOK) { BTC.longBlocked = !buyOK; log(buyOK ? `₿ ${btc.why} — лонги на дампах разрешены` : `₿ ${btc.why} — новые лонги не открываю (BTC вниз), шорты работают`, "info"); }
      // сторона монеты: растёт на минутной свече -> SELL сверху, падает -> BUY снизу; активная монета без позиции сторону не меняет
      // 📉 фильтр OI: SELL на монете только когда её открытый интерес падает (за 15 мин упал хотя бы на oiPct %) —
      // памп на закрытии шортов / выходе из позиций, а не на новых деньгах. OI растёт или нет данных -> SELL не ставлю.
      const oiOK = (x) => !CFG.oiOn || (x.oi != null && x.oi < 0 && x.oi <= -Math.max(0, CFG.oiPct || 0));
      let oiBlocked = [], pumpBlocked = [];
      const pumpP = Math.max(0, CFG.pumpPct || 0);   // 🚀 фильтр пампа: монета за 1 ч выросла на pumpPct % и больше -> SELL не ставлю (упала на столько же -> BUY не ставлю)
      for (const x of flat) {
        const c = coin(x.sym), want = x.move >= 0 ? "Sell" : "Buy", pumpUp = pumpP > 0 && x.ch1h >= pumpP, dumpDn = pumpP > 0 && x.ch1h <= -pumpP;
        if (pumpUp || dumpDn) pumpBlocked.push(x.sym.replace(/USDT$/, "") + ` ${x.ch1h >= 0 ? "+" : ""}${x.ch1h.toFixed(1)}%`);
        const sOK = sellOK && oiOK(x) && !pumpUp, bOK = buyOK && !dumpDn;
        if (sellOK && !sOK && want === "Sell") oiBlocked.push(x.sym.replace(/USDT$/, "") + (x.oi == null ? " (нет OI)" : ` OI ${x.oi >= 0 ? "+" : ""}${x.oi.toFixed(2)}%`));
        const keepSide = ACTIVE.includes(x.sym) && c.side && (c.side === "Sell" ? sOK : bOK);
        x.side = keepSide ? c.side : (want === "Sell" ? (sOK ? "Sell" : null) : (bOK ? "Buy" : sOK ? "Sell" : null));   // памп с растущим OI -> ни SELL, ни BUY
      }
      if (CFG.oiOn) { const key = oiBlocked.slice(0, 8).join(", "); if (key !== (BUD.oiLogged ?? "")) { BUD.oiLogged = key; if (key) log(`📉 Фильтр OI: открытый интерес не падает — SELL не ставлю: ${key}${oiBlocked.length > 8 ? " …" : ""}`, "info"); } }
      { const key = pumpBlocked.slice(0, 8).join(", "); if (key !== (BUD.pumpLogged ?? "")) { BUD.pumpLogged = key; if (key) log(`🚀 Фильтр пампа: за 1 ч ±${pumpP}% и больше — против движения не захожу: ${key}${pumpBlocked.length > 8 ? " …" : ""}`, "info"); } }
      for (let n = flat.length - 1; n >= 0; n--) if (!flat[n].side) flat.splice(n, 1);
      K = Math.max(0, Math.min(K, flat.length));
      const scale = K ? Math.max(1, B.free / (K * B.perCoinBase)) : 1;
      B.k = K; B.lot = +(CFG.usdt * scale).toFixed(2); B.perCoin = B.perCoinBase * scale;
      for (const x of flat) coin(x.sym).side = x.side;
      const keep = new Set(flat.slice(0, K + 1).map(x => x.sym));
      const next = flat.filter(x => ACTIVE.includes(x.sym) && keep.has(x.sym)).map(x => x.sym).slice(0, K);
      for (const x of flat.slice(0, K)) if (next.length < K && !next.includes(x.sym)) next.push(x.sym);
      if (!B.kLogged || B.k !== (BUD.prevK ?? -1) || Math.abs(B.lot / (BUD.prevLot || 1) - 1) > 0.1) log(`Бюджет: депозит ${B.eq.toFixed(2)} USDT · ${B.foreign != null && B.cap > B.capOwn + 0.01 ? `до ${CFG.fillPct}% счёта (другой бот держит ${B.foreign.toFixed(2)}) = ${B.cap.toFixed(2)}` : `${CFG.budgetPct}% = ${B.cap.toFixed(2)}`} · шорты ${B.posIM.toFixed(2)} + доборы ${B.addRes.toFixed(2)} · свободно ${Math.max(0, B.free).toFixed(2)} → сетки на ${K} монет по ${CFG.levels} заявок × ${B.lot} USDT (резерв на монету с доборами ${B.perCoin.toFixed(2)})${scale > 1.001 ? " (лот увеличен, чтобы занять " + CFG.budgetPct + "%)" : ""}`, "info");
      BUD.prevK = K; BUD.prevLot = B.lot; BUD.kLogged = true;
      const added = next.filter(s => !ACTIVE.includes(s)), removed = ACTIVE.filter(s => !next.includes(s));
      ACTIVE = next;
      CAND = out.filter(x => !x.held || ACTIVE.includes(x.sym)).slice(0, 15).map(x => ({ sym: x.sym, rPrev: x.rPrev, rCur: x.rCur, move: x.move, turn: x.turn, oi: x.oi, active: ACTIVE.includes(x.sym) }));
      for (const x of out) if (ACTIVE.includes(x.sym) || hasPos(x.sym)) { const c = coin(x.sym); c.range = x.range; c.last = x.last; c.anchor = x.anchor; }   // the grid anchor = the price delay min ago
      STAT.scans++; lastScanAt = now(); saveStat();
      jlog("scan", { btc: { down: btc.down, ch30: btc.ch30, e9: btc.e9, e21: btc.e21 }, btcBlock: !!(CFG.btcOnly && !btc.down), oiOn: !!CFG.oiOn, oiBlocked: oiBlocked.length, eq: +B.eq.toFixed(2), free: +B.free.toFixed(2), k: B.k, lot: B.lot, active: ACTIVE, held: Object.keys(C).filter(hasPos), sides: Object.fromEntries(ACTIVE.map(s => [s, sideOf(coin(s))])),
        top: out.slice(0, 10).map(x => [x.sym, +x.range.toFixed(2), +x.move.toFixed(2)]) });
      if (added.length || removed.length) log(`Скан: ${ACTIVE.length} монет на ${CFG.budgetPct}% депозита → ${ACTIVE.map(s => s.replace(/USDT$/, "") + (sideOf(coin(s)) === "Buy" ? "↑" : "↓")).join(", ")}${added.length ? " · + " + added.join(", ") : ""}${removed.length ? " · − " + removed.join(", ") : ""}`, "ok");
      for (const s of removed) if (!hasPos(s)) await dropGrid(s);   // a coin with an open short keeps its grid (the adds)
      for (const s of [...gridCoins()].sort((a, b) => hasPos(b) - hasPos(a))) { try { await gridFor(s); } catch (e) { log(`${s}: сетка: ${e.message}`, "err"); } }   // сначала доборы открытых позиций, потом сетки входа
    } catch (e) { log("Скан: " + e.message, "err"); }
    finally { scanning = false; }
  }

  // ---------------- 2) flat: the SELL entry grid; a short: adds every addStep % above the last SELL fill ----------------
  async function gridFor(sym) {
    const c = coin(sym), i = await inst(sym);
    if (!c.anchor) return;
    const qty = qtyFor(i, c.anchor), side = sideOf(c), d = sg(side), rm = side === "Buy" ? "down" : "up";
    c.grid = []; for (let k = 1; k <= CFG.levels; k++) c.grid.push(+rnd(c.anchor * (1 + d * CFG.step * k / 100), i.tick, rm));
    if (!isLive()) return;   // the simulation fills the levels from the price
    OURS.add(sym); saveOurs();
    if (!c.lev) { try { await post("/v5/position/set-leverage", { category: "linear", symbol: sym, buyLeverage: String(CFG.lev), sellLeverage: String(CFG.lev) }); } catch (e) { if (e.code !== 110043) log(`${sym}: плечо: ${e.message}`, "err"); } c.lev = true; }
    if (c.hedge == null) { try { const r = await priv("GET", "/v5/position/list", { category: "linear", symbol: sym }); c.hedge = (r.list || []).some(p => +p.positionIdx > 0); } catch (e) { c.hedge = false; } }
    const posQty = c.pos && c.pos.side === side ? c.pos.size : 0, room = Math.max(0, CFG.maxPos - Math.round(posQty / qty));
    let want, wantQty = null;
    if (posQty > 0) {
      const base = side === "Buy" ? Math.min(c.lastAdd || Infinity, c.pos.avg) : Math.max(c.lastAdd || 0, c.pos.avg);
      const addsDone = Math.max(0, ((TRADES[sym] && TRADES[sym].fills) || 1) - 1), addsLeft = Math.max(0, Math.round(CFG.maxAdds) - addsDone);
      const cap = capPx(c);
      want = []; wantQty = []; for (let k = 1; k <= Math.min(CFG.levels, room, addsLeft); k++) { const px = +rnd(base * (1 + d * CFG.addStep * k / 100), i.tick, rm); if (cap != null && d * (px - cap) >= 0) break; want.push(px); wantQty.push(qtyFor(i, px, addK(addsDone + k), tradeLot(sym))); }   // добор за стопом по лимиту минуса не ставлю
      if (!addsLeft && !c.addsLogged) { c.addsLogged = true; log(`${sym}: сделано ${addsDone} доборов из ${CFG.maxAdds} (${word(side)}) — новых заявок нет, жду тейк`, "info"); } else if (addsLeft) c.addsLogged = false;
      c.grid = want;
    } else want = c.grid.slice(0, Math.min(CFG.levels, room));
    if (BTC.usPaused) { want = []; wantQty = []; }   // 🔔 открытие США: снимаю все заявки входа и доборов (тейк и стоп позиций остаются)
    if (posQty && !room && !c.fullLogged) { c.fullLogged = true; log(`${sym}: позиция набрала ${CFG.maxPos} заявок — новых заявок нет, жду тейк`, "info"); } else if (room) c.fullLogged = false;
    const all = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_e"));
    for (const o of all) if (o.side !== side) await cancelOrder(sym, o);   // сторона сменилась — старые заявки другой стороны снимаем
    const open = all.filter(o => o.side === side).sort((a, b) => d * (+a.price - +b.price));   // ближние к цене первыми
    const tick = +i.tick;
    for (let k = 0; k < Math.max(want.length, open.length); k++) {
      const o = open[k], px = want[k], q = wantQty ? wantQty[k] : qty;
      if (o && px == null) { await cancelOrder(sym, o); continue; }
      if (!o) { if (!(await placeEntry(sym, px, q, c)) && posQty > 0 && (await makeRoom(sym))) await placeEntry(sym, px, q, c); continue; }
      const qtyOff = Math.abs(+o.qty / q - 1) > 0.1 && !(+o.cumExecQty > 0);   // the lot changed a lot -> the order follows it
      if (Math.abs(+o.price - px) >= tick / 2 || qtyOff) {
        try { await post("/v5/order/amend", Object.assign({ category: "linear", symbol: sym, orderId: o.orderId, price: String(px) }, qtyOff ? { qty: String(q) } : {})); }
        catch (e) { await cancelOrder(sym, o); await placeEntry(sym, px, q, c); }
      }
    }
  }
  async function placeEntry(sym, px, qty, c) {
    const side = sideOf(c);
    const send = () => post("/v5/order/create", { category: "linear", symbol: sym, side, orderType: "Limit", qty: String(qty), price: String(px), timeInForce: "GTC", positionIdx: c.hedge ? (side === "Buy" ? 1 : 2) : 0, orderLinkId: "vg_e" + now() + Math.floor(Math.random() * 1e4) });
    try {
      try { await send(); }
      catch (e) {
        if (String(e.code) !== "10001" || !/position idx/i.test(e.message || "")) throw e;
        c.hedge = !c.hedge;   // режим позиций на бирже другой (Hedge / One-way) — подстраиваемся и повторяем
        log(`${sym}: режим позиций на бирже — ${c.hedge ? "Hedge" : "односторонний"}, подстроился`, "info");
        await send();
      }
      return true;
    }
    catch (e) {
      if (/^(110004|110007|110012|110044|110045|110052)$/.test(String(e.code)) || /not enough|insufficient/i.test(e.message)) { if (hasPos(sym)) return false; if (now() - (noMoneyAt || 0) > 60000) { noMoneyAt = now(); log(`Маржи на новые заявки не хватает — ${CFG.budgetPct}% депозита уже в работе, жду`, "info"); } }
      else log(`${sym}: ${side === "Buy" ? "BUY" : "SELL"} ${qty} @ ${px}: ${e.message}`, "err");
      return false;
    }
  }
  // добору не хватило маржи: доборы важнее новых входов — снимаем сетку входа с наименее волатильной монеты без позиции
  // 💸 на добор нет маржи и снимать нечего: закрываю маркетом позицию, которая «светится нулём» (PnL ≥ −0.2 %, лучше в плюсе),
  // и отдаю освободившуюся маржу на добор против пампа — резерв под доборы зря не лежит
  const FREE_PCT = 0.2;
  let freeBusyAt = 0;
  async function freeFromFlat(sym) {
    const cand = Object.values(C).filter(c => c.sym !== sym && c.pos && c.pos.size > 0 && OURS.has(c.sym) && TRADES[c.sym] && c.pos.avg > 0)
      .map(c => ({ c, pct: c.pos.upnl / (c.pos.size * c.pos.avg) * 100 })).filter(x => x.pct >= -FREE_PCT).sort((a, b) => b.pct - a.pct);
    if (!cand.length || now() - freeBusyAt < 5000) { log(`${sym}: ⚠ на добор не хватает маржи, а снимать уже нечего (нет позиций около нуля) — уменьшите лот / «% депозита» / число доборов`, "err"); return false; }
    const { c, pct } = cand[0], L = c.pos.side === "Buy";
    freeBusyAt = now();
    try {
      await post("/v5/order/create", { category: "linear", symbol: c.sym, side: L ? "Sell" : "Buy", orderType: "Market", qty: String(c.pos.size), reduceOnly: true, positionIdx: c.pos.idx, orderLinkId: "vg_fr" + now() + Math.floor(Math.random() * 1e4) });
    } catch (e) { log(`${sym}: не смог закрыть ${c.sym} под добор: ${e.message}`, "err"); return false; }
    if (TRADES[c.sym]) { TRADES[c.sym].freeExit = true; saveTrades(); }   // закрыто ради маржи, а не стопом — без кулдауна
    ACTIVE = ACTIVE.filter(s => s !== c.sym);
    log(`${sym}: 💸 на добор не хватало маржи — закрыл ${word(c.pos.side)} ${c.sym} около нуля (${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%, ${c.pos.upnl >= 0 ? "+" : ""}${c.pos.upnl.toFixed(2)} USDT), маржу отдаю на добор`, "ok");
    await sleep(1500);
    return true;
  }
  async function makeRoom(sym) {
    const victim = [...ACTIVE].reverse().find(s => s !== sym && !hasPos(s));
    if (!victim) return await freeFromFlat(sym);
    ACTIVE = ACTIVE.filter(s => s !== victim); await dropGrid(victim);
    log(`${sym}: на добор не хватало маржи — снял сетку входа ${victim}, ставлю добор`, "info");
    return true;
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
      if (id.startsWith("vg_e")) {
        const c = coin(x.symbol), buy = x.side === "Buy"; STAT.fills++;
        c.lastAdd = buy ? Math.min(c.lastAdd || Infinity, +x.execPrice) : Math.max(c.lastAdd || 0, +x.execPrice); LAST_ADD[x.symbol] = c.lastAdd; saveLastAdd(); touched.add(x.symbol);
        if (!hasPos(x.symbol)) c.side = x.side;
        log(`${x.symbol}: ${buy ? "BUY (лонг)" : "SELL (шорт)"} ${x.execQty} исполнен @ ${x.execPrice}${hasPos(x.symbol) ? " — добор" : ""} → следующий добор ${buy ? "−" : "+"}${CFG.addStep}%`, "ok");
        const tr = TRADES[x.symbol] || (TRADES[x.symbol] = { sym: x.symbol, side: x.side, open: now(), first: +x.execPrice, fills: 0, qty: 0, maxPx: +x.execPrice, minPx: +x.execPrice, range: +(c.range || 0).toFixed(2), btcDown: BTC.down, lot: lot(), cfg: { step: CFG.step, addStep: CFG.addStep, take: CFG.take, trailGap: CFG.trailGap, lev: CFG.lev, levels: CFG.levels } });
        // one order filled in several parts is still ONE fill (otherwise partial fills eat the adds)
        tr.ords = tr.ords || [];
        if (!tr.ords.includes(x.orderId)) { tr.ords.push(x.orderId); tr.fills++; }
        tr.qty += +x.execQty; saveTrades();
        jlog("fill", { sym: x.symbol, px: +x.execPrice, qty: +x.execQty, n: tr.fills, add: tr.fills > 1 });
      } else if (id.startsWith("vg_tu") && +x.leavesQty === 0) { log(`${x.symbol}: ⏸ позиция закрыта лимиткой на паузе @ ${x.execPrice}`, "ok"); if (TRADES[x.symbol]) { TRADES[x.symbol].exitPx = +x.execPrice; TRADES[x.symbol].usExit = true; saveTrades(); } }
      else if (id.startsWith("vg_t") && +x.leavesQty === 0) { STAT.takes++; coin(x.symbol).takes++; log(`${x.symbol}: ✅ тейк исполнен @ ${x.execPrice} — ${word((TRADES[x.symbol] || {}).side || (x.side === "Sell" ? "Buy" : "Sell"))} закрыт в плюс`, "ok"); if (TRADES[x.symbol]) { TRADES[x.symbol].exitPx = +x.execPrice; saveTrades(); } }
    }
    if (SEEN.size > 5000) { const a = [...SEEN].slice(-2000); SEEN.clear(); a.forEach(v => SEEN.add(v)); }
    saveStat();
    return touched;
  }
  async function syncTake(sym, c, i) {
    const L = c.pos.side === "Buy", px = rnd(c.pos.avg * (1 + (L ? 1 : -1) * CFG.take / 100), i.tick, L ? "up" : "down"), qty = rnd(c.pos.size, i.step, "down"), key = px + "|" + qty;
    if (c.takeKey === key) return;
    const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_t"));
    for (const o of open.slice(1)) await cancelOrder(sym, o);
    const o = open[0];
    try {
      if (o) await post("/v5/order/amend", { category: "linear", symbol: sym, orderId: o.orderId, price: px, qty });
      else await post("/v5/order/create", { category: "linear", symbol: sym, side: L ? "Sell" : "Buy", orderType: "Limit", qty, price: px, timeInForce: "GTC", reduceOnly: true, positionIdx: c.pos.idx, orderLinkId: "vg_t" + now() + Math.floor(Math.random() * 1e4) });
      c.takeKey = key; log(`${sym}: тейк на всю позицию: ${L ? "SELL" : "BUY"} ${qty} @ ${px} (${L ? "+" : "−"}${CFG.take}% от средней ${fmt(c.pos.avg)})`, "info");
    } catch (e) {
      if (o) { await cancelOrder(sym, o); c.takeKey = ""; }
      log(`${sym}: тейк: ${e.message}`, "err");
    }
  }
  // the trailing take: the price reached −take % from the average -> it follows the lowest price; a bounce of trailGap % from
  // the low -> out. The exit is a CONDITIONAL LIMIT on the exchange (trigger = the trail level, rising), never worse than −take %
  // from the average - always in profit, never market. While the move goes on the level goes down after it (the whole move).
  // 🔔 пауза на открытие США: позицию закрываю лимиткой-мейкером по лучшей цене стакана (PostOnly), переставляю за ценой, пока не закроется
  async function flatExit(sym, c, i) {
    const L = c.pos.side === "Buy", exitSide = L ? "Sell" : "Buy", qty = rnd(c.pos.size, i.step, "down");
    const t = TRAIL[sym]; if (t) { if (t.orderId) await cancelOrder(sym, { orderId: t.orderId }); if (t.exitId) await cancelOrder(sym, { orderId: t.exitId }); delete TRAIL[sym]; saveTrail(); }
    let ob; try { ob = await pub("/v5/market/orderbook", { category: "linear", symbol: sym, limit: 1 }); } catch (e) { return; }
    const best = L ? +((ob.a || [])[0] || [])[0] : +((ob.b || [])[0] || [])[0]; if (!(best > 0)) return;
    const px = rnd(best, i.tick, L ? "up" : "down"), key = "us|" + px + "|" + qty;
    if (c.takeKey === key) return;
    const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_t"));
    const mine = open.find(o => (o.orderLinkId || "").startsWith("vg_tu") && !(+o.triggerPrice > 0));
    for (const o of open) if (o !== mine) await cancelOrder(sym, o);
    try {
      if (mine) await post("/v5/order/amend", { category: "linear", symbol: sym, orderId: mine.orderId, price: px, qty });
      else await post("/v5/order/create", { category: "linear", symbol: sym, side: exitSide, orderType: "Limit", qty, price: px, timeInForce: "PostOnly", reduceOnly: true, positionIdx: c.pos.idx, orderLinkId: "vg_tu" + now() + Math.floor(Math.random() * 1e4) });
      if (!String(c.takeKey || "").startsWith("us|")) log(`${sym}: ${BTC.pauseKind === "night" ? "🌙 ночная пауза" : "🔔 открытие США"} — закрываю ${word(c.pos.side)} лимиткой ${exitSide.toUpperCase()} ${qty} @ ${px} (средняя ${fmt(c.pos.avg)}), веду за ценой`, "info");
      c.takeKey = key;
    } catch (e) { c.takeKey = ""; if (mine) await cancelOrder(sym, mine); }
  }
  async function syncExit(sym, c, i) {
    if (BTC.usPaused) return flatExit(sym, c, i);
    if (String(c.takeKey || "").startsWith("us|")) {   // пауза кончилась, а позиция осталась — убираю лимитку паузы, дальше обычный выход
      const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_tu"));
      for (const o of open) await cancelOrder(sym, o);
      c.takeKey = "";
    }
    if (!CFG.trailOn) return syncTake(sym, c, i);
    if (c.takeKey !== "trail") {   // a plain take would close at the take level at once - it goes
      const open = ((await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym })).list || []).filter(o => (o.orderLinkId || "").startsWith("vg_t"));
      for (const o of open) await cancelOrder(sym, o);
      c.takeKey = "trail";
    }
    // шорт: тейк ниже средней, ведём за МИНИМУМОМ, выход BUY на отскоке вверх;
    // лонг: тейк выше средней, ведём за МАКСИМУМОМ, выход SELL на откате вниз. Всегда в плюс, никогда маркетом.
    const L = c.pos.side === "Buy", d = L ? -1 : 1, exitSide = L ? "Sell" : "Buy";
    const avg = c.pos.avg, act = avg * (1 - d * CFG.take / 100), p = c.lp || c.last, tick = +i.tick;
    let t = TRAIL[sym];
    if (!t || Math.abs(t.avg - avg) > tick / 2 || (t.side && t.side !== c.pos.side)) {   // a new position, or an add moved the average: the trailing starts again
      if (t && t.orderId) await cancelOrder(sym, { orderId: t.orderId });
      if (t && t.exitId) await cancelOrder(sym, { orderId: t.exitId });
      t = TRAIL[sym] = { avg, side: c.pos.side, on: false, best: 0, level: 0, orderId: null, exitId: null, qty: 0 }; saveTrail();
    }
    if (!p) return;
    const better = (a, b) => (L ? a > b : a < b);   // a лучше b для позиции
    if (!t.on) {
      if (L ? p < act : p > act) return;
      t.on = true; t.best = p; jlog("trail", { sym, px: p, avg, side: c.pos.side });
      log(`${sym}: 🎯 трейлинг тейка (${word(c.pos.side)}) включён: цена ${fmt(p)} ${L ? "≥ +" : "≤ −"}${CFG.take}% от средней ${fmt(avg)} — веду за ${L ? "максимумом" : "минимумом"}, откат ${CFG.trailGap}% → выход`, "ok");
    }
    if (better(p, t.best)) t.best = p;
    const level = L ? +rnd(Math.max(t.best * (1 - CFG.trailGap / 100), act), i.tick, "up")
                    : +rnd(Math.min(t.best * (1 + CFG.trailGap / 100), act), i.tick, "down");
    const lim = L ? rnd(Math.max(level * 0.999, avg * (1 + CFG.take / 200)), i.tick, "down")
                  : rnd(Math.min(level * 1.001, avg * (1 - CFG.take / 200)), i.tick, "up");   // the limit after the trigger: still in profit
    const qty = rnd(c.pos.size, i.step, "down");
    const bounced = L ? p <= level : p >= level;
    try {
      if (t.exitId) return;   // the exit limit is already on the book (in profit) - nothing more to do
      if (bounced && !t.orderId) {   // bounced before the conditional order was set: one plain limit, in profit
        const r = await post("/v5/order/create", { category: "linear", symbol: sym, side: exitSide, orderType: "Limit", qty, price: lim, timeInForce: "GTC", reduceOnly: true, positionIdx: c.pos.idx, orderLinkId: "vg_t" + now() + Math.floor(Math.random() * 1e4) });
        t.exitId = r.orderId; log(`${sym}: 💰 трейлинг: откат от ${L ? "максимума" : "минимума"} ${fmt(t.best)} — выход лимиткой ${exitSide.toUpperCase()} ${qty} @ ${lim} (средняя ${fmt(avg)}, в плюс)`, "ok");
      } else if (bounced) {
        /* the conditional order on the exchange triggers by itself at the level */
      } else if (!t.orderId) {
        const r = await post("/v5/order/create", { category: "linear", symbol: sym, side: exitSide, orderType: "Limit", qty, price: lim, triggerPrice: String(level), triggerDirection: L ? 2 : 1, triggerBy: "LastPrice", timeInForce: "GTC", reduceOnly: true, positionIdx: c.pos.idx, orderLinkId: "vg_t" + now() + Math.floor(Math.random() * 1e4) });
        t.orderId = r.orderId; t.level = level; t.qty = qty;
        log(`${sym}: трейлинг-выход на бирже: при откате до ${level} → ${exitSide.toUpperCase()} ${qty} лимитом @ ${lim}`, "info");
      } else if ((L ? level > t.level + tick / 2 : level < t.level - tick / 2) || qty !== t.qty) {   // the level only follows the price in the profit direction
        try { await post("/v5/order/amend", { category: "linear", symbol: sym, orderId: t.orderId, triggerPrice: String(level), price: lim, qty }); t.level = level; t.qty = qty; }
        catch (e) { t.orderId = null; }   // triggered / gone - set again on the next tick
      }
    } catch (e) { log(`${sym}: трейлинг-выход: ${e.message}`, "err"); }
    saveTrail();
  }
  async function clearStops(sym, idx) {
    try { const t = (await priv("GET", "/v5/order/realtime", { category: "linear", symbol: sym, orderFilter: "tpslOrder" })).list || [];
      for (const o of t) if (/StopLoss|TrailingStop/i.test(o.stopOrderType || "")) await cancelOrder(sym, o); } catch (e) { /* none */ }
    try { await post("/v5/position/trading-stop", { category: "linear", symbol: sym, tpslMode: "Full", stopLoss: "0", trailingStop: "0", positionIdx: idx }); } catch (e) { /* none */ }
  }
  // the emergency stop: a market stop-loss on the exchange at +stopPct % above the average (moves after every add); 0 = no stop
  // 🛡 макс. минус на позицию: цена, при которой позиция (с комиссиями) теряет ровно maxLossUsd USDT
  function capPx(c) {
    if (!(CFG.maxLossUsd > 0) || !c.pos || !(c.pos.size > 0)) return null;
    const L = c.pos.side === "Buy", d = L ? -1 : 1, fee = 0.0011 * c.pos.size * c.pos.avg;
    const dist = Math.max(c.pos.avg * 0.003, (CFG.maxLossUsd - fee) / c.pos.size);
    return c.pos.avg + d * dist;
  }
  async function syncStop(sym, c, i) {
    if (!(CFG.stopPct > 0) && !(CFG.maxLossUsd > 0)) {
      if (c.stopKey !== "none") { await clearStops(sym, c.pos.idx); c.stopKey = "none"; log(`${sym}: стопа нет — в минус не выхожу; на росте до ${CFG.maxAdds} доборов каждые ${CFG.addStep}%, выход только тейком в плюс`, "info"); }
      return;
    }
    // the stop sits stopPct % ABOVE THE LAST PLANNED ADD, so it never fires before all the adds are done
    const L = c.pos.side === "Buy", d = L ? -1 : 1;
    const base = L ? Math.min(c.lastAdd || Infinity, c.pos.avg) : Math.max(c.lastAdd || 0, c.pos.avg);
    const done = Math.max(0, ((TRADES[sym] && TRADES[sym].fills) || 1) - 1), left = Math.max(0, Math.round(CFG.maxAdds) - done);
    const lastAddPx = base * (1 + d * CFG.addStep * left / 100);
    let sl = CFG.stopPct > 0 ? rnd(lastAddPx * (1 + d * CFG.stopPct / 100), i.tick, L ? "down" : "up") : null, why = "";
    const cap = capPx(c), mark = c.lp || c.last || c.pos.avg;
    if (cap != null && (sl == null || d * (cap - +sl) < 0)) {
      if (d * (cap - mark) > 0) { sl = rnd(cap, i.tick, L ? "up" : "down"); why = `макс. минус ${CFG.maxLossUsd} USDT`; }
      else if (!c.capLogged) { c.capLogged = true; log(`${sym}: 🛡 минус уже больше ${CFG.maxLossUsd} USDT — стоп по лимиту поставить нельзя, веду по обычному стопу`, "err"); }
    }
    if (sl == null) return;
    if (c.stopKey === sl) return;
    try {
      await post("/v5/position/trading-stop", { category: "linear", symbol: sym, tpslMode: "Full", stopLoss: sl, slTriggerBy: "MarkPrice", slOrderType: "Market", positionIdx: c.pos.idx });
      c.stopKey = sl; if (why) { log(`${sym}: 🛑 стоп ${sl} (${word(c.pos.side)}) — ${why}, средняя ${fmt(c.pos.avg)}, маркет на бирже`, "info"); return; }
      log(`${sym}: 🛑 стоп ${sl} (${word(c.pos.side)}) — на ${CFG.stopPct}% ${L ? "ниже" : "выше"} ${left ? "последнего добора " + fmt(lastAddPx) + " (осталось доборов: " + left + ")" : "последнего добора " + fmt(base) + " (все доборы сделаны)"}, средняя ${fmt(c.pos.avg)}, маркет на бирже`, "info");
    } catch (e) {
      if (e.code === 34040) { c.stopKey = sl; return; }   // not modified: already there
      log(`${sym}: стоп: ${e.message}`, "err");
    }
  }
  // ---------------- 4) the positions: the take always set, plus the emergency stop ----------------
  async function pollPositions() {
    const r = await priv("GET", "/v5/position/list", { category: "linear", settleCoin: "USDT" });
    const seen = new Set();
    for (const p of r.list || []) {
      if (!(+p.size > 0)) continue;
      const sym = p.symbol; seen.add(sym);
      const c = coin(sym); c.pos = { side: p.side, size: +p.size, avg: +p.avgPrice, upnl: +p.unrealisedPnl || 0, idx: +p.positionIdx || 0 }; c.last = +p.markPrice || c.last;
      if (!OURS.has(sym)) continue;
      if (p.side === "Buy" && !(TRADES[sym] && TRADES[sym].side === "Buy")) continue;   // лонг ведём, только если его открыл этот бот
      const i = await inst(sym);
      if (!c.anchor) c.anchor = c.pos.avg;
      await syncExit(sym, c, i);
      await syncStop(sym, c, i);
    }
    for (const s of Object.keys(C)) if (!seen.has(s) && C[s].pos) {
      if (C[s].pos.size > 0 && OURS.has(s)) log(`${s}: позиция (${word(C[s].pos.side)}) закрыта`, "info");
      if (TRAIL[s] && TRAIL[s].orderId) await cancelOrder(s, { orderId: TRAIL[s].orderId });
      if (TRAIL[s] && TRAIL[s].exitId) await cancelOrder(s, { orderId: TRAIL[s].exitId });
      await closeTrade(s, C[s].pos, null);
      C[s].pos = null; C[s].capLogged = false; C[s].stopKey = ""; C[s].takeKey = ""; C[s].lastAdd = 0; delete LAST_ADD[s]; saveLastAdd(); delete TRAIL[s]; saveTrail();
      if (!ACTIVE.includes(s)) await dropGrid(s);   // it was kept only for the adds
    }
  }

  // a trade is over: everything about it goes into the journal (the PnL from Bybit, net of fees)
  async function closeTrade(sym, pos, simPnl) {
    const tr = TRADES[sym]; if (!tr) return;
    let pnl = simPnl;
    if (pnl == null && isLive()) { try { const r = await priv("GET", "/v5/position/closed-pnl", { category: "linear", symbol: sym, startTime: tr.open - 60000, limit: 50 }); pnl = (r.list || []).reduce((a, x) => a + (+x.closedPnl || 0), 0); } catch (e) { pnl = null; } }
    const avg = pos ? pos.avg : tr.first, hold = (now() - tr.open) / 60000;
    if (pnl != null && +pnl < 0 && !tr.usExit && !tr.freeExit && CFG.coolMin > 0) { COOL[sym] = now() + CFG.coolMin * 60000; saveCool(); log(`${sym}: 🧊 закрыто в минус ${(+pnl).toFixed(2)} USDT (стоп) — ${CFG.coolMin} мин не торгую эту монету`, "err"); }
    jlog("trade", Object.assign({}, tr, { close: now(), holdMin: +hold.toFixed(1), avg, adverse: +((tr.side === "Buy" ? 1 - tr.minPx / avg : tr.maxPx / avg - 1) * 100).toFixed(2), best: +((tr.side === "Buy" ? tr.maxPx / avg - 1 : 1 - tr.minPx / avg) * 100).toFixed(2), pnl: pnl == null ? null : +(+pnl).toFixed(4) }));
    delete TRADES[sym]; saveTrades();
  }
  function trackTrades() { for (const s of Object.keys(TRADES)) { const c = C[s], p = c && (c.lp || c.last); if (p) { TRADES[s].maxPx = Math.max(TRADES[s].maxPx, p); TRADES[s].minPx = Math.min(TRADES[s].minPx, p); } } }

  // ---------------- the simulation (Live off): the same rules on the live prices ----------------
  async function simTick() {
    const t = await pub("/v5/market/tickers", { category: "linear" });
    const px = {}; for (const x of t.list || []) px[x.symbol] = +x.lastPrice;
    for (const s of Object.keys(C)) {
      const c = C[s], p = px[s]; if (!p) continue; c.last = p; c.lp = p;
      const i = INST[s]; if (!i) continue;
      const fills = c.sim.fills;
      if (!fills.length) {
        if (ACTIVE.includes(s)) for (const lv of c.grid) if (p >= lv && fills.length < CFG.maxPos && !fills.some(o => Math.abs(o.px - lv) < +i.tick / 2)) { fills.push({ px: lv, qty: qtyFor(i, lv) }); STAT.fills++; log(`[СИМ] ${s}: SELL @ ${lv}`, "ok");
          if (!TRADES[s]) TRADES[s] = { sym: s, open: now(), first: lv, fills: 0, qty: 0, maxPx: p, minPx: p, range: +(c.range || 0).toFixed(2), btcDown: BTC.down, lot: lot(), cfg: { step: CFG.step, addStep: CFG.addStep, take: CFG.take, trailGap: CFG.trailGap, lev: CFG.lev, levels: CFG.levels } };
          TRADES[s].fills++; TRADES[s].qty += fills[fills.length - 1].qty; jlog("fill", { sym: s, px: lv, n: TRADES[s].fills, add: false }); }
      } else {
        let top = Math.max(...fills.map(o => o.px));
        while (fills.length < CFG.maxPos && fills.length - 1 < Math.round(CFG.maxAdds) && p >= top * (1 + CFG.addStep / 100)) { const lv = +rnd(top * (1 + CFG.addStep / 100), i.tick, "up"); fills.push({ px: lv, qty: qtyFor(i, lv, addK(fills.length)) }); STAT.fills++; top = lv; log(`[СИМ] ${s}: добор №${fills.length - 1} SELL @ ${lv} (+${CFG.addStep}% от прошлого)`, "ok"); if (TRADES[s]) { TRADES[s].fills++; jlog("fill", { sym: s, px: lv, n: TRADES[s].fills, add: true }); } }
        c.grid = []; for (let k = 1; k <= Math.min(CFG.levels, CFG.maxPos - fills.length, Math.max(0, Math.round(CFG.maxAdds) - (fills.length - 1))); k++) c.grid.push(+rnd(top * (1 + CFG.addStep * k / 100), i.tick, "up"));
      }
      if (fills.length) {
        const size = fills.reduce((a, o) => a + o.qty, 0), avg = fills.reduce((a, o) => a + o.px * o.qty, 0) / size, take = avg * (1 - CFG.take / 100);
        c.pos = { side: "Sell", size, avg, upnl: (avg - p) * size, sim: true };
        const close = (px, how) => { const pnl = (avg - px) * size - (avg + px) * size * 0.0004; if (TRADES[s]) TRADES[s].exitPx = px; closeTrade(s, { avg }, pnl); STAT.simPnl += pnl; STAT.takes++; c.takes++; c.sim.fills = []; c.sim.trail = null; c.pos = null; log(`[СИМ] ${s}: ✅ ${how} @ ${rnd(px, i.tick)} на всю позицию (${fills.length} заявок, средняя ${fmt(avg)}) PnL +${pnl.toFixed(4)}`, "ok"); };
        if (!CFG.trailOn) { if (p <= take) close(take, "тейк"); }
        else {
          let t = c.sim.trail; if (!t || Math.abs(t.avg - avg) > +i.tick / 2) t = c.sim.trail = { avg, on: false, best: 0 };
          if (!t.on && p <= take) { t.on = true; t.best = p; jlog("trail", { sym: s, px: p, avg }); log(`[СИМ] ${s}: 🎯 трейлинг тейка включён @ ${fmt(p)}`, "info"); }
          if (t.on) {
            t.best = Math.min(t.best, p); const level = Math.min(t.best * (1 + CFG.trailGap / 100), take); c.pos.trail = level;
            if (p >= level) close(level, `трейлинг-тейк (минимум ${fmt(t.best)}, откат ${CFG.trailGap}%)`);
          }
        }
      } else c.pos = null;
    }
    saveStat();
  }

  async function lastPrices() {
    const t = await pub("/v5/market/tickers", { category: "linear" });
    for (const x of t.list || []) if (C[x.symbol]) C[x.symbol].lp = +x.lastPrice;
  }
  // the trailing grid: the price went down from the entry grid -> it follows the price down at once (up only with the 1-min delay)
  async function trailGrids() {
    if (!(CFG.gridTrail > 0)) return;
    for (const s of ACTIVE) {
      const c = C[s]; if (!c || hasPos(s) || (c.sim && c.sim.fills.length) || !c.anchor) continue;
      const p = c.lp || c.last; if (!p) continue;
      const L = sideOf(c) === "Buy";   // лонг-сетка снизу едет за ценой вверх сразу
      if (L ? p > c.anchor * (1 + CFG.gridTrail / 100) : p < c.anchor * (1 - CFG.gridTrail / 100)) {
        c.anchor = p;
        try { await gridFor(s); } catch (e) { log(`${s}: трейлинг сетки: ${e.message}`, "err"); }
      }
    }
  }

  // ---------------- earned since the launch: the closed PnL from Bybit (net of fees) of the bot's coins ----------------
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
      while (now() - STAT.doneUntil > W + 3600e3) {   // whole past windows are summed once
        const w = await sumWin(STAT.doneUntil, STAT.doneUntil + W);
        STAT.realDone += w.sum; STAT.closedDone = (STAT.closedDone || 0) + w.n; STAT.doneUntil += W;
      }
      const cur = await sumWin(STAT.doneUntil, now());
      STAT.realized = +(STAT.realDone + cur.sum).toFixed(4); STAT.closed = (STAT.closedDone || 0) + cur.n; saveStat();
    } catch (e) { log("PnL с запуска: " + e.message, "err"); }
  }

  // ---------------- the loops ----------------
  async function fast() {
    if (!running || fastBusy) return; fastBusy = true;
    try {
      if (isLive()) {
        await lastPrices();
        const touched = await pollFills(); await pollPositions();
        for (const s of touched) { try { await gridFor(s); } catch (e) { log(`${s}: доборы: ${e.message}`, "err"); } }
        await trailGrids();
        await refreshRealized();
      } else { await simTick(); await trailGrids(); }
      trackTrades();
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
    if (CFG.turbo && !o._turbo) { o = Object.assign({}, o); for (const k of Object.keys(TURBO)) delete o[k]; }   // пока турбо ВКЛ, его параметры с формы не перетираются
    for (const k of NUMS) if (o[k] != null && o[k] !== "" && Number.isFinite(Number(o[k])) && Number(o[k]) !== CFG[k]) { CFG[k] = Number(o[k]); ch.push(k); }
    CFG.budgetPct = Math.max(5, Math.min(100, CFG.budgetPct)); CFG.fillPct = Math.max(CFG.budgetPct, Math.min(95, CFG.fillPct || 90)); CFG.maxCoins = Math.max(1, Math.min(60, Math.round(CFG.maxCoins))); CFG.simEq = Math.max(10, CFG.simEq); CFG.levels = Math.max(1, Math.min(20, Math.round(CFG.levels))); CFG.delay = Math.max(1, Math.min(10, Math.round(CFG.delay)));
    CFG.addStep = Math.max(0.5, CFG.addStep || 5); CFG.addMult = Math.max(1, Math.min(3, +CFG.addMult || 1)); CFG.maxAdds = Math.max(0, Math.min(50, Math.round(CFG.maxAdds))); CFG.maxPos = Math.max(1, Math.round(CFG.maxPos)); CFG.step = Math.max(0.1, CFG.step); CFG.take = Math.max(0.1, CFG.take);
    CFG.trailGap = Math.max(0.05, CFG.trailGap); CFG.gridTrail = Math.max(0, CFG.gridTrail);
    if (typeof o.trailOn === "boolean" && o.trailOn !== CFG.trailOn) { CFG.trailOn = o.trailOn; ch.push("trailOn"); for (const c of Object.values(C)) c.takeKey = ""; log(CFG.trailOn ? `Трейлинг тейка ВКЛ: от −${CFG.take}% ведётся за минимумом, откат ${CFG.trailGap}% → выход` : `Трейлинг тейка ВЫКЛ: тейк лимиткой −${CFG.take}% от средней`, "info"); }
    CFG.lev = Math.max(1, Math.min(50, Math.round(CFG.lev))); CFG.usdt = Math.max(1, CFG.usdt);
    if (typeof o.live === "boolean" && o.live !== CFG.live) { CFG.live = o.live; ch.push("live"); log(CFG.live ? "⚠ LIVE включён: реальные ордера" : "LIVE выключен: симуляция", CFG.live ? "err" : "info"); for (const c of Object.values(C)) { c.sim.fills = []; c.pos = null; c.lev = false; } }
    if (ch.includes("lev")) for (const c of Object.values(C)) c.lev = false;
    if (typeof o.longOn === "boolean" && o.longOn !== CFG.longOn) { CFG.longOn = o.longOn; ch.push("longOn"); BTC.longBlocked = null; log(CFG.longOn ? "↑ Лонги на дампах ВКЛ (BUY-сетка снизу у падающих монет)" : "↑ Лонги ВЫКЛ — только шорты", "info"); }
    if (typeof o.longBtc === "boolean" && o.longBtc !== CFG.longBtc) { CFG.longBtc = o.longBtc; ch.push("longBtc"); BTC.longBlocked = null; }
    if (typeof o.usPause === "boolean" && o.usPause !== CFG.usPause) { CFG.usPause = o.usPause; ch.push("usPause"); log(CFG.usPause ? "⏸ Паузы ВКЛ: пн–пт 15:00–16:00 (открытие США) и каждую ночь 00:45–03:00 по Мадриду — заявки сняты, позиции закрываются лимитками" : "⏸ Паузы ВЫКЛ", "info"); }
    if (typeof o.btcOnly === "boolean" && o.btcOnly !== CFG.btcOnly) { CFG.btcOnly = o.btcOnly; ch.push("btcOnly"); BTC.blocked = null; }
    CFG.oiPct = Math.max(0, Math.min(50, +CFG.oiPct || 0));
    CFG.coolMin = Math.max(0, Math.min(1440, Math.round(+CFG.coolMin || 0))); CFG.pumpPct = Math.max(0, Math.min(100, +CFG.pumpPct || 0)); CFG.resPct = Math.max(0, Math.min(100, Math.round(+CFG.resPct))); CFG.maxLossUsd = Math.max(0, Math.min(1000, +CFG.maxLossUsd || 0));
    if (typeof o.oiOn === "boolean" && o.oiOn !== CFG.oiOn) { CFG.oiOn = o.oiOn; ch.push("oiOn"); if (BUD) BUD.oiLogged = null; log(CFG.oiOn ? `📉 Фильтр OI ВКЛ: новый SELL только если открытый интерес монеты за 15 мин падает${CFG.oiPct > 0 ? " на " + CFG.oiPct + "% и больше" : ""} (открытые позиции веду как обычно)` : "📉 Фильтр OI ВЫКЛ: SELL при любом открытом интересе", "info"); }
    saveCfg();
    if (ch.length) jlog("cfg", { changed: ch, cfg: CFG });
    if (ch.includes("delay")) schedule();
    return ch;
  }
  // ⚡ турбо: сохраняем обычные настройки, ставим быстрый набор; выкл — возвращаем как было
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
      for (const c of Object.values(C)) c.takeKey = "";
      log(`⚡ ТУРБО ВКЛ: заявки ближе (шаг ${TURBO.step}%, ${TURBO.levels} шт), тейк от −${TURBO.take}% с откатом ${TURBO.trailGap}%, доборы до ${TURBO.maxAdds} через ${TURBO.addStep}%, стоп +${TURBO.stopPct}% над последним добором · авто-выкл при −${TURBO_LOSS_PCT}% депозита`, "ok");
      jlog("turbo", { on: true, save });
      return "⚡ Турбо включено — быстрый оборот, риск ограничен";
    }
    const pnl = turboPnl(), save = CFG.turboSave || {};
    CFG.turbo = false; CFG.turboSave = null; CFG.turboFrom = 0;
    setCfg(Object.assign({ _turbo: true }, save));
    for (const c of Object.values(C)) c.takeKey = "";
    log(`⚡ ТУРБО ВЫКЛ${why ? " (" + why + ")" : ""}: обычные настройки возвращены · за турбо ${pnl >= 0 ? "+" : ""}${pnl.toFixed(2)} USDT`, why ? "err" : "info");
    jlog("turbo", { on: false, why: why || "", pnl });
    return "⚡ Турбо выключено — обычные настройки возвращены";
  }
  function turboGuard(eq) {
    if (!CFG.turbo || !(eq > 0)) return;
    const pnl = turboPnl();
    if (pnl <= -eq * TURBO_LOSS_PCT / 100) setTurbo(false, `убыток ${pnl.toFixed(2)} USDT ≥ ${TURBO_LOSS_PCT}% депозита`);
  }
  async function start() {
    if (CFG.live && !keys()) return "Нет BYBIT_API_KEY / BYBIT_API_SECRET в Railway → Variables";
    running = true; lsSet("vg.running", true); execFrom = now() - 60000;
    if (!STAT.since) { STAT.since = now(); STAT.doneUntil = STAT.since; saveStat(); }
    log(`СТАРТ ${isLive() ? "LIVE" : "СИМУЛЯЦИЯ"} 24/7: лимитки на ${CFG.budgetPct}% депозита — сетки на столько самых волатильных за 1 мин монет, сколько помещается (до ${CFG.maxCoins}) · ${CFG.levels} SELL по ${CFG.usdt} USDT, шаг ${CFG.step}% · сетка за ценой с опозданием ${CFG.delay} мин · тейк −${CFG.take}% от средней · ${CFG.stopPct > 0 ? "стоп +" + CFG.stopPct + "% над последним добором" : "без стопа (только плюс)"} · доборы ×${CFG.addMult} · до ${CFG.maxAdds} доборов каждые ${CFG.addStep}% · плечо x${CFG.lev} · лонги на дампах: ${CFG.longOn ? "ВКЛ" + (CFG.longBtc ? " (не при BTC вниз)" : "") : "выкл"}`, "ok");
    jlog("start", { cfg: CFG });
    schedule(); scan().catch(() => {});
    return "Бот запущен — торгует на сервере 24/7";
  }
  async function stop() {
    running = false; lsSet("vg.running", false); schedule();
    const list = ACTIVE.slice(); ACTIVE = [];
    for (const s of list) if (!hasPos(s)) await dropGrid(s);
    for (const s of Object.keys(C)) if (hasPos(s) && isLive()) await dropGrid(s);
    for (const c of Object.values(C)) if (!isLive()) { c.sim.fills = []; c.pos = null; }
    log("СТОП: сетки и доборы сняты. Открытые позиции остаются с тейком на бирже.", "info"); jlog("stop", {});
    return "Остановлен";
  }
  function resetStats() { Object.assign(STAT, { scans: 0, fills: 0, takes: 0, simPnl: 0, since: running ? now() : 0, realized: 0, closed: 0, realDone: 0, closedDone: 0, doneUntil: running ? now() : 0 }); realAt = 0; saveStat(); return "Статистика сброшена — счёт заработка с этого момента"; }
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

  // ---------------- 📒 the days: trades + PnL per day; 🧠 the fal analysis of a day -> the summary and better settings ----------------
  const DAYC = {};
  function readDay(day) {
    const f = path.join(JDIR, day + ".jsonl"); let st;
    try { st = fs.statSync(f); } catch (e) { return null; }
    if (DAYC[day] && DAYC[day].mtime === st.mtimeMs) return DAYC[day];
    const ev = []; for (const l of fs.readFileSync(f, "utf8").split("\n")) { if (!l) continue; try { ev.push(JSON.parse(l)); } catch (e) { /* a broken line */ } }
    const trades = ev.filter(e => e.type === "trade"), pnl = trades.reduce((a, x) => a + (+x.pnl || 0), 0);
    return (DAYC[day] = { mtime: st.mtimeMs, ev, trades, pnl: +pnl.toFixed(4), n: trades.length, wins: trades.filter(x => x.pnl > 0).length, size: st.size });
  }
  function anFile(day) { return path.join(JDIR, day + ".analysis.json"); }
  function days() {
    let files = []; try { files = fs.readdirSync(JDIR).filter(f => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)).sort().reverse().slice(0, 60); } catch (e) { /* none */ }
    return files.map(f => { const day = f.slice(0, 10), d = readDay(day) || {}; let an = null; try { an = JSON.parse(fs.readFileSync(anFile(day), "utf8")); } catch (e) { /* none */ }
      return { day, n: d.n || 0, wins: d.wins || 0, pnl: d.pnl || 0, kb: Math.round((d.size || 0) / 1024), an }; });
  }
  let anBusy = false;
  async function analyze(day, why) {
    if (!host.fal) return "Нет FAL_KEY в Railway → Variables — анализ fal недоступен";
    if (anBusy) return "Анализ уже идёт"; anBusy = true;
    try {
      const d = readDay(day); if (!d) return "Нет журнала за " + day;
      const scans = d.ev.filter(e => e.type === "scan"), cfgs = d.ev.filter(e => e.type === "cfg" || e.type === "start");
      const btcDown = scans.length ? Math.round(scans.filter(x => x.btc && x.btc.down).length / scans.length * 100) : null;
      const trades = d.trades.slice(-150).map(x => ({ sym: x.sym, side: x.side || "Sell", holdMin: x.holdMin, fills: x.fills, range1m: x.range, btcDown: x.btcDown, adversePct: x.adverse, bestPct: x.best, pnl: x.pnl, lot: x.lot, cfg: x.cfg }));
      const open = Object.values(TRADES).map(x => ({ sym: x.sym, minOpen: Math.round((now() - x.open) / 60000), fills: x.fills, adversePct: +((x.maxPx / x.first - 1) * 100).toFixed(2) }));
      const prompt = `You analyse one UTC day (${day}) of a crypto bot on Bybit (USDT perpetuals). Goal: more profit next days, the account must survive.
STRATEGY (SHORTS on pumps + LONGS on dumps when longOn — a long is the mirror: BUY grid below, adds below, take above, stop below; no new longs while BTC 5m trend is down if longBtc; trades have side Sell/Buy; limit orders; adds grow ×addMult each; market stop-loss stopPct% above the last planned add, 0 = none): every minute it picks the most volatile coins of the last 1m candle and keeps budgetPct% of the deposit in SELL limit grids on them (levels orders, step% apart, the grid follows the price with a delay of delay min, down at once by gridTrail%). A short open -> adds every addStep% above the last fill, at most maxAdds adds (and maxPos orders) per coin. Exit only in profit: at -take% from the average a trailing take starts and exits on a bounce of trailGap% from the low (never worse than -take%). btcOnly = new SELLs only while BTC is in a local downtrend (5m EMA9<EMA21).
Settings now: ${JSON.stringify(CFG)}
Settings changes during the day: ${JSON.stringify(cfgs.slice(-10).map(x => ({ t: new Date(x.t).toISOString().slice(11, 16), changed: x.changed || "start", cfg: x.cfg })))}
The day: ${d.n} closed trades, ${d.wins} in profit, PnL ${d.pnl} USDT. Scans: ${scans.length}, BTC was in a downtrend ${btcDown}% of them, average coins with a grid ${scans.length ? (scans.reduce((a, x) => a + (x.k || 0), 0) / scans.length).toFixed(1) : "—"}.
Closed trades (holdMin, fills = entries+adds, range1m % at the entry, adversePct = the max move against the short from the average, bestPct = the max move in favour, pnl USDT net of fees):
${JSON.stringify(trades)}
Shorts still open: ${JSON.stringify(open)}
Write the result of the day and the best settings for the next days. Only change what the data supports. Allowed keys: budgetPct 10-100, maxCoins 1-60, minTurn, maxCand, levels 1-20, step, usdt, lev 1-50, delay 1-10, take, trailOn, trailGap, gridTrail, addStep, maxAdds 0-20, maxPos, btcOnly.
Answer ONE JSON: {"summary": "итоги дня по-русски, 4-8 коротких пунктов через \\n", "params": {...only the keys to change...}, "reason": "почему такие настройки, по-русски, 2-4 предложения"}`;
      const r = await host.fal(prompt);
      const params = {}; for (const [k, v] of Object.entries(r.params || {})) if (NUMS.includes(k) || ["trailOn", "btcOnly", "longOn"].includes(k)) params[k] = v;
      const an = { day, at: now(), why: why || "по кнопке", pnl: d.pnl, n: d.n, summary: String(r.summary || "").slice(0, 2000), reason: String(r.reason || "").slice(0, 1000), params, applied: false };
      fs.writeFileSync(anFile(day), JSON.stringify(an));
      log(`🧠 Анализ fal за ${day} (${an.why}): PnL ${d.pnl}, сделок ${d.n} — ${Object.keys(params).length ? "рекомендованы настройки: " + JSON.stringify(params) : "настройки менять не нужно"}`, "ok");
      jlog("analysis", { day, params, pnl: d.pnl });
      return `Анализ за ${day} готов`;
    } catch (e) { log("🧠 Анализ fal: " + e.message, "err"); return "Ошибка анализа: " + e.message; }
    finally { anBusy = false; }
  }
  function applyRec(day) {
    let an; try { an = JSON.parse(fs.readFileSync(anFile(day), "utf8")); } catch (e) { return "Нет анализа за " + day; }
    const o = Object.assign({}, an.params);
    delete o.budgetPct;   // долю депозита задаёт пользователь (80 %), анализ её не меняет
    delete o.oiOn; for (const k of ["trailOn", "btcOnly", "longOn"]) if (k in o) o[k] = o[k] === true || o[k] === "true";
    const ch = setCfg(o); an.applied = now(); fs.writeFileSync(anFile(day), JSON.stringify(an));
    log(`🧠 Применены настройки из анализа за ${day}: ${ch.join(", ") || "без изменений"}`, "ok");
    return ch.length ? "Применено: " + ch.join(", ") : "Настройки уже такие";
  }
  // a positive day is analysed by itself after the UTC midnight
  setInterval(() => {
    if (!host.fal || anBusy) return;
    const y = dayOf(now() - 864e5), d = readDay(y);
    if (d && d.pnl > 0 && !fs.existsSync(anFile(y))) analyze(y, "автоматически: плюсовой день").catch(() => {});
  }, 10 * 60000);
  const journalPath = (day) => /^\d{4}-\d{2}-\d{2}$/.test(day) ? path.join(JDIR, day + ".jsonl") : null;

  function view() {
    const rows = [...new Set([...ACTIVE, ...Object.keys(C).filter(s => C[s].pos)])].map(s => {
      const c = coin(s);
      return { sym: s, active: ACTIVE.includes(s), held: hasPos(s), side: sideOf(c), range: c.range, last: c.lp || c.last, anchor: c.anchor, grid: c.grid, pos: c.pos, trail: (TRAIL[s] && TRAIL[s].on) ? { best: TRAIL[s].best, level: TRAIL[s].level } : (c.sim && c.sim.trail && c.sim.trail.on) ? { best: c.sim.trail.best, level: c.pos && c.pos.trail } : null, take: c.pos ? c.pos.avg * (1 + (c.pos.side === "Buy" ? 1 : -1) * CFG.take / 100) : null, takes: c.takes };
    });
    const upnl = rows.reduce((a, r) => a + (r.pos ? r.pos.upnl : 0), 0);
    const unreal = Object.keys(C).filter(hasPos).filter(x => !isLive() || OURS.has(x)).reduce((a, x) => a + (C[x].pos.upnl || 0), 0);
    const realized = isLive() ? STAT.realized || 0 : STAT.simPnl || 0;
    const pnl = { since: STAT.since, realized: +realized.toFixed(4), unreal: +unreal.toFixed(4), total: +(realized + unreal).toFixed(4), closed: isLive() ? STAT.closed || 0 : STAT.takes };
    return { us: { on: !!CFG.usPause, now: CFG.usPause ? pauseNow() : "" }, turbo: { on: !!CFG.turbo, from: CFG.turboFrom || 0, pnl: CFG.turbo ? turboPnl() : 0, lossPct: TURBO_LOSS_PCT, preset: TURBO }, oi: { on: CFG.oiOn, pct: CFG.oiPct }, btc: { on: CFG.btcOnly, down: BTC.down, why: BTC.why, longOn: CFG.longOn, longBtc: CFG.longBtc, longBlocked: !!BTC.longBlocked }, fal: !!host.fal, open: Object.values(TRADES).length, pnl, bud: BUD, running, live: isLive(), cfg: CFG, stat: STAT, rows, cand: CAND, upnl, nextScan: running && lastScanAt ? Math.max(0, Math.round((lastScanAt + CFG.delay * 60000 - now()) / 1000)) : null, log: LOG.slice(0, 200) };
  }

  (async () => { await adoptOld(); if (running) { log("Сервер перезапущен — бот продолжает торговать", "ok"); execFrom = now() - 5000; /* only new fills: the base of the adds is kept on the disk */ schedule(); scan().catch(() => {}); } })();
  return { start, stop, setCfg, setTurbo, resetStats, view, days, analyze, applyRec, journalPath };
}
module.exports = { create };
