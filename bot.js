// ЩУКИН НА ПРОБОЯХ — серверный движок, торгует 24/7.
//  • раз в минуту: скан USDT-перпов Bybit — самые подвижные монеты (24ч / 1ч), оборот от minTurn млн $;
//  • у каждой: диапазон последних lookback закрытых минутных свечей (максимум / минимум);
//  • на бирже ставятся условные маркет-ордера на ПРОБОЙ в обе стороны: BUY выше максимума, SELL ниже минимума (+buf %);
//  • пробой исполнен: второй ордер снимается, на бирже сразу стоп за пробитым уровнем (stopBuf %) и трейлинг-стоп
//    (включается на +trailStart % от входа, идёт за ценой с отступом trailGap %) — прибыль бежит, убыток короткий;
//  • 📉 фильтр OI (опция): входить только если открытый интерес монеты за 15 мин растёт (пробой на новых деньгах);
//  • ⚡ турбо: короткий диапазон, близкий стоп и быстрый трейлинг; само выключается при минусе 3 % депозита;
//  • без доборов, без сеток, без усреднения. LIVE выкл — бот только показывает сигналы, ордеров нет.
"use strict";
const fs = require("fs");
const path = require("path");

const DEF = { live: false, usdt: 5, lev: 10, maxPos: 5, maxCoins: 8, minTurn: 5, maxCand: 60, lookback: 15, buf: 0.1, minRange: 0.8, maxRange: 10, minAtr: 0.25, stopBuf: 0.5, trailStart: 1, trailGap: 0.5, oiOn: false, oiPct: 0, turbo: false, turboSave: null, turboFrom: 0 };
// ⚡ ТУРБО на пробоях: диапазон 5 мин, вход ближе, стоп ближе, трейлинг раньше и плотнее
const TURBO = { lookback: 5, buf: 0.05, minRange: 0.4, maxRange: 8, stopBuf: 0.3, trailStart: 0.5, trailGap: 0.3 };
const TURBO_LOSS_PCT = 3;   // турбо выключается само при минусе 3 % депозита с момента включения
const NUMS = ["usdt", "lev", "maxPos", "maxCoins", "minTurn", "maxCand", "lookback", "buf", "minRange", "maxRange", "minAtr", "stopBuf", "trailStart", "trailGap", "oiPct"];

function create(host) {
  const { bx, lsGet, lsSet, keys } = host;
  const now = () => Date.now(), sleep = (ms) => new Promise(r => setTimeout(r, ms));
  // ---------------- 📒 журнал: строка JSON на событие, файл на день UTC (на томе) ----------------
  const JDIR = path.join(host.dataDir || ".", "journal");
  try { fs.mkdirSync(JDIR, { recursive: true }); } catch (e) { /* read-only */ }
  const dayOf = (t) => new Date(t || Date.now()).toISOString().slice(0, 10);
  function jlog(type, data) { fs.appendFile(path.join(JDIR, dayOf() + ".jsonl"), JSON.stringify(Object.assign({ t: now(), type, live: isLive() }, data)) + "\n", () => {}); }

  const old = lsGet("vg.cfg", {});
  const CFG = Object.assign({}, DEF, { live: !!old.live, oiOn: !!old.oiOn, oiPct: +old.oiPct || 0 }, lsGet("bo.cfg", {}));
  const saveCfg = () => lsSet("bo.cfg", CFG);
  if (!lsGet("bo.crazy1", false)) { if (CFG.maxRange === 6) CFG.maxRange = 10; if (CFG.maxCand === 40) CFG.maxCand = 60; if (CFG.turboSave && CFG.turboSave.maxRange === 6) CFG.turboSave.maxRange = 10; if (CFG.turbo && CFG.maxRange === 4) CFG.maxRange = 8; saveCfg(); lsSet("bo.crazy1", true); }   // «бешеные» монеты: шире диапазон, больше кандидатов
  const STAT = Object.assign({ scans: 0, entries: 0, wins: 0, losses: 0, since: 0, realized: 0, closed: 0, realDone: 0, closedDone: 0, doneUntil: 0 }, lsGet("bo.stat", {}));
  const saveStat = () => lsSet("bo.stat", STAT);
  const OURS = new Set(lsGet("bo.syms", []));    // монеты, на которых этот бот ставил ордера пробоя
  const saveOurs = () => lsSet("bo.syms", [...OURS]);
  const TRADES = lsGet("bo.trades", {});         // "SYM|Buy" -> открытая сделка
  const saveTrades = () => lsSet("bo.trades", TRADES);
  const ENTRY = lsGet("bo.entry", {});           // sym -> { Buy: {id, trig, qty}, Sell: {...}, hi, lo }
  const saveEntry = () => lsSet("bo.entry", ENTRY);
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

  // ---------------- состояние ----------------
  const C = {};   // sym -> { hedge, lev, last }
  const coin = (sym) => C[sym] || (C[sym] = { sym, hedge: null, lev: false, last: 0 });
  let POS = {};   // "SYM|Side" -> позиция с биржи
  let BUSY_SYMS = new Set();   // монеты с ЛЮБОЙ позицией на счёте (в т.ч. не этого бота) — туда пробой не ставлю
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

  // ---------------- 1) скан: диапазоны, кандидаты, ордера пробоя ----------------
  async function scan() {
    if (scanning || !running) return; scanning = true;
    try {
      const t = await pub("/v5/market/tickers", { category: "linear" });
      const mv = (x) => Math.max(Math.abs(+x.price24hPcnt || 0) * 100 / 4, +x.prevPrice1h > 0 ? Math.abs(+x.lastPrice / +x.prevPrice1h - 1) * 100 : 0);
      const pool = (t.list || []).filter(x => x.symbol.endsWith("USDT") && +x.turnover24h >= CFG.minTurn * 1e6 && +x.lastPrice > 0)
        .sort((a, b) => mv(b) - mv(a)).slice(0, Math.max(10, Math.round(CFG.maxCand)));
      const out = [];
      for (let i = 0; i < pool.length; i += 10) {
        const part = await Promise.all(pool.slice(i, i + 10).map(async (x) => {
          try {
            const k = await pub("/v5/market/kline", { category: "linear", symbol: x.symbol, interval: "1", limit: Math.round(CFG.lookback) + 1 });
            const rows = (k.list || []).slice(1).map(r => ({ h: +r[2], l: +r[3] }));   // без текущей свечи
            const atr = rows.reduce((a, r) => a + (r.h / r.l - 1) * 100, 0) / Math.max(1, rows.length);   // средняя минутная свеча, % — насколько монета «бешеная»
            if (rows.length < Math.round(CFG.lookback)) return null;
            const hi = Math.max(...rows.map(r => r.h)), lo = Math.min(...rows.map(r => r.l)), range = (hi / lo - 1) * 100, last = +x.lastPrice;
            let oi = null;
            if (CFG.oiOn) { try { const o = await pub("/v5/market/open-interest", { category: "linear", symbol: x.symbol, intervalTime: "5min", limit: 4 }); const l = (o.list || []).map(r => +r.openInterest); if (l.length >= 2 && l[l.length - 1] > 0) oi = (l[0] / l[l.length - 1] - 1) * 100; } catch (e) { /* нет данных */ } }
            return { sym: x.symbol, hi, lo, range, atr, last, ch1h: +x.prevPrice1h > 0 ? (last / +x.prevPrice1h - 1) * 100 : 0, turn: +x.turnover24h, oi, mv: mv(x) };
          } catch (e) { return null; }
        }));
        out.push(...part.filter(Boolean));
      }
      for (const x of out) coin(x.sym).last = x.last;
      // годится: диапазон не слишком узкий (шум) и не слишком широкий (уже улетела), цена внутри диапазона, OI растёт (если фильтр ВКЛ)
      const oiOK = (x) => !CFG.oiOn || (x.oi != null && x.oi > 0 && x.oi >= Math.max(0, CFG.oiPct || 0));
      const blockedOI = [];
      const good = out.filter(x => {
        if (x.atr < CFG.minAtr) return false;
        if (x.range < CFG.minRange || x.range > CFG.maxRange) return false;
        if (x.last >= x.hi || x.last <= x.lo) return false;
        if (!oiOK(x)) { blockedOI.push(short(x.sym) + (x.oi == null ? " (нет OI)" : ` OI ${x.oi >= 0 ? "+" : ""}${x.oi.toFixed(2)}%`)); return false; }
        return true;
      }).sort((a, b) => b.atr - a.atr);   // самые «бешеные» (крупные минутные свечи) — первыми
      if (CFG.oiOn) { const key = blockedOI.slice(0, 8).join(", "); if (key !== (scan.oiLogged ?? "")) { scan.oiLogged = key; if (key) log(`📉 Фильтр OI: открытый интерес не растёт — пробой не ставлю: ${key}${blockedOI.length > 8 ? " …" : ""}`, "info"); } }
      if (isLive()) await account();
      turboGuard(ACC.eq);
      // сколько монет держать «на взводе»: до maxCoins, пока открытых позиций меньше maxPos и хватает свободной маржи
      const slots = Math.max(0, Math.round(CFG.maxPos) - ourOpen());
      const money = !isLive() || ACC.avail >= CFG.usdt * 1.1;
      const want = slots > 0 && money ? good.filter(x => !posOf(x.sym).length && !BUSY_SYMS.has(x.sym)).slice(0, Math.round(CFG.maxCoins)) : [];
      if (!money && !scan.noMoney) log(`Свободной маржи ${ACC.avail.toFixed(2)} USDT меньше залога на сделку ${CFG.usdt} — новые пробои не ставлю`, "err");
      scan.noMoney = !money;
      if (!slots && !scan.full) log(`Открыто ${ourOpen()} позиций из ${CFG.maxPos} — новые пробои не ставлю, веду открытые`, "info");
      scan.full = !slots;
      const next = want.map(x => x.sym), removed = ARMED.filter(s => !next.includes(s)), added = next.filter(s => !ARMED.includes(s));
      ARMED = next;
      CAND = out.sort((a, b) => b.atr - a.atr).slice(0, 15).map(x => ({ sym: x.sym, range: x.range, atr: x.atr, hi: x.hi, lo: x.lo, last: x.last, ch1h: x.ch1h, oi: x.oi, turn: x.turn, armed: next.includes(x.sym), why: next.includes(x.sym) ? "" : x.atr < CFG.minAtr ? "спокойная" : x.range < CFG.minRange ? "узкий" : x.range > CFG.maxRange ? "широкий" : (x.last >= x.hi || x.last <= x.lo) ? "уже вне" : !oiOK(x) ? "OI" : posOf(x.sym).length ? "позиция" : "лимит" }));
      STAT.scans++; lastScanAt = now(); saveStat();
      jlog("scan", { eq: ACC.eq, armed: next, open: Object.keys(TRADES), top: out.slice(0, 10).map(x => [x.sym, +x.range.toFixed(2), +x.atr.toFixed(2), +x.ch1h.toFixed(2)]) });
      if (added.length || removed.length) log(`Скан: на взводе ${next.length} монет → ${next.map(short).join(", ") || "—"}${added.length ? " · + " + added.map(short).join(", ") : ""}${removed.length ? " · − " + removed.map(short).join(", ") : ""}`, "ok");
      for (const s of removed) await dropEntry(s);
      for (const x of want) { try { await arm(x); } catch (e) { log(`${short(x.sym)}: ордера пробоя: ${e.message}`, "err"); } }
    } catch (e) { log("Скан: " + e.message, "err"); }
    finally { scanning = false; }
  }

  // ---------------- 2) условные маркет-ордера пробоя: BUY над максимумом, SELL под минимумом ----------------
  async function arm(x) {
    const sym = x.sym, i = await inst(sym), c = coin(sym);
    const levels = { Buy: +rnd(x.hi * (1 + CFG.buf / 100), i.tick, "up"), Sell: +rnd(x.lo * (1 - CFG.buf / 100), i.tick, "down") };
    const e = ENTRY[sym] || (ENTRY[sym] = {}); e.hi = x.hi; e.lo = x.lo;
    if (!isLive()) { e.Buy = { trig: levels.Buy }; e.Sell = { trig: levels.Sell }; saveEntry(); return; }
    OURS.add(sym); saveOurs();
    await hedgeOf(sym);
    if (!c.lev) { const lev = String(Math.min(CFG.lev, i.maxLev)); try { await post("/v5/position/set-leverage", { category: "linear", symbol: sym, buyLeverage: lev, sellLeverage: lev }); } catch (er) { if (er.code !== 110043) log(`${short(sym)}: плечо: ${er.message}`, "err"); } c.lev = true; }
    for (const side of ["Buy", "Sell"]) {
      const trig = levels[side], qty = qtyFor(i, trig), o = e[side];
      if (o && o.id && Math.abs(o.trig - trig) < +i.tick / 2 && Math.abs(o.qty / qty - 1) < 0.1) continue;
      if (o && o.id) {
        try { await post("/v5/order/amend", { category: "linear", symbol: sym, orderId: o.id, triggerPrice: String(trig), qty: String(qty) }); Object.assign(o, { trig, qty }); }
        catch (er) { await cancel(sym, o.id); e[side] = null; }   // ордер мог уже сработать — новый поставлю только на следующем скане, если позиции нет
        continue;
      }
      try {
        const r = await post("/v5/order/create", { category: "linear", symbol: sym, side, orderType: "Market", qty: String(qty), triggerPrice: String(trig), triggerDirection: side === "Buy" ? 1 : 2, triggerBy: "LastPrice", positionIdx: idxFor(c, side), orderLinkId: "bo_" + now() + Math.floor(Math.random() * 1e4) });
        e[side] = { id: r.orderId, trig, qty };
      } catch (er) {
        if (er.code === 10001 && /position idx/i.test(er.message || "")) { c.hedge = !c.hedge; log(`${short(sym)}: режим позиций Bybit другой — переключаюсь, повтор на следующем скане`, "info"); }
        else log(`${short(sym)}: ордер пробоя ${side === "Buy" ? "↑" : "↓"} @ ${trig}: ${er.message}`, "err");
      }
    }
    saveEntry();
  }
  async function cancel(sym, id) { try { await post("/v5/order/cancel", { category: "linear", symbol: sym, orderId: id }); } catch (e) { /* уже нет */ } }
  async function dropEntry(sym, keepSide) {
    const e = ENTRY[sym]; if (!e) return;
    for (const side of ["Buy", "Sell"]) if (side !== keepSide && e[side]) { if (e[side].id && isLive()) await cancel(sym, e[side].id); e[side] = null; }
    if (!e.Buy && !e.Sell) delete ENTRY[sym];
    saveEntry();
  }

  // ---------------- 3) позиции: новая — снять второй ордер, поставить стоп и трейлинг; закрытая — в журнал ----------------
  async function pollPositions() {
    const r = await priv("GET", "/v5/position/list", { category: "linear", settleCoin: "USDT" });
    const seen = {}, busy = new Set();
    for (const p of r.list || []) {
      if (!(+p.size > 0)) continue;
      busy.add(p.symbol);
      if (!OURS.has(p.symbol)) continue;
      const key = p.symbol + "|" + p.side;
      seen[key] = { sym: p.symbol, side: p.side, size: +p.size, avg: +p.avgPrice, upnl: +p.unrealisedPnl || 0, idx: +p.positionIdx || 0, sl: +p.stopLoss || 0, ts: +p.trailingStop || 0, mark: +p.markPrice || 0 };
      coin(p.symbol).last = +p.markPrice || coin(p.symbol).last;
    }
    POS = seen; BUSY_SYMS = busy;
    for (const [key, p] of Object.entries(seen)) {
      let tr = TRADES[key];
      if (!tr) {
        const e = ENTRY[p.sym] || {};
        tr = TRADES[key] = { sym: p.sym, side: p.side, open: now(), entry: p.avg, level: p.side === "Buy" ? e.hi : e.lo, qty: p.size, maxPx: p.mark, minPx: p.mark, protected: false, cfg: { lookback: CFG.lookback, buf: CFG.buf, stopBuf: CFG.stopBuf, trailStart: CFG.trailStart, trailGap: CFG.trailGap, turbo: !!CFG.turbo } };
        STAT.entries++; saveStat(); saveTrades();
        log(`${short(p.sym)}: 🚀 пробой ${p.side === "Buy" ? "ВВЕРХ → лонг" : "ВНИЗ → шорт"} ${p.size} @ ${fmt(p.avg)} (уровень ${fmt(tr.level)})`, "ok");
        jlog("entry", { sym: p.sym, side: p.side, px: p.avg, level: tr.level, qty: p.size });
        await dropEntry(p.sym, null);   // второй ордер (и повтор этого) снимаю
        ARMED = ARMED.filter(s => s !== p.sym);
      }
      tr.maxPx = Math.max(tr.maxPx, p.mark); tr.minPx = Math.min(tr.minPx, p.mark);
      if (!tr.protected) await protect(p, tr);
    }
    for (const key of Object.keys(TRADES)) if (!seen[key]) await closeTrade(key);
  }
  // стоп за пробитым уровнем + трейлинг-стоп на бирже
  async function protect(p, tr) {
    const i = await inst(p.sym), L = p.side === "Buy", d = L ? 1 : -1;
    const level = tr.level || p.avg;
    const sl = rnd(level * (1 - d * CFG.stopBuf / 100), i.tick, L ? "down" : "up");
    const dist = Math.max(+i.tick, p.avg * CFG.trailGap / 100), act = p.avg * (1 + d * CFG.trailStart / 100);
    const mark = coin(p.sym).last || p.mark;
    if (L ? mark <= +sl : mark >= +sl) {   // цена уже за стопом — выхожу сразу
      try { await post("/v5/order/create", { category: "linear", symbol: p.sym, side: L ? "Sell" : "Buy", orderType: "Market", qty: String(p.size), reduceOnly: true, positionIdx: p.idx, orderLinkId: "bo_x" + now() }); log(`${short(p.sym)}: цена уже вернулась за уровень — закрываю маркетом`, "err"); }
      catch (e) { log(`${short(p.sym)}: выход: ${e.message}`, "err"); }
      tr.protected = true; saveTrades(); return;
    }
    const body = { category: "linear", symbol: p.sym, tpslMode: "Full", stopLoss: sl, slTriggerBy: "LastPrice", slOrderType: "Market", trailingStop: rnd(dist, i.tick, "up"), positionIdx: p.idx };
    if (L ? mark < act : mark > act) body.activePrice = rnd(act, i.tick, L ? "up" : "down");
    try {
      await post("/v5/position/trading-stop", body);
      tr.protected = true; tr.sl = +sl; saveTrades();
      log(`${short(p.sym)}: 🛡 стоп ${sl} (за уровнем ${fmt(level)}, −${(Math.abs(p.avg / sl - 1) * 100).toFixed(2)}% от входа) · трейлинг ${CFG.trailGap}% ${body.activePrice ? "с " + body.activePrice + " (+" + CFG.trailStart + "%)" : "сразу"}`, "info");
    } catch (e) {
      if (e.code === 34040) { tr.protected = true; saveTrades(); return; }
      tr.tries = (tr.tries || 0) + 1; saveTrades();
      log(`${short(p.sym)}: стоп/трейлинг: ${e.message}${tr.tries >= 3 ? " — 3 попытки не вышло, закрываю маркетом" : ""}`, "err");
      if (tr.tries >= 3) { try { await post("/v5/order/create", { category: "linear", symbol: p.sym, side: L ? "Sell" : "Buy", orderType: "Market", qty: String(p.size), reduceOnly: true, positionIdx: p.idx, orderLinkId: "bo_x" + now() }); } catch (er) { log(`${short(p.sym)}: выход: ${er.message}`, "err"); } tr.protected = true; saveTrades(); }
    }
  }
  async function closeTrade(key) {
    const tr = TRADES[key]; if (!tr) return;
    let pnl = null;
    try { const r = await priv("GET", "/v5/position/closed-pnl", { category: "linear", symbol: tr.sym, startTime: tr.open - 60000, limit: 50 }); pnl = (r.list || []).filter(x => (x.side === "Buy" ? "Sell" : "Buy") === tr.side || !x.side).reduce((a, x) => a + (+x.closedPnl || 0), 0); } catch (e) { pnl = null; }
    const hold = (now() - tr.open) / 60000, L = tr.side === "Buy";
    if (pnl != null) { if (pnl > 0) STAT.wins++; else STAT.losses++; }
    log(`${short(tr.sym)}: ${pnl == null ? "позиция закрыта" : pnl > 0 ? "✅ закрыто в плюс " + "+" + pnl.toFixed(2) + " USDT" : "🛑 закрыто в минус " + pnl.toFixed(2) + " USDT"} (${word(tr.side)}, ${hold.toFixed(0)} мин)`, pnl > 0 ? "ok" : "err");
    jlog("trade", Object.assign({}, tr, { close: now(), holdMin: +hold.toFixed(1), best: +((L ? tr.maxPx / tr.entry - 1 : 1 - tr.minPx / tr.entry) * 100).toFixed(2), adverse: +((L ? 1 - tr.minPx / tr.entry : tr.maxPx / tr.entry - 1) * 100).toFixed(2), pnl: pnl == null ? null : +pnl.toFixed(4) }));
    delete TRADES[key]; saveTrades(); saveStat();
  }

  // ---------------- заработано с запуска: закрытый PnL с Bybit (за вычетом комиссий) по монетам бота ----------------
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
    for (const x of t.list || []) if (C[x.symbol]) C[x.symbol].last = +x.lastPrice;
  }
  async function fast() {
    if (fastBusy) return; fastBusy = true;
    try {
      await lastPrices();
      if (isLive()) { await pollPositions(); await refreshRealized(); }
      else if (running) for (const s of ARMED) {   // без LIVE: только сигналы
        const e = ENTRY[s], p = coin(s).last; if (!e || !p) continue;
        for (const side of ["Buy", "Sell"]) if (e[side] && (side === "Buy" ? p >= e[side].trig : p <= e[side].trig)) { log(`[СИГНАЛ] ${short(s)}: пробой ${side === "Buy" ? "вверх" : "вниз"} @ ${fmt(p)} (уровень ${e[side].trig}) — LIVE выкл, ордера нет`, "ok"); e[side] = null; }
      }
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
    if (CFG.turbo && !o._turbo) { o = Object.assign({}, o); for (const k of Object.keys(TURBO)) delete o[k]; }   // пока турбо ВКЛ, его параметры с формы не перетираются
    for (const k of NUMS) if (k in DEF && o[k] != null && o[k] !== "" && Number.isFinite(Number(o[k])) && Number(o[k]) !== CFG[k]) { CFG[k] = Number(o[k]); ch.push(k); }
    CFG.usdt = Math.max(1, CFG.usdt); CFG.lev = Math.max(1, Math.min(50, Math.round(CFG.lev))); CFG.maxPos = Math.max(1, Math.min(30, Math.round(CFG.maxPos))); CFG.maxCoins = Math.max(1, Math.min(40, Math.round(CFG.maxCoins)));
    CFG.lookback = Math.max(3, Math.min(120, Math.round(CFG.lookback))); CFG.buf = Math.max(0, Math.min(2, CFG.buf)); CFG.minRange = Math.max(0.1, CFG.minRange); CFG.minAtr = Math.max(0, Math.min(5, +CFG.minAtr || 0)); CFG.maxRange = Math.max(CFG.minRange + 0.1, CFG.maxRange);
    CFG.stopBuf = Math.max(0.05, Math.min(10, CFG.stopBuf)); CFG.trailStart = Math.max(0, Math.min(20, CFG.trailStart)); CFG.trailGap = Math.max(0.05, Math.min(10, CFG.trailGap)); CFG.oiPct = Math.max(0, Math.min(50, +CFG.oiPct || 0));
    if (ch.includes("lev")) for (const c of Object.values(C)) c.lev = false;
    if (typeof o.live === "boolean" && o.live !== CFG.live) { CFG.live = o.live; ch.push("live"); log(CFG.live ? "⚠ LIVE включён: реальные ордера" : "LIVE выключен: только сигналы", CFG.live ? "err" : "info"); }
    if (typeof o.oiOn === "boolean" && o.oiOn !== CFG.oiOn) { CFG.oiOn = o.oiOn; ch.push("oiOn"); scan.oiLogged = null; log(CFG.oiOn ? `📉 Фильтр OI ВКЛ: пробой только если открытый интерес за 15 мин растёт${CFG.oiPct > 0 ? " на " + CFG.oiPct + "% и больше" : ""}` : "📉 Фильтр OI ВЫКЛ", "info"); }
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
      log(`⚡ ТУРБО ВКЛ: диапазон ${TURBO.lookback} мин, вход +${TURBO.buf}% за уровнем, стоп ${TURBO.stopBuf}% за уровнем, трейлинг с +${TURBO.trailStart}% отступ ${TURBO.trailGap}% · авто-выкл при −${TURBO_LOSS_PCT}% депозита`, "ok");
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
    log(`СТАРТ ${isLive() ? "LIVE" : "(только сигналы)"}: пробои диапазона ${CFG.lookback} мин в обе стороны на ${CFG.maxCoins} монетах · залог ${CFG.usdt} USDT × плечо ${CFG.lev} · до ${CFG.maxPos} позиций · стоп ${CFG.stopBuf}% за уровнем · трейлинг с +${CFG.trailStart}%, отступ ${CFG.trailGap}%${CFG.oiOn ? " · фильтр OI" : ""}${CFG.turbo ? " · ⚡ турбо" : ""}`, "ok");
    jlog("start", { cfg: CFG });
    schedule();
    if (isLive()) { try { await pollPositions(); } catch (e) { /* следующий опрос */ } }
    scan().catch(() => {});
    return "Бот запущен — торгует пробои на сервере 24/7";
  }
  async function stop() {
    running = false; lsSet("bo.running", false); schedule();
    for (const s of Object.keys(ENTRY)) await dropEntry(s);
    ARMED = [];
    log("СТОП: ордера пробоя сняты. Открытые позиции остаются со стопом и трейлингом на бирже.", "info"); jlog("stop", {});
    return "Остановлен";
  }
  function resetStats() { Object.assign(STAT, { scans: 0, entries: 0, wins: 0, losses: 0, since: running ? now() : 0, realized: 0, closed: 0, realDone: 0, closedDone: 0, doneUntil: running ? now() : 0 }); realAt = 0; saveStat(); return "Статистика сброшена"; }
  // разово: заявки сеток прежней стратегии (vg_e…) снимаются; её позиции бот не трогает
  async function dropOldGrids() {
    if (!keys() || lsGet("bo.oldDropped", false)) return;
    try {
      const orders = (await priv("GET", "/v5/order/realtime", { category: "linear", settleCoin: "USDT", limit: 50 })).list || [];
      let n = 0; for (const o of orders) if (/^vg_e/.test(o.orderLinkId || "")) { await cancel(o.symbol, o.orderId); n++; }
      log(`Новая стратегия «пробои»: снято ${n} заявок сеток прежней стратегии. Её открытые позиции бот не трогает — их тейки и стопы остаются на бирже.`, "ok");
    } catch (e) { log("Переход: " + e.message, "err"); return; }
    lsSet("bo.oldDropped", true);
  }

  // ---------------- 📒 дни: сделки и PnL по дням ----------------
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
      const e = ENTRY[s] || {}, ps = posOf(s);
      return { sym: s, last: coin(s).last, hi: e.hi, lo: e.lo, up: e.Buy ? e.Buy.trig : null, dn: e.Sell ? e.Sell.trig : null,
        pos: ps.map(p => ({ side: p.side, size: p.size, avg: p.avg, upnl: p.upnl, sl: p.sl, ts: p.ts })) };
    });
    const unreal = Object.values(POS).reduce((a, p) => a + (p.upnl || 0), 0), realized = STAT.realized || 0;
    const pnl = { since: STAT.since, realized: +realized.toFixed(4), unreal: +unreal.toFixed(4), total: +(realized + unreal).toFixed(4), closed: STAT.closed || 0 };
    return { turbo: { on: !!CFG.turbo, from: CFG.turboFrom || 0, pnl: CFG.turbo ? turboPnl() : 0, lossPct: TURBO_LOSS_PCT, preset: TURBO }, oi: { on: CFG.oiOn, pct: CFG.oiPct },
      open: ourOpen(), pnl, acc: ACC, running, live: isLive(), cfg: CFG, stat: STAT, rows, cand: CAND, nextScan: running && lastScanAt ? Math.max(0, Math.round((lastScanAt + 60000 - now()) / 1000)) : null, log: LOG.slice(0, 200) };
  }

  (async () => {
    await dropOldGrids();
    if (isLive()) { try { await pollPositions(); } catch (e) { /* следующий опрос */ } }
    if (running) log("Сервер перезапущен — бот продолжает торговать пробои", "ok");
    schedule(); if (running) scan().catch(() => {});
  })();
  return { start, stop, setCfg, setTurbo, resetStats, view, days, journalPath };
}
module.exports = { create };
