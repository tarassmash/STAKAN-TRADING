// 🎯 Охотник за импульсом: стакан + лента 25 ликвидных монет в реальном времени (Bybit WebSocket orderbook.50 + publicTrade).
// 4 раза в секунду на каждой монете ищет признаки взрыва за секунды ДО него:
//   • перекос стакана (у лучшей цены покупателей в разы больше, чем продавцов, или наоборот),
//   • со стороны движения убрали ликвидность (стенку сняли / съели),
//   • лента ускорилась в разы и бьёт в одну сторону,
//   • цена только начала идти (ещё не поздно).
// Сигнал → вход ЛИМИТКОЙ (по встречной лучшей цене: исполняется сразу, но не хуже этой цены; остаток снимается через 3 с),
// сразу стоп-лимит на бирже и трейлинг-выход ЛИМИТКОЙ с догонкой: тейк включается на +hAct %, едет за лучшей ценой,
// откат hGap % → выход. Импульс не пошёл за hTimeSec — выход. И так без перерыва, по кругу.
"use strict";
const WS = (() => { try { return require("ws"); } catch (e) { return null; } })();

function create(h) {
  const { P, log } = h;
  const now = () => Date.now();
  const B = {};            // symbol -> book + tape state
  const H = {};            // symbol -> an active hunt
  const COOL = {};         // symbol -> cooldown until
  let ws = null, up = false, ping = null, subs = new Set(), universeAt = 0, timer = null, on = false;
  const STATS = Object.assign({ n: 0, wins: 0, losses: 0, pnl: 0, sim: 0 }, h.lsGet("hunt.stats", {}));
  const saveStats = () => h.lsSet("hunt.stats", STATS);
  let top = [];            // the best candidates right now (for the page)

  // ---------- WebSocket: order books + trades ----------
  function connect() {
    const url = h.wsUrl(); if (!WS || !url || !on) return;
    try { ws = new WS(url); } catch (e) { setTimeout(connect, 3000); return; }
    ws.on("open", () => { up = true; sendSub([...subs], "subscribe"); clearInterval(ping); ping = setInterval(() => { try { ws.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    ws.on("message", (raw) => { let j; try { j = JSON.parse(String(raw)); } catch (e) { return; } if (j.topic) onMsg(j); });
    ws.on("close", () => { up = false; clearInterval(ping); for (const s of Object.keys(B)) B[s].ready = false; if (on) setTimeout(connect, 2000); });
    ws.on("error", () => {});
  }
  function sendSub(list, op) {
    if (!up || !ws || !list.length) return;
    const args = []; for (const s of list) args.push("orderbook.50." + s, "publicTrade." + s);
    for (let i = 0; i < args.length; i += 10) { try { ws.send(JSON.stringify({ op, args: args.slice(i, i + 10) })); } catch (e) { /* reconnect */ } }
  }
  function onMsg(j) {
    if (j.topic.startsWith("orderbook.")) {
      const sym = j.topic.split(".")[2], b = B[sym]; if (!b || !j.data) return;
      if (j.type === "snapshot") { b.bids = new Map(); b.asks = new Map(); }
      else if (!b.ready) return;
      for (const [p, q] of j.data.b || []) { const n = +q; if (n === 0) b.bids.delete(p); else b.bids.set(p, n); }
      for (const [p, q] of j.data.a || []) { const n = +q; if (n === 0) b.asks.delete(p); else b.asks.set(p, n); }
      b.ready = true; b.bookAt = now();
      const hb = H[sym]; if (hb) { const bb = bestOf(b); if (bb) onPrice(sym, hb.d > 0 ? bb.bid : bb.ask); }
    } else if (j.topic.startsWith("publicTrade.")) {
      const sym = j.topic.slice(12), b = B[sym]; if (!b || !Array.isArray(j.data)) return;
      for (const x of j.data) b.trades.push({ t: Number(x.T) || now(), p: +x.p, v: +x.v * +x.p, buy: x.S === "Buy" });
      const cut = now() - 120000; let i = 0; while (i < b.trades.length && b.trades[i].t < cut) i++; if (i) b.trades.splice(0, i);
      if (b.trades.length) b.last = b.trades[b.trades.length - 1].p;
      if (H[sym] && b.last) onPrice(sym, b.last);
    }
  }
  function bestOf(b) {
    let bid = 0, ask = Infinity;
    for (const p of b.bids.keys()) { const x = +p; if (x > bid) bid = x; }
    for (const p of b.asks.keys()) { const x = +p; if (x < ask) ask = x; }
    return bid > 0 && ask < Infinity ? { bid, ask } : null;
  }

  // ---------- the universe: the liquid USDT perps that move ----------
  async function refreshUniverse() {
    universeAt = now();
    try {
      const r = await h.pub("/v5/market/tickers", { category: P.category });
      const list = (r.list || []).filter(t => t.symbol.endsWith("USDT") && +t.turnover24h > 8e6 && +t.lastPrice > 0)
        .map(t => ({ s: t.symbol, turn: +t.turnover24h, mv: Math.abs(+t.price24hPcnt || 0) * 100, hl: (+t.highPrice24h / +t.lowPrice24h - 1) * 100 || 0 }));
      // half by turnover (deep books), half by how much they move
      const n = Math.max(6, Math.min(40, Math.round(P.hUniverse) || 25));
      const byTurn = list.slice().sort((a, b) => b.turn - a.turn).slice(0, Math.ceil(n / 2)).map(x => x.s);
      const byMove = list.slice().sort((a, b) => b.hl - a.hl).map(x => x.s).filter(s => !byTurn.includes(s)).slice(0, n - byTurn.length);
      const want = new Set([...byTurn, ...byMove, ...Object.keys(H)]);
      const add = [...want].filter(s => !subs.has(s)), del = [...subs].filter(s => !want.has(s));
      for (const s of add) B[s] = { bids: new Map(), asks: new Map(), trades: [], hist: [], ready: false, last: 0 };
      sendSub(del, "unsubscribe"); for (const s of del) delete B[s];
      subs = want; sendSub(add, "subscribe");
      if (add.length || del.length) log(`🎯 Охотник: слежу за стаканами ${subs.size} монет${add.length ? ` (+${add.length})` : ""}`, "info");
    } catch (e) { log("🎯 Охотник: список монет: " + e.message, "err"); }
  }

  // ---------- the detector: is an explosive move seconds away? ----------
  function depth(map, from, to) { let v = 0; for (const [p, q] of map) { const x = +p; if (x >= from && x <= to) v += x * q; } return v; }
  function analyse(sym) {
    const b = B[sym]; if (!b || !b.ready || now() - (b.bookAt || 0) > 5000) return null;
    const bb = bestOf(b); if (!bb) return null;
    const mid = (bb.bid + bb.ask) / 2, spread = (bb.ask - bb.bid) / mid * 100;
    const band = Math.max(0.0025, (P.hBand || 0.25) / 100);
    const bidD = depth(b.bids, mid * (1 - band), mid), askD = depth(b.asks, mid, mid * (1 + band));
    const imb = bidD + askD > 0 ? (bidD - askD) / (bidD + askD) : 0;
    b.hist.push({ t: now(), bidD, askD, mid }); while (b.hist.length && now() - b.hist[0].t > 30000) b.hist.shift();
    const old = b.hist.filter(x => now() - x.t > 3000);
    const med = (k) => { if (old.length < 8) return 0; const a = old.map(x => x[k]).sort((x, y) => x - y); return a[a.length >> 1]; };
    const bidMed = med("bidD"), askMed = med("askD");
    // the tape: the last 5 s against the last 2 min
    const tr = b.trades, t5 = now() - 5000, w5 = tr.filter(x => x.t >= t5);
    const span = Math.max(30000, now() - (tr.length ? tr[0].t : now()));
    const rate5 = w5.length / 5, rateAvg = tr.length / (span / 1000) || 0.01, speed = rate5 / rateAvg;
    const vol5 = w5.reduce((a, x) => a + x.v, 0), buy5 = vol5 ? w5.reduce((a, x) => a + (x.buy ? x.v : 0), 0) / vol5 : 0.5;
    const p5 = b.hist.find(x => now() - x.t <= 5000), mv5 = p5 ? (mid / p5.mid - 1) * 100 : 0;
    const late = P.hLate || 0.8, minVol = (P.orderUsdt || 20) * (P.leverage || 3) * 3;
    const sc = (d) => {
      let s = 0; const why = [];
      const im = d * imb; if (im >= 0.7) { s += 2; why.push(`стакан ${Math.round(im * 100)}% в ${d > 0 ? "покупку" : "продажу"}`); } else if (im >= 0.45) { s += 1; why.push(`перекос стакана ${Math.round(im * 100)}%`); }
      const thin = d > 0 ? (askMed ? askD / askMed : 1) : (bidMed ? bidD / bidMed : 1);
      if (thin <= 0.4) { s += 2; why.push(`${d > 0 ? "продавцов" : "покупателей"} сняли: осталось ${Math.round(thin * 100)}%`); } else if (thin <= 0.6) { s += 1; why.push(`${d > 0 ? "продавцы" : "покупатели"} тают (${Math.round(thin * 100)}%)`); }
      const ag = d > 0 ? buy5 : 1 - buy5;
      if (speed >= (P.hSpeed || 3) && ag >= 0.7 && vol5 >= minVol) { s += speed >= 2 * (P.hSpeed || 3) ? 3 : 2; why.push(`лента ×${speed.toFixed(1)}, ${Math.round(ag * 100)}% ${d > 0 ? "покупок" : "продаж"}`); }
      const m = d * mv5; if (m >= 0.08 && m < late) { s += 1; why.push(`цена пошла ${mv5 >= 0 ? "+" : ""}${mv5.toFixed(2)}% за 5 с`); }
      const flowOk = speed >= 1.5 && ag >= 0.6;            // the book alone can be a spoof: the tape must confirm
      const ok = flowOk && m > -0.05 && m < late && spread <= (P.hMaxSpread || 0.12);
      return { s: ok ? s : Math.min(s, (P.hScore || 5) - 1), why, ok };
    };
    const up1 = sc(1), dn1 = sc(-1), d = up1.s >= dn1.s ? 1 : -1, r = d > 0 ? up1 : dn1;
    return { sym, d, score: r.s, ok: r.ok, why: r.why, mid, bid: bb.bid, ask: bb.ask, spread: +spread.toFixed(3), imb: +imb.toFixed(2), speed: +speed.toFixed(1), buy5: +buy5.toFixed(2), mv5: +mv5.toFixed(3) };
  }

  // ---------- the loop: 4 times a second ----------
  let tickBusy = false;
  async function tick() {
    if (!on || tickBusy) return; tickBusy = true;
    try {
      if (now() - universeAt > 60000) refreshUniverse();
      const res = [];
      for (const s of subs) { const a = analyse(s); if (a) res.push(a); }
      res.sort((a, b) => b.score - a.score || b.speed - a.speed);
      top = res.slice(0, 8);
      if (h.dayStopped()) return;
      const active = Object.keys(H).length, maxN = Math.max(1, Math.round(P.hMax) || 2);
      for (const a of res) {
        if (Object.keys(H).length >= maxN || active >= maxN) break;
        if (a.score < (P.hScore || 5) || !a.ok || H[a.sym] || (COOL[a.sym] || 0) > now() || h.hasPosition(a.sym)) continue;
        enter(a).catch(e => { log(`🎯 ${a.sym}: вход: ${e.message}`, "err"); delete H[a.sym]; COOL[a.sym] = now() + 60000; });
        break;   // one entry per tick
      }
    } catch (e) { log("🎯 Охотник: " + e.message, "err"); }
    finally { tickBusy = false; }
  }

  // ---------- entry: a limit at the opposite best price (fills now, never worse); the rest is cancelled in 3 s ----------
  async function enter(a) {
    const sym = a.sym, d = a.d, live = P.liveEnabled && h.keys();
    const inst = await h.getInstrument(sym, P.category);
    const tick = parseFloat(inst.tickSize), px = h.roundToStep(d > 0 ? a.ask + tick * (P.hSlipTicks || 1) : a.bid - tick * (P.hSlipTicks || 1), inst.tickSize);
    const usdt = (P.orderUsdt || 20);
    let qty = h.floorToStep(usdt / px, inst.qtyStep);
    if (qty < parseFloat(inst.minQty)) qty = parseFloat(inst.minQty);
    if (qty * px < inst.minNotional) qty = h.roundToStep(Math.ceil(inst.minNotional * 1.02 / px / parseFloat(inst.qtyStep)) * parseFloat(inst.qtyStep), inst.qtyStep);
    const hunt = H[sym] = { sym, d, state: "entering", at: now(), px, qty, score: a.score, why: a.why.join(", "), best: px, entry: px, act: false, live };
    log(`🎯 ${sym}: ${d > 0 ? "▲ ПАМП" : "▼ ДАМП"} через секунды? (оценка ${a.score}: ${hunt.why}) — вход ${d > 0 ? "BUY" : "SELL"} лимиткой ${qty} @ ${px}${live ? "" : " [СИМУЛЯЦИЯ]"}`, "ok");
    if (!live) { hunt.state = "in"; hunt.entry = px; hunt.best = px; hunt.stop = px * (1 - d * (P.hStop || 0.6) / 100); return; }
    await h.ensureLev(sym, P.category);
    const idx = await h.posIdx(sym, d);
    const body = { category: P.category, symbol: sym, side: d > 0 ? "Buy" : "Sell", orderType: "Limit", qty: String(qty), price: String(px), timeInForce: "GTC", positionIdx: idx, orderLinkId: "sg_h_" + now() };
    const r = await h.placeEntry(body, inst);   // the lot is fitted to the free money on the account
    if (!r) { delete H[sym]; COOL[sym] = now() + 30000; return; }
    hunt.orderId = r.orderId; hunt.qty = Number(body.qty);
  }

  // ---------- the position: every price tick from the WebSocket ----------
  function onPrice(sym, px) {
    const x = H[sym]; if (!x || x.state !== "in" || !px) return;
    const d = x.d, prof = (px / x.entry - 1) * 100 * d;
    x.last = px; x.prof = +prof.toFixed(3);
    if (d > 0 ? px > x.best : px < x.best) x.best = px;
    if (!x.act && prof >= (P.hAct || 0.3)) { x.act = true; log(`🎯 ${sym}: импульс пошёл +${prof.toFixed(2)}% — трейлинг-выход включён, откат ${P.hGap || 0.25}% → выход лимиткой`, "ok"); }
    const back = (d > 0 ? (x.best - px) / x.best : (px - x.best) / x.best) * 100;
    if (x.act && back >= (P.hGap || 0.25)) return exit(sym, `трейлинг: откат ${back.toFixed(2)}% от лучшей (+${((x.best / x.entry - 1) * 100 * d).toFixed(2)}%)`);
    if (prof <= -(P.hStop || 0.6)) return exit(sym, `стоп −${P.hStop || 0.6}%`);
  }
  async function exit(sym, why) {
    const x = H[sym]; if (!x || x.state !== "in") return; x.state = "exiting"; x.exitWhy = why; x.exitAt = now();
    if (!x.live) { finish(sym, ((x.last || x.entry) / x.entry - 1) * 100 * x.d * (x.qty * x.entry) / 100 - (x.qty * x.entry) * 0.0011, true); return; }
    try { await h.cancelOurOrders(sym, P.category); } catch (e) { /* gone */ }
    await h.closeChase(sym, P.category, "🎯 " + why, { aggrSec: P.hChaseSec || 2 });
  }
  function finish(sym, pnl, sim) {
    const x = H[sym]; if (!x) return;
    STATS.n++; STATS.pnl += pnl; if (pnl >= 0) STATS.wins++; else STATS.losses++; if (sim) STATS.sim++; saveStats();
    if (!sim) h.recordTrade(pnl, sym, "🎯 импульс: " + (x.exitWhy || "выход"));
    else log(`🎯 ${sym} [СИМ]: ${x.exitWhy} — PnL ${pnl >= 0 ? "+" : ""}${pnl.toFixed(3)} USDT`, pnl >= 0 ? "ok" : "err");
    delete H[sym]; COOL[sym] = now() + (P.hCoolSec || 120) * 1000;
  }

  // ---------- the hunts against the exchange: twice a second ----------
  let manBusy = false;
  async function manage() {
    if (manBusy) return; manBusy = true;
    try {
      for (const sym of Object.keys(H)) {
        const x = H[sym]; if (!x.live) { if (x.state === "in" && now() - x.at > (P.hTimeSec || 30) * 1000 && !x.act) exit(sym, `импульс не пошёл за ${P.hTimeSec || 30} с`); continue; }
        try {
          const pos = await h.getPosition(sym, P.category);
          const mine = pos.size > 0 && (pos.side === "Buy" ? 1 : -1) === x.d;
          if (x.state === "entering") {
            if (mine) {
              x.state = "in"; x.entry = pos.avgPrice; x.best = pos.avgPrice; x.size = pos.size;
              const inst = await h.getInstrument(sym, P.category), slPx = h.roundToStep(pos.avgPrice * (1 - x.d * (P.hStop || 0.6) * 1.5 / 100), inst.tickSize);
              try { await h.setLimitStop(sym, P.category, pos, slPx, inst); } catch (e) { log(`🎯 ${sym}: стоп на бирже: ${e.message}`, "err"); }
              log(`🎯 ${sym}: вошёл ${x.d > 0 ? "ЛОНГ" : "ШОРТ"} ${pos.size} @ ${pos.avgPrice} — трейлинг-выход лимиткой ждёт +${P.hAct || 0.3}%, стоп −${P.hStop || 0.6}% (на бирже стоп-лимит ${slPx})`, "ok");
            }
            if (now() - x.at > 3000) {   // the rest of the entry limit is cancelled: the impulse does not wait
              try { await h.priv("POST", "/v5/order/cancel", { category: P.category, symbol: sym, orderId: x.orderId }); } catch (e) { /* filled / gone */ }
              if (!mine) { log(`🎯 ${sym}: лимитка не исполнилась за 3 с — снял, ищу дальше`, "info"); delete H[sym]; COOL[sym] = now() + 30000; }
            }
          } else if (x.state === "in") {
            if (!mine) { x.exitWhy = x.exitWhy || "закрыто на бирже (стоп)"; x.state = "exiting"; x.exitAt = now(); }
            else if (!x.act && now() - x.at > (P.hTimeSec || 30) * 1000) await exit(sym, `импульс не пошёл за ${P.hTimeSec || 30} с`);
          }
          if (x.state === "exiting" && !h.chasing(sym) && !(pos.size > 0 && mine)) {
            let pnl = 0;
            try { const cp = ((await h.priv("GET", "/v5/position/closed-pnl", { category: P.category, symbol: sym, limit: 5 })).list || []).filter(c => Number(c.updatedTime || c.createdTime) >= x.at - 2000);
              pnl = cp.reduce((a, c) => a + Number(c.closedPnl || 0), 0); } catch (e) { /* 0 */ }
            log(`🎯 ${sym}: ✅ готово за ${Math.round((now() - x.at) / 1000)} с — ${x.exitWhy}`, pnl >= 0 ? "ok" : "err");
            finish(sym, pnl, false);
          }
        } catch (e) { log(`🎯 ${sym}: ${e.message}`, "err"); }
      }
    } finally { manBusy = false; }
  }
  // positions left from before a restart are taken over (trailing + stop)
  async function adopt() {
    if (!P.liveEnabled || !h.keys()) return;
    try {
      const r = await h.priv("GET", "/v5/position/list", { category: P.category, settleCoin: "USDT" });
      for (const p of r.list || []) if (+p.size > 0 && !H[p.symbol]) {
        const d = p.side === "Buy" ? 1 : -1;
        H[p.symbol] = { sym: p.symbol, d, state: "in", at: now(), entry: +p.avgPrice, best: +p.avgPrice, act: false, live: true, qty: +p.size, why: "позиция с биржи" };
        if (!subs.has(p.symbol)) { subs.add(p.symbol); B[p.symbol] = { bids: new Map(), asks: new Map(), trades: [], hist: [], ready: false, last: 0 }; sendSub([p.symbol], "subscribe"); }
        log(`🎯 ${p.symbol}: беру открытую позицию под трейлинг-выход`, "info");
      }
    } catch (e) { /* skip */ }
  }

  let mTimer = null;
  function start() {
    if (on) return; on = true; universeAt = 0;
    if (!WS) { log("🎯 Охотник: нет модуля ws", "err"); return; }
    connect(); refreshUniverse(); adopt();
    timer = setInterval(() => { tick().catch(() => {}); }, 250);
    mTimer = setInterval(() => { manage().catch(() => {}); }, 500);
    log(`🎯 ОХОТНИК ЗА ИМПУЛЬСОМ запущен: стаканы и лента ~${P.hUniverse || 25} монет в реальном времени, оценка ≥ ${P.hScore || 5} → вход лимиткой, трейлинг-выход лимиткой (+${P.hAct || 0.3}% / откат ${P.hGap || 0.25}%), стоп ${P.hStop || 0.6}%, до ${P.hMax || 2} сделок одновременно${P.liveEnabled ? "" : " — СИМУЛЯЦИЯ (Live выкл)"}`, "ok");
  }
  function stop() {
    on = false; clearInterval(timer); clearInterval(mTimer); timer = mTimer = null;
    try { if (ws) ws.close(); } catch (e) { /* closed */ }
    ws = null; up = false; subs = new Set(); for (const s of Object.keys(B)) delete B[s]; top = [];
    for (const s of Object.keys(H)) if (H[s].state === "entering" && H[s].live && H[s].orderId) h.priv("POST", "/v5/order/cancel", { category: P.category, symbol: s, orderId: H[s].orderId }).catch(() => {});
    log("🎯 Охотник остановлен (открытые позиции веду до выхода обычным циклом)", "info");
  }
  function view() {
    return { on, up, coins: subs.size, stats: STATS,
      top: top.map(a => ({ sym: a.sym, d: a.d, score: a.score, ok: a.ok, why: a.why.join(" · "), imb: a.imb, speed: a.speed, mv5: a.mv5, spread: a.spread })),
      hunts: Object.values(H).map(x => ({ sym: x.sym, d: x.d, state: x.state, entry: x.entry, best: x.best, prof: x.prof || 0, act: x.act, sec: Math.round((now() - x.at) / 1000), why: x.why, live: x.live })) };
  }
  const levels = (sym) => { const x = H[sym]; if (!x || x.state !== "in") return null; const d = x.d;
    return { entry: x.entry, stop: x.entry * (1 - d * (P.hStop || 0.6) / 100), act: x.entry * (1 + d * (P.hAct || 0.3) / 100), trail: x.act ? x.best * (1 - d * (P.hGap || 0.25) / 100) : null, d }; };
  return { start, stop, view, levels, isOn: () => on, has: (s) => !!H[s], _analyse: analyse, _B: B, _H: H, _tick: tick, _manage: manage, _onMsg: onMsg, _subs: () => subs };
}
module.exports = { create };
