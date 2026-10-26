// 💥 ПРОБІЙ ПІСЛЯ СТИСНЕННЯ (Боллінджер) — жива торгівля, кілька монет одночасно.
//  • Кожні 15 хв (закриття свічки) сканує ліквідні монети: смуги Боллінджера звузились до мінімуму за ~30 год, і ціна закрилась за смугою -> вхід у бік пробою.
//  • Вхід — лише лімітом (PostOnly) за ціною закриття сигнальної свічки; не виконався до наступної свічки -> скасовується.
//  • Стоп прикріплений до самого ордера (стоїть на Bybit з моменту входу, працює навіть коли сервер вимкнений): trail × ATR.
//  • Далі стоп підтягується за ціною на кожній свічці (трейлінг на ATR) — виходу по тейку немає, прибуток забирає трейлінг.
//  • Розмір угоди — від ризику: при стопі втрачається riskPct % балансу; одночасно не більше maxPos угод.
//  • Параметри (trail, поріг стиснення) беруться з щоденної перевірки лабораторії.
"use strict";
const { sqzPrep } = require("./ind");

const DEFAULTS = { on: false, riskPct: 1.5, maxPos: 3, maxLev: 10, coins: 15, minTurnoverM: 100, trail: 2, sq: 1.1 };
const NUM = { riskPct: [0.2, 10], maxPos: [1, 10], maxLev: [1, 50], coins: [3, 40], minTurnoverM: [5, 5000], trail: [1, 5], sq: [1, 1.5] };

function create(host) {
  const { bx, spec, align, engineLog, lsGet, lsSet } = host;
  const P = Object.assign({}, DEFAULTS, lsGet("sq.p", {}));
  const save = () => lsSet("sq.p", P);
  const POS = lsGet("sq.pos", {});            // sym -> { d, qty, e, sl, at, ord, state: "pending" | "open", sigAt, lev }
  const TR = lsGet("sq.tr", []);
  const savePos = () => { lsSet("sq.pos", POS); if (host.flush) host.flush(); };
  const ST = { err: "", errAt: 0, scanAt: 0, lastBar: 0, coins: [], board: [], busy: false, hedge: {} };
  const now = () => Date.now();
  const setErr = (t) => { ST.err = t; ST.errAt = now(); };
  const BAR = 15 * 60000;

  async function candles(sym, n) {
    const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: sym, interval: "15", limit: String(n) } });
    const rows = (k.list || []).map(x => ({ t: Number(x[0]), o: Number(x[1]), h: Number(x[2]), l: Number(x[3]), c: Number(x[4]), v: Number(x[5]) })).reverse();
    return rows.filter(x => x.t + BAR <= now());   // only closed candles
  }
  async function hedgeOf(sym) {
    if (ST.hedge[sym] != null) return ST.hedge[sym];
    try { const r = await bx("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true }); ST.hedge[sym] = (r.list || []).some(x => Number(x.positionIdx) > 0); } catch (e) { ST.hedge[sym] = false; }
    return ST.hedge[sym];
  }
  const ix = (h, d) => (h ? (d > 0 ? 1 : 2) : 0);
  async function universe() {
    const r = await bx("/v5/market/tickers", { query: { category: "linear" } });
    return (r.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.lastPrice) > 0 && Number(x.turnover24h) >= P.minTurnoverM * 1e6)
      .sort((a, b) => Number(b.turnover24h) - Number(a.turnover24h)).slice(0, P.coins).map(x => x.symbol);
  }
  function params() {   // the lab's choice when it has one
    const L = host.lab && host.lab(), x = L && L.res && L.res.out && L.res.out.find(o => o.id === "sqz" && o.params);
    return x ? { trail: x.params[0], sq: x.params[1], ok: !!x.ok, from: "лабораторія" } : { trail: P.trail, sq: P.sq, ok: null, from: "за замовчуванням" };
  }

  // ---------- entry ----------
  async function enter(sym, d, c, Z, i) {
    const pr = params(), A = Z.A[i], e0 = c[i].c, eq = host.equity();
    const s = await spec(sym), px = (v, up) => Number(align(v, s.tick, up)), qa = (v, up) => Number(align(v, s.step, up));
    const stopDist = A * pr.trail, lev = Math.max(1, Math.min(P.maxLev, Math.floor(s.maxLev)));
    const minN = Math.max(s.minNotional * 1.05, s.minQty * e0);
    let qty = qa(eq * P.riskPct / 100 / stopDist, false);                         // the loss at the stop = riskPct % of the balance
    const maxQty = qa(eq * 0.9 / P.maxPos * lev / e0, false);                      // the margin of one trade fits its share of the balance
    qty = Math.min(qty, maxQty);
    if (qty * e0 < minN) {
      const q2 = qa(minN / e0, true), risk = q2 * stopDist / eq * 100;
      if (risk > P.riskPct * 3 || q2 > maxQty * 1.01) { engineLog(`⏭ ${sym}: сигнал ${d > 0 ? "▲" : "▼"}, але мінімальний ордер Bybit (${minN.toFixed(2)} USDT) замалий для балансу ${eq.toFixed(2)} — ризик був би ${risk.toFixed(1)}%`, "mute"); return; }
      qty = q2;
    }
    const h = await hedgeOf(sym);
    try { await bx("/v5/position/set-leverage", { method: "POST", auth: true, body: { category: "linear", symbol: sym, buyLeverage: String(lev), sellLeverage: String(lev) } }); } catch (e2) { /* not modified */ }
    // a PostOnly limit at the signal close (if the price is already past it on our side - at the best price of our side)
    const tk = ((await bx("/v5/market/tickers", { query: { category: "linear", symbol: sym } })).list || [])[0] || {};
    const bid = Number(tk.bid1Price), ask = Number(tk.ask1Price);
    const price = d > 0 ? px(Math.min(e0, bid || e0), false) : px(Math.max(e0, ask || e0), true);
    const sl = px(price - d * stopDist, d < 0);
    const id = ("sq" + now().toString(36) + sym.slice(0, 6)).slice(0, 36);
    await bx("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: sym, side: d > 0 ? "Buy" : "Sell", orderType: "Limit", price: String(price), qty: String(qty),
      timeInForce: "PostOnly", orderLinkId: id, positionIdx: ix(h, d), stopLoss: String(sl), slTriggerBy: "LastPrice", tpslMode: "Full", slOrderType: "Market" } });
    POS[sym] = { d, qty, e: price, sl, at: now(), sigAt: c[i].t, ord: id, state: "pending", lev, h, trail: pr.trail, peak: price };
    savePos();
    engineLog(`💥 ${sym}: стиснення → пробій ${d > 0 ? "▲ ЛОНГ" : "▼ ШОРТ"} · ліміт ${price}, стоп ${sl} (${pr.trail}×ATR), ${qty} шт · x${lev} · ризик ≈ ${(qty * stopDist / eq * 100).toFixed(1)}% балансу`, d > 0 ? "up" : "down");
  }

  // ---------- managing what is open ----------
  async function manage() {
    for (const [sym, p] of Object.entries(POS)) {
      try {
        if (p.state === "pending") {
          const o = ((await bx("/v5/order/realtime", { query: { category: "linear", symbol: sym, orderLinkId: p.ord }, auth: true })).list || [])[0]
            || ((await bx("/v5/order/history", { query: { category: "linear", symbol: sym, orderLinkId: p.ord }, auth: true })).list || [])[0];
          const st = o && o.orderStatus, got = o ? Number(o.cumExecQty || 0) : 0;
          if (st === "Filled" || (got > 0 && st !== "New" && st !== "PartiallyFilled")) { p.state = "open"; p.qty = got || p.qty; p.e = Number(o.avgPrice) || p.e; p.at = now(); savePos(); engineLog(`✅ ${sym}: вхід виконано ${p.d > 0 ? "лонг" : "шорт"} ${p.qty} по ${p.e} · стоп ${p.sl} уже на біржі`, "up"); }
          else if ((!o && now() - p.at > 60000) || (o && ["Cancelled", "Rejected", "Deactivated"].includes(st))) { if (got > 0) { p.state = "open"; p.qty = got; savePos(); } else { delete POS[sym]; savePos(); engineLog(`⏭ ${sym}: лімітний вхід не виконався${st === "Rejected" || st === "Cancelled" ? " (ціна вже пройшла)" : ""} — пропускаю`, "mute"); } }
          else if (now() - p.sigAt > 2 * BAR) {   // the next candle closed without a fill
            try { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: sym, orderLinkId: p.ord } }); } catch (e) { /* gone */ }
          }
          continue;
        }
        // open: is the position still there?
        const r = await bx("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true });
        const row = (r.list || []).find(x => Number(x.size) > 0 && (p.h ? Number(x.positionIdx) === ix(true, p.d) : true));
        if (!row) {   // closed by the stop on the exchange
          const cp = ((await bx("/v5/position/closed-pnl", { query: { category: "linear", symbol: sym, limit: "5" }, auth: true })).list || []).filter(x => Number(x.updatedTime || x.createdTime) >= p.at - 60000);
          const pnl = cp.reduce((a, x) => a + Number(x.closedPnl || 0), 0), xp = cp[0] ? Number(cp[0].avgExitPrice) : p.sl;
          TR.push({ at: now(), sym, d: p.d, e: p.e, x: xp, qty: p.qty, pnl: +pnl.toFixed(5), how: p.why || ((xp - p.e) * p.d > 0 ? "трейлінг-стоп у плюсі" : "стоп"), min: Math.round((now() - p.at) / 60000), lev: p.lev });
          if (TR.length > 2000) TR.splice(0, TR.length - 2000); lsSet("sq.tr", TR);
          delete POS[sym]; savePos();
          engineLog(`${pnl >= 0 ? "💰" : "🛑"} ${sym}: ${p.d > 0 ? "лонг" : "шорт"} закрито ${p.e} → ${xp} · ${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} USDT`, pnl >= 0 ? "up" : "down");
          continue;
        }
        p.mark = Number(row.markPrice); p.upnl = Number(row.unrealisedPnl); p.qty = Number(row.size); p.e = Number(row.avgPrice) || p.e;
      } catch (e) { setErr(`${sym}: ${e.message}`); }
    }
  }
  // trailing: on every closed candle the stop moves to close - trail × ATR (only towards profit)
  async function trail(sym, c, Z) {
    const p = POS[sym]; if (!p || p.state !== "open") return;
    const i = c.length - 1, A = Z.A[i]; if (!A) return;
    const s = await spec(sym), nsl = Number(align(c[i].c - p.d * A * (p.trail || P.trail), s.tick, p.d < 0));
    if ((p.d > 0 && nsl > p.sl) || (p.d < 0 && nsl < p.sl)) {
      try { await bx("/v5/position/trading-stop", { method: "POST", auth: true, body: { category: "linear", symbol: sym, tpslMode: "Full", stopLoss: String(nsl), slTriggerBy: "LastPrice", positionIdx: ix(p.h, p.d) } });
        engineLog(`↗ ${sym}: стоп підтягнуто ${p.sl} → ${nsl}`, "mute"); p.sl = nsl; savePos(); }
      catch (e) { if (e.code !== 34040) setErr(`${sym} трейлінг: ${e.message}`); }
    }
  }

  // ---------- the 15-minute scan ----------
  async function scan() {
    if (ST.busy) return; ST.busy = true;
    try {
      const pr = params();
      ST.coins = await universe();
      const syms = [...new Set(ST.coins.concat(Object.keys(POS)))], board = [];
      for (const sym of syms) {
        try {
          const c = await candles(sym, 300); if (c.length < 200) continue;
          const Z = sqzPrep(c, { sq: pr.sq }), i = c.length - 1, F = (process.env.TEST_SQZ_FORCE || "").split(":"), d = F[0] === sym ? Number(F[1]) : Z.sig(i);   // TEST_SQZ_FORCE: simulator only
          const w = Z.W[i - 1], mn = Z.minW(i), ratio = mn && isFinite(mn) ? +(w / mn).toFixed(2) : null;
          board.push({ sym, price: c[i].c, squeeze: ratio != null && ratio <= pr.sq, ratio, width: w ? +(w * 100).toFixed(2) : null, sig: d, at: c[i].t });
          await trail(sym, c, Z);
          const open = Object.values(POS).length;
          if (d && P.on && !POS[sym] && open < P.maxPos && c[i].t >= ST.lastBar - BAR) await enter(sym, d, c, Z, i);
        } catch (e) { setErr(`${sym}: ${e.message}`); }
        await new Promise(r => setTimeout(r, 120));
      }
      ST.board = board.sort((a, b) => (b.sig !== 0) - (a.sig !== 0) || (a.ratio || 9) - (b.ratio || 9));
      ST.scanAt = now();
    } catch (e) { setErr(e.message); }
    finally { ST.busy = false; }
  }
  // a scan right after every 15-minute close (+8 s for the exchange), the open trades are checked every 10 s
  setInterval(async () => {
    if (!host.keys()) return;
    const bar = Math.floor((now() - 8000) / BAR) * BAR;
    if (bar > ST.lastBar) { ST.lastBar = bar; await scan(); }
    await manage();
  }, 10000);

  function setOn(on) {
    P.on = !!on; save();
    const pr = params();
    engineLog(on ? `▶ Запущено «Пробій після стиснення»: ${P.coins} монет, ризик ${P.riskPct}% на угоду, до ${P.maxPos} угод одночасно, плече до x${P.maxLev}, стоп ${pr.trail}×ATR (${pr.from})` : "■ Зупинено: нових входів немає; відкриті угоди лишаються зі стопом на біржі (трейлінг продовжується)", on ? "up" : "mute");
    if (on) { ST.lastBar = 0; }
  }
  async function closeAll() {
    for (const [sym, p] of Object.entries(POS)) {
      try {
        if (p.state === "pending") { await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: sym, orderLinkId: p.ord } }).catch(() => {}); continue; }
        p.why = "закрито вручну"; savePos();
        await bx("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: sym, side: p.d > 0 ? "Sell" : "Buy", orderType: "Market", qty: String(p.qty), reduceOnly: true, positionIdx: ix(p.h, p.d), timeInForce: "IOC" } });
      } catch (e) { setErr(`${sym}: ${e.message}`); }
    }
    return "Угоди закриваються ринком";
  }
  function setParams(o) { const ch = []; for (const [k, v] of Object.entries(o || {})) if (NUM[k] && Number.isFinite(Number(v))) { const n = Math.max(NUM[k][0], Math.min(NUM[k][1], Number(v))); if (P[k] !== n) { ch.push(`${k} ${P[k]}→${n}`); P[k] = n; } } save(); return ch; }
  function view() {
    const day = TR.filter(t => t.at >= now() - 86400e3), pr = params();
    return { params: P, active: pr, err: now() - ST.errAt < 180000 ? ST.err : "", scanAt: ST.scanAt, next: Math.max(0, Math.round((ST.lastBar + 2 * BAR + 8000 - now()) / 60000)),
      pos: Object.entries(POS).map(([sym, p]) => ({ sym, d: p.d, state: p.state, qty: p.qty, e: p.e, sl: p.sl, mark: p.mark || null, upnl: p.upnl ?? null, min: Math.round((now() - p.at) / 60000), lev: p.lev })),
      board: ST.board.slice(0, 20), trades: TR.slice(-40).reverse(),
      stats: { n: TR.length, pnl: +TR.reduce((a, t) => a + t.pnl, 0).toFixed(4), win: TR.length ? Math.round(TR.filter(t => t.pnl > 0).length / TR.length * 100) : null, day: +day.reduce((a, t) => a + t.pnl, 0).toFixed(4), dayN: day.length } };
  }
  return { setOn, setParams, closeAll, view, scan, params: P, tradesSince: (t) => TR.filter(x => x.at >= t) };
}
module.exports = { create };
