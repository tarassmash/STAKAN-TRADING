// 🧠 АВТОПЕРЕМИКАЧ РЕЖИМІВ — «мозок» над обома ботами.
// Щогодини (і за кнопкою) сканує ~30 ліквідних монет, визначає ринок кожної і роздає роботу:
//   флет            -> звичайна сітка (нейтральна) на найкращій флетовій монеті (перевірка на історії > 0)
//   тренд           -> пробійна сітка в бік тренду на найкращій трендовій монеті (перевірка на історії > 0)
//   стиснення       -> пробійна сітка «в обидва боки» (межі навколо ціни), якщо трендової монети немає
//   хаос / пилка    -> нічого не відкривається, бот чекає
// Захист від смикання: сітка міняє монету, лише коли її монета 2 перевірки поспіль не флет; пробійна сітка з відкритими угодами не чіпається.
"use strict";
const BT = require("./backtest");

const DEFAULTS = { on: false, full: false, everyMin: 60, minTurnoverM: 100, coins: 30 };
// "full auto": everything tuned for micro takes on high leverage; the size of each bot comes from the real balance (works from 1 USDT)
const FULL_GRID = { leverage: 50, feeX: 4, autoRange: true, autoTune: true, btNeedPlus: true, trailing: true, autoRestart: true, slPct: 0.5,
  breakout: true, bkTrigger: 0.3, bkMult: 0.5, bkStopPct: 0.6, bkTrailStart: 0.4, bkTrailGap: 0.25 };
const FULL_BG = { leverage: 50, scalpStep: 0.3, stopSteps: 2, trailSteps: 2, runner: true, autoTune: true, btNeedPlus: true };

function create(host) {
  const { bx, engineLog, lsGet, lsSet, GR, BG } = host;
  const acc = host.acc || (() => ({ equity: 0, available: 0 })), spec = host.spec;
  const MAKER = host.makerFee != null ? host.makerFee : 0.02, TAKER = 0.055;
  const P = Object.assign({}, DEFAULTS, lsGet("brain.p", {}));
  const save = () => lsSet("brain.p", P);
  const ST = Object.assign({ last: null, at: 0, miss: {}, busy: false, log: [] }, lsGet("brain.st", {}));
  ST.busy = false;
  const saveST = () => lsSet("brain.st", { last: ST.last, at: ST.at, miss: ST.miss, log: ST.log.slice(-40) });
  const now = () => Date.now();
  const note = (t, cls) => { ST.log.push({ at: now(), t }); engineLog("🧠 " + t, cls || "gold-t"); };

  async function candles(sym, iv, n) {
    const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: sym, interval: String(iv), limit: String(n) } });
    return (k.list || []).map(x => ({ o: Number(x[1]), h: Number(x[2]), l: Number(x[3]), c: Number(x[4]) })).reverse();
  }
  // the coins to look at: the most traded + the biggest movers of the day
  async function universe() {
    const r = await bx("/v5/market/tickers", { query: { category: "linear" } });
    const rows = (r.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.lastPrice) > 0).map(x => ({ sym: x.symbol, turnoverM: Number(x.turnover24h) / 1e6, chg: Math.abs(Number(x.price24hPcnt || 0) * 100) }));
    const liquid = rows.filter(x => x.turnoverM >= P.minTurnoverM).sort((a, b) => b.turnoverM - a.turnoverM).slice(0, Math.ceil(P.coins * 0.7));
    const movers = rows.filter(x => x.turnoverM >= P.minTurnoverM * 0.3 && !liquid.includes(x)).sort((a, b) => b.chg - a.chg).slice(0, P.coins - liquid.length);
    return liquid.concat(movers);
  }
  // history check of the job a coin would get
  function testGrid(c5, gp) {
    const swing = (Math.max(...c5.slice(-288).map(k => k.h)) / Math.min(...c5.slice(-288).map(k => k.l)) - 1) * 100;
    const lev = Math.min(gp.leverage, 50), w = Math.min(Math.max(0.8, 100 / lev), Math.max(3, Math.min(30, swing * 0.7))), minStep = gp.feeX * 2 * MAKER;
    const n = Math.max(2, Math.min(Math.round(gp.grids), Math.floor(Math.log(1 + w / 100) / Math.log(1 + minStep / 100))));
    return BT.simulate(c5, { mode: "neutral", widthPct: w, n, investment: gp.investment, lev: gp.leverage, maker: MAKER, slPct: gp.slPct, trailing: gp.trailing, autoRestart: true, minNotional: 5,
      breakout: gp.breakout, bkTrigger: gp.bkTrigger, bkMult: gp.bkMult, bkStopPct: gp.bkStopPct, bkTrailStart: gp.bkTrailStart, bkTrailGap: gp.bkTrailGap });
  }
  function testBreakout(c5, bp) {
    const step = Math.max(bp.scalp ? bp.scalpStep : bp.stepPct, (bp.scalp ? 2 : bp.feeX) * (TAKER + MAKER));
    return Object.assign(BT.simulateB2(c5, { stepPct: step, maxUnits: bp.maxUnits, investment: bp.investment, lev: bp.leverage, maker: MAKER, taker: TAKER, stopSteps: bp.stopSteps, runner: bp.runner, trailSteps: bp.trailSteps, band: bp.band / 100 }), { step });
  }

  // the exchange minimum must fit: an order of at least max(5 USDT, min qty × price)
  async function fits(sym, price, inv, parts) {
    if (!spec) return true;
    try { const s = await spec(sym), lev = Math.min(50, Math.floor(s.maxLev || 10)), need = Math.max(Number(s.minNotional || 5) * 1.05, Number(s.minQty || 0) * price);
      return inv * lev >= need * parts; } catch (e) { return false; }
  }
  const budget = () => Math.max(0, (acc().equity || 0) * 0.9);   // 10% of the balance stays as a buffer against liquidation
  async function run(why) {
    if (ST.busy) return "Аналіз уже йде";
    ST.busy = true;
    try {
      const coins = await universe(), res = [];
      for (let i = 0; i < coins.length; i += 6) {
        await Promise.all(coins.slice(i, i + 6).map(async (cd) => {
          try {
            const [c5, c60] = await Promise.all([candles(cd.sym, 5, 864), candles(cd.sym, 60, 72)]);
            const rg = BT.regimeOf(c5.slice(-300), c60.map(k => k.c));
            res.push(Object.assign({ sym: cd.sym, turnoverM: Math.round(cd.turnoverM), c5 }, rg));
          } catch (e) { /* skip the coin */ }
        }));
      }
      const gp = GR.params, bp = BG.params;
      // validate the best candidates of each kind on 3 days of history
      const flat = res.filter(x => x.regime === "flat").sort((a, b) => b.cross - a.cross || a.er - b.er).slice(0, 4);
      for (const x of flat) { const t = testGrid(x.c5, gp); x.test = t && { roi: t.roi, info: `${t.cycles} кіл, стопів ${t.stops}, пробоїв ${t.breakouts || 0}` }; }
      const trend = res.filter(x => x.regime === "trend").sort((a, b) => b.er * Math.abs(b.slope) - a.er * Math.abs(a.slope)).slice(0, 4);
      for (const x of trend) { const t = testBreakout(x.c5, bp); x.test = t && { roi: t.roi, info: `крок ${t.step}%, тейків ${t.wins}, стопів ${t.losses}, розворотів ${t.flips}` }; }
      const squeeze = res.filter(x => x.regime === "squeeze").sort((a, b) => a.atrRatio - b.atrRatio);
      if (P.full) {   // with a small balance only coins whose minimum order fits
        const half = budget() / 2;
        for (const x of flat.concat(trend, squeeze)) if (!(await fits(x.sym, x.c5[x.c5.length - 1].c, half, x.regime === "flat" ? 2 : 2))) { x.small = true; if (x.test) x.test.info += " · замалий баланс для мін. ордера"; }
      }
      for (const L2 of [flat, trend]) for (const x of L2) if (x.small && x.test) x.test.roi = Math.min(x.test.roi, 0);
      for (let i = squeeze.length - 1; i >= 0; i--) if (squeeze[i].small) squeeze.splice(i, 1);
      const bestFlat = flat.filter(x => x.test && x.test.roi > 0).sort((a, b) => b.test.roi - a.test.roi)[0] || null;
      const bestTrend = trend.filter(x => x.test && x.test.roi > 0 && (!bestFlat || x.sym !== bestFlat.sym)).sort((a, b) => b.test.roi - a.test.roi)[0] || null;
      const bestSq = squeeze.find(x => !bestFlat || x.sym !== bestFlat.sym) || null;
      const by = Object.fromEntries(res.map(x => [x.sym, x]));
      const acts = [];

      // ---- the grid: a flat coin ----
      const g = GR.view().grid;
      if (g) {
        const rg = by[g.sym];
        if (rg && rg.regime === "flat") ST.miss[g.sym] = 0; else ST.miss[g.sym] = (ST.miss[g.sym] || 0) + 1;
        if (ST.miss[g.sym] >= 2) {
          GR.setOn(false); await GR.closeNow(); ST.miss[g.sym] = 0;   // off first: otherwise the grid's own loop rebuilds while the close is in flight
          acts.push(`сітку на ${g.sym} закрито (${rg ? rg.why : "немає даних"})`);
        }
      }
      if (!GR.view().grid) {
        if (bestFlat) {
          const sz = {};
          if (P.full) { const bv = BG.view().b, bgUsed = bv && bv.units.length ? BG.params.investment : 0, bgWants = !bgUsed && (bestTrend || bestSq);
            sz.investment = +Math.max(0.1, (budget() - bgUsed) * (bgWants ? 0.5 : 1)).toFixed(2); }
          GR.setParams(Object.assign({ autoCoin: false, symbol: bestFlat.sym, mode: "neutral" }, sz)); GR.setOn(true);
          let t = await GR.newGrid();
          if (!GR.view().grid) { GR.setOn(false); t = "не вийшло: " + t; }
          acts.push(`сітка → ${bestFlat.sym}: ${bestFlat.why}; на історії ${bestFlat.test.roi >= 0 ? "+" : ""}${bestFlat.test.roi}% (${bestFlat.test.info}) · ${t}`);
        } else if (GR.params.on) { GR.setOn(false); acts.push("сітка чекає: немає флетової монети з плюсом на історії"); }
      }
      // ---- the breakout grid: a trend coin, or both sides on a squeeze ----
      const b = BG.view().b, gSym = (GR.view().grid || {}).sym;
      const bgSize = () => { if (P.full) BG.setParams({ investment: +Math.max(0.1, budget() - (GR.view().grid ? GR.params.investment : 0)).toFixed(2) }); };
      if (b && b.units && b.units.length) acts.push(`пробійна сітка ${b.sym} веде угоди — не чіпаю`);
      else if (bestTrend && bestTrend.sym !== gSym) {
        if (!(b && b.sym === bestTrend.sym && b.d === bestTrend.dir && !b.straddle)) {
          bgSize(); const t = await BG.enterNow(bestTrend.dir, bestTrend.sym);
          acts.push(`пробійна сітка → ${bestTrend.sym} ${bestTrend.dir > 0 ? "▲ лонг" : "▼ шорт"}: ${bestTrend.why}; на історії ${bestTrend.test.roi >= 0 ? "+" : ""}${bestTrend.test.roi}% (${bestTrend.test.info}) · ${t}`);
        } else acts.push(`пробійна сітка лишається на ${b.sym} (тренд триває)`);
      } else if (bestSq && bestSq.sym !== gSym) {
        if (!(b && b.sym === bestSq.sym && (b.straddle || b.box))) {
          if (b) await BG.closeAll();
          const pct = +Math.max(0.6, Math.min(3, bestSq.range6 * 0.6)).toFixed(2);
          bgSize(); const t = await BG.straddleNow(pct, bestSq.sym);
          acts.push(`пробійна сітка → ${bestSq.sym} ↕ обидва боки ±${pct}%: ${bestSq.why} · ${t}`);
        } else acts.push(`межі на ${b.sym} чекають пробою`);
      } else {
        if (b && !b.units.length) { await BG.closeAll(); }
        if (BG.params.on) { BG.setOn(false); acts.push("пробійна сітка чекає: немає тренду чи стиснення з плюсом на історії"); }
      }
      const cnt = (r) => res.filter(x => x.regime === r).length;
      ST.last = { at: now(), why, counts: { flat: cnt("flat"), trend: cnt("trend"), squeeze: cnt("squeeze"), chaos: cnt("chaos") },
        coins: res.map(x => ({ sym: x.sym, regime: x.regime, dir: x.dir || 0, why: x.why, er: x.er, slope: x.slope, atrRatio: x.atrRatio, cross: x.cross, range6: x.range6, turnoverM: x.turnoverM, test: x.test || null }))
          .sort((a, b) => ["trend", "flat", "squeeze", "chaos"].indexOf(a.regime) - ["trend", "flat", "squeeze", "chaos"].indexOf(b.regime)),
        acts };
      ST.at = now(); saveST();
      note(`Аналіз ринку (${res.length} монет: тренд ${cnt("trend")}, флет ${cnt("flat")}, стиснення ${cnt("squeeze")}, хаос ${cnt("chaos")}) → ${acts.join(" · ") || "без змін"}`);
      return acts.join("; ") || "Без змін";
    } catch (e) { note("помилка аналізу: " + e.message, "down"); return "Помилка: " + e.message; }
    finally { ST.busy = false; }
  }
  setInterval(() => { if (P.on && now() - ST.at >= P.everyMin * 60000) run("за розкладом"); }, 30000);

  function setOn(on) { P.on = !!on; if (!on) P.full = false; save(); note(on ? `Автоперемикач увімкнено: кожні ${P.everyMin} хв сам вибирає монети й стратегії` : "Автоперемикач вимкнено: боти працюють із поточними налаштуваннями", on ? "up" : "mute"); if (on) setTimeout(() => run("увімкнення"), 500); }
  function setFull(on) {
    P.full = !!on;
    if (on) {
      GR.setParams(FULL_GRID); BG.setParams(FULL_BG); if (!BG.params.scalp) BG.setScalp(true);
      P.everyMin = 15; P.on = true; save();
      const eq = acc().equity || 0;
      note(`🤖 ПОВНИЙ АВТОМАТ: плече до x50 (де біржа дозволяє), мікро-тейки (пробійна сітка ${FULL_BG.scalpStep}%, звичайна від ${(FULL_GRID.feeX * 0.04).toFixed(2)}%), розмір від балансу ${eq.toFixed(2)} USDT (90%, 10% — запас), сітка вузька (2%) зі стопом 0.5% за краєм, ринок перевіряю кожні ${P.everyMin} хв`, "up");
      setTimeout(() => run("повний автомат"), 500);
    } else { save(); note("Повний автомат вимкнено (автоперемикач і налаштування лишаються, плече можна змінити на вкладках)", "mute"); }
  }
  function setParams(o) { const ch = []; for (const k of ["everyMin", "minTurnoverM", "coins"]) if (o && Number(o[k]) > 0 && P[k] !== Number(o[k])) { P[k] = Math.round(Number(o[k])); ch.push(`${k}→${P[k]}`); } save(); return ch; }
  function view() { return { equity: acc().equity || 0, budget: +budget().toFixed(2), gInv: GR.params.investment, bInv: BG.params.investment, params: P, busy: ST.busy, last: ST.last, next: P.on ? Math.max(0, Math.round((ST.at + P.everyMin * 60000 - now()) / 60000)) : null, log: ST.log.slice(-15).reverse() }; }
  return { params: P, setOn, setFull, setParams, run, view };
}
module.exports = { create, DEFAULTS };
