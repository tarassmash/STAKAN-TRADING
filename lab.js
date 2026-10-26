// 🧪 ЛАБОРАТОРІЯ СТРАТЕГІЙ: перевіряє всі стратегії на історії найліквідніших монет і шукає ту, що має ДОДАТНЄ МАТОЧІКУВАННЯ.
// Чесна перевірка: параметри підбираються на перших 70% історії, а вердикт — лише на останніх 30%, яких стратегія «не бачила».
// Усе з комісіями: лімітний вхід = maker, тейк лімітом = maker, стоп / вихід ринком = taker + проковзування.
"use strict";

const { MAKER, TAKER } = require("./ind");
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const tick = () => new Promise(r => setImmediate(r));

const { runSig, sqzPrep } = require("./ind");

// ---------- the strategies ----------
// only the chosen strategy is tested (its parameters are re-checked every day on fresh history)
const STRATS = [
  { id: "sqz", name: "Пробій після стиснення (Боллінджер)", tf: "15", grid: [[1.5, 1.1], [2, 1.1], [2.5, 1.1], [3, 1.1], [2, 1.2], [3, 1.2]], run(c, f, t, [tr, sq]) {
      const Z = sqzPrep(c, { sq });
      return runSig(c, f, t, Z.sig, { atr: Z.A, stop: tr, trail: tr }); } },
];

function stats(trades) {
  const n = trades.length; if (!n) return { n: 0, exp: 0, win: 0, pf: 0, sum: 0, dd: 0 };
  let sum = 0, gp = 0, gl = 0, w = 0, pk = 0, dd = 0;
  for (const t of trades) { sum += t.r; if (t.r > 0) { gp += t.r; w++; } else gl -= t.r; pk = Math.max(pk, sum); dd = Math.max(dd, pk - sum); }
  const m = sum / n, sd = Math.sqrt(trades.reduce((a, t) => a + (t.r - m) ** 2, 0) / Math.max(1, n - 1));
  return { n, exp: +m.toFixed(4), win: Math.round(w / n * 100), pf: gl ? +(gp / gl).toFixed(2) : (gp ? 99 : 0), sum: +sum.toFixed(2), dd: +dd.toFixed(2), t: sd ? +(m / (sd / Math.sqrt(n))).toFixed(2) : 0 };
}

function create(host) {
  const { bx, lsGet, lsSet, engineLog } = host;
  const ST = { busy: false, progress: "", res: lsGet("lab.res", null), err: "" };
  async function candles(sym, tf, days) {   // pages of 1000 back in time
    const per = Number(tf) * 60000, need = Math.ceil(days * 86400e3 / per); let end = Date.now(), out = [];
    while (out.length < need) {
      const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: sym, interval: tf, limit: String(Math.min(1000, need - out.length)), end: String(end) } });
      const rows = (k.list || []).map(x => ({ t: Number(x[0]), o: Number(x[1]), h: Number(x[2]), l: Number(x[3]), c: Number(x[4]), v: Number(x[5]) }));
      if (!rows.length) break; out = rows.concat(out); end = rows[rows.length - 1].t - 1; await sleep(120);
      if (rows.length < 200) break;
    }
    const seen = new Set(); return out.filter(x => !seen.has(x.t) && seen.add(x.t)).sort((a, b) => a.t - b.t);
  }
  async function run(opt = {}) {
    if (ST.busy) return "Лабораторія вже працює";
    ST.busy = true; ST.err = ""; ST.progress = "монети…";
    try {
      const nCoins = opt.coins || host.coins || 15;
      const tk = await bx("/v5/market/tickers", { query: { category: "linear" } });
      const coins = (tk.list || []).filter(x => /USDT$/.test(x.symbol) && Number(x.lastPrice) > 0).sort((a, b) => Number(b.turnover24h) - Number(a.turnover24h)).slice(0, nCoins).map(x => x.symbol);
      const data = {};
      for (const [i, s] of coins.entries()) { ST.progress = `історія ${i + 1}/${coins.length}: ${s}`; data[s] = { "15": await candles(s, "15", 60) }; }
      const out = [];
      for (const S of STRATS) {
        ST.progress = `перевіряю: ${S.name}`;
        // 1) pick the parameters on the first 70% of every coin
        let best = null;
        for (const p of S.grid) {
          let isT = [], agg = { roi: 0, n: 0, wins: 0 };
          for (const s of coins) {
            const c = data[s][S.tf]; if (!c || c.length < 300) continue;
            const cut = Math.floor(c.length * 0.7), r = S.run(c, S.agg ? 0 : 50, cut, p);
            if (S.agg) { agg.roi += r.roi; agg.n += r.n; agg.wins += r.wins; } else isT = isT.concat(r);
            await tick();
          }
          const st2 = S.agg ? { n: agg.n, exp: agg.n ? +(agg.roi / agg.n).toFixed(4) : 0 } : stats(isT);
          if (st2.n >= 20 && (!best || st2.exp > best.is.exp)) best = { p, is: st2 };
        }
        if (!best) { out.push({ id: S.id, name: S.name, err: "замало угод" }); continue; }
        // 2) the verdict on the last 30% (never seen while choosing)
        let oos = [], agg = { roi: 0, n: 0, wins: 0 }, pos = 0, tot = 0, perCoin = [];
        for (const s of coins) {
          const c = data[s][S.tf]; if (!c || c.length < 300) continue;
          const cut = Math.floor(c.length * 0.7);
          if (S.agg) { const r = S.run(c, cut, c.length, best.p); agg.roi += r.roi; agg.n += r.n; agg.wins += r.wins; if (r.n) { tot++; if (r.roi > 0) pos++; perCoin.push(r.roi / r.n); } }
          else { const r = S.run(c, cut - 50, c.length, best.p).filter(x => x.i >= cut); oos = oos.concat(r); if (r.length) { tot++; if (r.reduce((a, x) => a + x.r, 0) > 0) pos++; } }
          await tick();
        }
        // significance: per trade for the signal strategies; across coins for the bots (their trades are not independent)
        let tAgg = 0; if (perCoin.length >= 3) { const m = perCoin.reduce((a, x) => a + x, 0) / perCoin.length, sd = Math.sqrt(perCoin.reduce((a, x) => a + (x - m) ** 2, 0) / (perCoin.length - 1)); tAgg = sd ? +(m / (sd / Math.sqrt(perCoin.length))).toFixed(2) : 0; }
        const o2 = S.agg ? { n: agg.n, exp: agg.n ? +(agg.roi / agg.n).toFixed(4) : 0, win: agg.n ? Math.round(agg.wins / agg.n * 100) : 0, sum: +agg.roi.toFixed(2), pf: null, dd: null, t: tAgg } : stats(oos);
        const ok = o2.n >= 30 && o2.exp > 0 && best.is.exp > 0 && tot && pos / tot >= 0.6 && o2.t >= 2;
        out.push({ id: S.id, name: S.name, tf: S.tf, params: best.p, is: best.is, oos: o2, coinsPos: pos, coinsTot: tot, ok });
      }
      out.sort((a, b) => (b.ok - a.ok) || ((b.oos ? b.oos.exp : -1e9) - (a.oos ? a.oos.exp : -1e9)));
      const win = out.find(x => x.ok);
      ST.res = { at: Date.now(), coins, days15: 60, n: STRATS.length, out,
        verdict: win ? `Знайдено стратегію з додатним маточікуванням на даних, яких вона не бачила: «${win.name}» — у середньому ${win.oos.exp > 0 ? "+" : ""}${win.oos.exp}% на угоду після комісій (${win.oos.n} угод, у плюсі ${win.coinsPos} з ${win.coinsTot} монет, достовірність t=${win.oos.t}).`
          : `«${out[0].name}» не пройшла перевірку на даних, яких не бачила: ${out[0].oos ? (out[0].oos.exp > 0 ? "+" : "") + out[0].oos.exp + "% на угоду після комісій, t=" + out[0].oos.t : "—"}. Плюс, що може бути випадковим (t < 2), не рахується — на цих даних очікуваний результат не додатний.` };
      lsSet("lab.res", ST.res);
      engineLog("🧪 Лабораторія: " + ST.res.verdict, win ? "up" : "gold-t");
      return ST.res.verdict;
    } catch (e) { ST.err = e.message; return "Помилка: " + e.message; }
    finally { ST.busy = false; ST.progress = ""; }
  }
  // once a day by itself
  setInterval(() => { if (!ST.busy && (!ST.res || Date.now() - ST.res.at > 24 * 3600e3)) run(); }, 10 * 60000);
  setTimeout(() => { if (!ST.res) run(); }, 20000);
  return { run, view: () => ({ busy: ST.busy, progress: ST.progress, err: ST.err, res: ST.res }) };
}
module.exports = { create, STRATS, stats };
