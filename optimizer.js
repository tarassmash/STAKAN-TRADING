// 🧠 Молниеносное обучение: быстрый бэктест текущей стратегии на минутных свечах + перебор настроек.
// Честно: подбор на первых 70% истории, выбор и вердикт — по последним 30%, которых вариант не видел.
// Результаты — в % от суммы одной заявки (одного уровня), с комиссиями (вход лимитом = maker, выход трейлингом / стопом = taker + проскальзывание).
"use strict";
const SLIP = 0.02;   // % on market exits

// the path inside a candle: an up candle goes open -> low -> high -> close, a down one open -> high -> low -> close
const path = (k) => (k.close >= k.open ? [k.open, k.low, k.high, k.close] : [k.open, k.high, k.low, k.close]);

// ---------- «Автопилот за ценой»: limits +-off, levels, adds, trailing take, stop, pulse ----------
function simBand(c, p, fee, from, to) {
  const mk = fee.maker, tk = fee.taker + SLIP, off = p.bandPct / 100, step = p.step / 100, tp = p.tp / 100, gap = p.gap / 100, sl = p.sl / 100;
  const nRef = p.bandRef || 5, pulseN = Math.max(1, Math.round((p.pulseSec || 60) / 60));
  let pos = null, mom = null, sum = 0, peak = 0, dd = 0, wins = 0, losses = 0, n = 0;
  const close = (x, how) => {
    const g = pos.units.reduce((a, u) => a + ((x / u.e - 1) * 100 * pos.d - mk), 0) - pos.units.length * (how === "take" ? tk : tk);
    sum += g; n++; g > 0 ? wins++ : losses++; peak = Math.max(peak, sum); dd = Math.max(dd, peak - sum); pos = null;
  };
  const open = (d, e, momPos) => { pos = { d, units: [{ e }], avg: e, adds: 0, trail: false, best: e, momPos }; };
  const add = (e) => { pos.units.push({ e }); pos.adds++; pos.avg = pos.units.reduce((a, u) => a + u.e, 0) / pos.units.length; };
  for (let i = Math.max(from, nRef + pulseN); i < to; i++) {
    const k = c[i];
    let ref = 0; for (let j = i - nRef; j < i; j++) ref += c[j].close; ref /= nRef;
    const mv = (c[i - 1].close / c[i - 1 - pulseN].close - 1) * 100;
    if (p.pulse && Math.abs(mv) >= p.pulse) mom = { d: Math.sign(mv), until: i + (p.momHold || 10) };
    if (mom && i > mom.until && !pos) mom = null;
    const md = mom ? mom.d : 0, pre = p.pulse && Math.abs(mv) >= p.pulse / 2 ? Math.sign(mv) : 0;
    if (pos && md && pos.d === -md) close(k.open, "pulse");                        // against a pump / dump: out at once
    if (!pos && md) open(md, k.open, true);                                         // with the move (a limit at the best price ~ the open)
    for (const x of path(k)) {
      if (!pos) {
        if (md) continue;
        for (let l = 0; l < p.levels && !pos; l++) {
          const s = ref * (1 + off + step * l), b = ref * (1 - off - step * l);
          if (x >= s) { open(-1, s, false); for (let m = l + 1; m < p.levels; m++) if (x >= ref * (1 + off + step * m)) add(ref * (1 + off + step * m)); }
          else if (x <= b) { open(1, b, false); for (let m = l + 1; m < p.levels; m++) if (x <= ref * (1 - off - step * m)) add(ref * (1 - off - step * m)); }
        }
        continue;
      }
      const d = pos.d, slx = pos.avg * (1 - d * (pos.momPos ? (p.momSl || 1.5) / 100 : sl));
      if ((d > 0 && x <= slx) || (d < 0 && x >= slx)) { close(slx, "stop"); continue; }
      if (!pos.trail && ((d > 0 && x >= pos.avg * (1 + tp)) || (d < 0 && x <= pos.avg * (1 - tp)))) { pos.trail = true; pos.best = x; }
      if (pos.trail) {
        pos.best = d > 0 ? Math.max(pos.best, x) : Math.min(pos.best, x);
        const st = pos.best * (1 - d * gap);
        if ((d > 0 && x <= st) || (d < 0 && x >= st)) { close(st, "take"); continue; }
      }
      if (!pos.momPos && pos.adds < p.adds && !(pre && pre === -d)) {
        const lvl = pos.avg * (1 - d * step); if ((d > 0 && x <= lvl) || (d < 0 && x >= lvl)) add(lvl);
      }
    }
  }
  if (pos) close(c[to - 1].close, "end");
  return { pnl: +sum.toFixed(3), dd: +dd.toFixed(3), n, win: n ? Math.round(wins / n * 100) : 0 };
}

// ---------- Sell Grid (normal mode): sells above the price, take / stop from the average ----------
function simSell(c, p, fee, from, to) {
  const mk = fee.maker, tk = fee.taker + SLIP, step = p.step / 100, tp = p.tp / 100, sl = p.sl / 100, exitFee = p.makerExit ? mk : tk;
  let pos = null, sum = 0, peak = 0, dd = 0, wins = 0, losses = 0, n = 0;
  const close = (x, f) => { const g = pos.units.reduce((a, e) => a + ((e / x - 1) * 100 - mk - f), 0); sum += g; n++; g > 0 ? wins++ : losses++; peak = Math.max(peak, sum); dd = Math.max(dd, peak - sum); pos = null; };
  for (let i = Math.max(from, 1); i < to; i++) {
    const k = c[i], mid = c[i - 1].close;
    const lv = Array.from({ length: p.levels }, (_, j) => mid * (1 + step * (j + 1)));
    for (const x of path(k)) {
      if (!pos) { for (const L of lv) if (x >= L) { pos = pos || { units: [] }; pos.units.push(L); } if (pos) pos.avg = pos.units.reduce((a, e) => a + e, 0) / pos.units.length; continue; }
      if (x >= pos.avg * (1 + sl)) { close(pos.avg * (1 + sl), tk); continue; }
      if (x <= pos.avg * (1 - tp)) { close(pos.avg * (1 - tp), exitFee); continue; }
    }
  }
  if (pos) close(c[to - 1].close, tk);
  return { pnl: +sum.toFixed(3), dd: +dd.toFixed(3), n, win: n ? Math.round(wins / n * 100) : 0 };
}

// ---------- the search ----------
const yieldNow = () => new Promise(r => setImmediate(r));
async function optimize(c, P, steps, strategy) {
  const fee = { maker: steps.maker, taker: steps.taker }, cut = Math.floor(c.length * 0.7), lim = steps.lim, mkt = steps.mkt;
  const grid = [];
  if (strategy === "follow") {
    for (const bandPct of [1, 1.5, 2, 3, 4]) for (const levels of [1, 3, 5]) for (const adds of [0, 3, 5]) for (const tp of [mkt, 1, 1.5, 2.5].filter((v, i, a) => v >= mkt && a.indexOf(v) === i))
      for (const gr of [0.3, 0.5]) for (const sl of [3, 5, 10]) for (const pulse of [0, 1, 1.5, 2.5])
        grid.push({ bandPct, levels, adds, step: lim, tp: +tp.toFixed(3), gap: +(tp * gr).toFixed(3), sl, pulse, pulseSec: P.pulseSec, momSl: P.momSl, momHold: P.momHoldMin, bandRef: P.bandRef });
  } else {
    for (const step of [lim, 0.6, 1, 1.5, 2].filter((v, i, a) => v >= lim && a.indexOf(v) === i)) for (const levels of [3, 5, 8]) for (const tp of [lim, 0.6, 1, 1.5].filter((v, i, a) => v >= lim && a.indexOf(v) === i)) for (const sl of [3, 5, 10])
      grid.push({ step: +step.toFixed(3), levels, tp: +tp.toFixed(3), sl, makerExit: P.makerExit });
  }
  const sim = strategy === "follow" ? simBand : simSell, res = [];
  for (let i = 0; i < grid.length; i++) {
    const g = grid[i], tr = sim(c, g, fee, 0, cut);
    res.push({ p: g, train: tr, score: tr.n >= 8 ? tr.pnl - 0.5 * tr.dd : -1e9 + tr.pnl });   // too few trades = luck, not a pattern
    if (i % 200 === 0) await yieldNow();
  }
  res.sort((a, b) => b.score - a.score);
  const top = res.slice(0, 30);
  for (const r of top) { r.valid = sim(c, r.p, fee, cut, c.length); r.vscore = r.train.n >= 8 && r.valid.n >= 3 ? r.valid.pnl - 0.5 * r.valid.dd : -1e9 + r.valid.pnl; r.enough = r.train.n >= 8 && r.valid.n >= 3; }
  top.sort((a, b) => b.vscore - a.vscore);
  // the current settings on the same data, for comparison
  const cur = strategy === "follow"
    ? { bandPct: P.bandPct, levels: P.bandLevels, adds: P.bandAdds, step: lim, tp: P.autoStep ? mkt : Math.max(P.bandTp, mkt), gap: P.autoStep ? +(mkt * 0.4).toFixed(3) : P.bandTrailGap, sl: P.bandSl, pulse: P.pulseOn ? P.pulsePct : 0, pulseSec: P.pulseSec, momSl: P.momSl, momHold: P.momHoldMin, bandRef: P.bandRef }
    : { step: P.autoStep ? lim : Math.max(P.gridStepPct, lim), levels: P.gridLevels, tp: P.autoStep ? lim : Math.max(P.tpPct, lim), sl: P.slPct, makerExit: P.makerExit };
  const current = { p: cur, train: sim(c, cur, fee, 0, cut), valid: sim(c, cur, fee, cut, c.length) };
  return { tested: grid.length, candles: c.length, top: top.slice(0, 8), current };
}
module.exports = { optimize, simBand, simSell };
