// Тренд і перевірка сітки на історії. Чисті функції (без мережі), щоб їх можна було тестувати окремо.
"use strict";

function ema(arr, n) {
  const k = 2 / (n + 1); let e = arr[0]; const out = [e];
  for (let i = 1; i < arr.length; i++) { e = arr[i] * k + e * (1 - k); out.push(e); }
  return out;
}

// trend from 1-hour closes (oldest first): EMA20 vs EMA50, the slope of EMA20 over the last 6 hours, the price vs EMA20
// -> "long" (up), "short" (down) or "neutral" (flat / unclear)
function trendOf(closes, thrPct = 0.8) {
  if (!closes || closes.length < 30) return { mode: "neutral", slope: 0, why: "мало історії" };
  const e20 = ema(closes, 20), e50 = ema(closes, 50), n = closes.length - 1;
  const slope = (e20[n] / e20[Math.max(0, n - 6)] - 1) * 100;
  const chg24 = (closes[n] / closes[Math.max(0, n - 24)] - 1) * 100;
  const up = e20[n] > e50[n] && slope >= thrPct && closes[n] > e20[n];
  const down = e20[n] < e50[n] && slope <= -thrPct && closes[n] < e20[n];
  return { mode: up ? "long" : down ? "short" : "neutral", slope: +slope.toFixed(2), chg24: +chg24.toFixed(2), strong: Math.abs(slope) >= thrPct * 2,
    why: up ? `росте: EMA20 > EMA50, нахил +${slope.toFixed(2)}% за 6 год` : down ? `падає: EMA20 < EMA50, нахил ${slope.toFixed(2)}% за 6 год` : `флет: нахил ${slope.toFixed(2)}% за 6 год` };
}

// grid simulation on OHLC candles (oldest first) with the same rules as the live engine:
// cells, counter orders, trailing, grid stop and a new grid after the stop; maker fee on limits, taker on market orders
function simulate(c, o) {
  const { mode, widthPct, n, investment, lev, maker = 0.02, taker = 0.055, slPct = 3, trailing = true, autoRestart = true, minNotional = 5,
    breakout = false, bkTrigger = 1, bkMult = 1, bkStopPct = 1.5, bkTrailStart = 1, bkTrailGap = 0.7 } = o;
  let bk = null, breakouts = 0, bkProfit = 0;
  const mk = maker / 100, tk = taker / 100;
  let cells = [], low = 0, high = 0, r = 1, qty = 0;
  let profit = 0, cycles = 0, stops = 0, trails = 0, fees = 0, maxDD = 0, peak = 0;
  function build(p) {
    low = p * (1 - widthPct / 200); high = p * (1 + widthPct / 200); r = Math.pow(high / low, 1 / n);
    qty = Math.max(investment * lev / n / p, minNotional * 1.05 / low);
    cells = [];
    for (let i = 0; i < n; i++) {
      const lo = low * Math.pow(r, i), hi = lo * r, below = hi <= p, above = lo >= p;
      let dir, state = "open", openPx = null;
      if (mode === "long") { dir = 1; if (!below) { state = "close"; openPx = p; fees += qty * p * tk; } }
      else if (mode === "short") { dir = -1; if (!above) { state = "close"; openPx = p; fees += qty * p * tk; } }
      else dir = below || (!above && p - lo > hi - p) ? 1 : -1;
      cells.push({ lo, hi, dir, state, openPx });
    }
  }
  const unreal = (x) => cells.reduce((s, q) => s + (q.state === "close" ? (x - q.openPx) * qty * q.dir : 0), 0);
  function step(x) {
    // a breakout trade runs instead of the grid: stop for a false breakout, trailing take from bkTrailStart
    if (bk) {
      bk.peak = bk.dir > 0 ? Math.max(bk.peak, x) : Math.min(bk.peak, x);
      const moved = (bk.peak / bk.entry - 1) * 100 * bk.dir;
      let stop = bk.entry * (1 - bk.dir * bkStopPct / 100);
      if (moved >= bkTrailStart) { const t = bk.peak * (1 - bk.dir * bkTrailGap / 100); stop = bk.dir > 0 ? Math.max(stop, t) : Math.min(stop, t); }
      if ((bk.dir > 0 && x <= stop) || (bk.dir < 0 && x >= stop)) {
        const pr = (x - bk.entry) * bk.q * bk.dir; profit += pr; bkProfit += pr; fees += bk.q * x * tk; bk = null;
        if (autoRestart) build(x); else { cells = []; return false; }
      }
      return true;
    }
    // fills in the order the price meets them
    for (const q of cells) {
      if (q.state === "open") {
        if (q.dir > 0 && x <= q.lo) { q.state = "close"; q.openPx = q.lo; fees += qty * q.lo * mk; }
        else if (q.dir < 0 && x >= q.hi) { q.state = "close"; q.openPx = q.hi; fees += qty * q.hi * mk; }
      } else {
        if (q.dir > 0 && x >= q.hi) { profit += (q.hi - q.openPx) * qty; fees += qty * q.hi * mk; cycles++; q.state = "open"; q.openPx = null; }
        else if (q.dir < 0 && x <= q.lo) { profit += (q.openPx - q.lo) * qty; fees += qty * q.lo * mk; cycles++; q.state = "open"; q.openPx = null; }
      }
    }
    // trailing (up to 3 cells per point)
    for (let k = 0; trailing && k < 3; k++) {
      if (x > high * r) {
        const q = cells.filter(z => z.state === "open").sort((a, b) => a.lo - b.lo)[0];
        if (!q || q.lo >= low * r * r) break;
        q.lo = high; q.hi = high * r; q.dir = mode === "long" ? 1 : -1; q.state = "open"; q.openPx = null;
        if (mode === "long") { q.state = "close"; q.openPx = x; fees += qty * x * tk; }
      } else if (x < low / r) {
        const q = cells.filter(z => z.state === "open").sort((a, b) => b.hi - a.hi)[0];
        if (!q || q.hi <= high / r / r) break;
        q.hi = low; q.lo = low / r; q.dir = mode === "short" ? -1 : 1; q.state = "open"; q.openPx = null;
        if (mode === "short") { q.state = "close"; q.openPx = x; fees += qty * x * tk; }
      } else break;
      trails++;
      cells.sort((a, b) => a.lo - b.lo); low = cells[0].lo; high = cells[cells.length - 1].hi;
    }
    // grid stop
    const longs = cells.some(q => q.state === "close" && q.dir > 0), shorts = cells.some(q => q.state === "close" && q.dir < 0);
    const k = breakout ? bkTrigger : slPct;
    if (breakout && ((longs && x <= low * (1 - k / 100)) || (shorts && x >= high * (1 + k / 100)))) {
      const held = cells.filter(q => q.state === "close");
      profit += unreal(x); fees += held.length * qty * x * tk; breakouts++;
      const dir = x >= high ? 1 : -1, q = investment * lev * bkMult / x;
      bk = { dir, entry: x, peak: x, q }; fees += q * x * tk; cells = [];
      return true;
    }
    if ((longs && x <= low * (1 - slPct / 100)) || (shorts && x >= high * (1 + slPct / 100))) {
      const held = cells.filter(q => q.state === "close");
      profit += unreal(x); fees += held.length * qty * x * tk; stops++;
      if (autoRestart) build(x); else { cells = []; return false; }
    }
    const eq = profit - fees + unreal(x) + (bk ? (x - bk.entry) * bk.q * bk.dir : 0);
    peak = Math.max(peak, eq); maxDD = Math.max(maxDD, peak - eq);
    return true;
  }
  if (!c.length) return null;
  build(c[0].o);
  for (const k of c) {
    const path = k.c >= k.o ? [k.o, k.l, k.h, k.c] : [k.o, k.h, k.l, k.c];
    let alive = true;
    for (const x of path) { if (!(alive = step(x))) break; }
    if (!alive) break;
  }
  const last = c[c.length - 1].c;
  const net = profit - fees + (cells.length ? unreal(last) : 0) + (bk ? (last - bk.entry) * bk.q * bk.dir : 0);
  return { net: +net.toFixed(4), roi: +(net / investment * 100).toFixed(2), cycles, stops, trails, breakouts, bkProfit: +bkProfit.toFixed(4), fees: +fees.toFixed(4), maxDD: +(maxDD / investment * 100).toFixed(2), qty };
}

module.exports = { ema, trendOf, simulate };

// breakout grid (live engine bgrid.js): the direction comes from the breakout/trend on 5-minute candles (EMA fast vs slow with a band);
// every level crossed - up or down - opens a unit in that direction with a micro take one level further; a pullback is bought (in an uptrend)
// with a short take; one "runner" unit trails stopSteps behind the best price to take the whole trend; a direction flip closes everything.
// direction-following breakout grid
function simulateB2(c, o) {
  const { stepPct, maxUnits = 6, investment, lev, maker = 0.02, taker = 0.055, stopSteps = 3, runner = true, trailSteps = 2, fast = 36, slow = 96, band = 0.002 } = o;
  const r = 1 + stepPct / 100, a0 = c[0].o, lr = Math.log(r);
  const L = (j) => a0 * Math.pow(r, j), idx = (x) => Math.floor(Math.log(x / a0) / lr);
  const closes = c.map(k => k.c), ef = ema(closes, fast), es = ema(closes, slow);
  let last = idx(a0), units = [], profit = 0, fees = 0, wins = 0, losses = 0, D = 0, run = null, flips = 0;
  const q = investment * lev / (maxUnits + 1) / a0;
  const unreal = (x) => units.reduce((s, u) => s + (x - u.e) * q * u.d, 0) + (run ? (x - run.e) * q * run.d : 0);
  function closeAll(x) { for (const u of units) { profit += (x - u.e) * q * u.d; fees += q * x * taker / 100; (x - u.e) * u.d > 0 ? wins++ : losses++; } units = []; if (run) { profit += (x - run.e) * q * run.d; fees += q * x * taker / 100; run = null; } }
  for (let t = 0; t < c.length; t++) {
    const k = c[t], dNew = ef[t] > es[t] * (1 + band) ? 1 : ef[t] < es[t] * (1 - band) ? -1 : 0;
    if (t > slow && dNew !== D) { if (D !== 0) { closeAll(k.o); flips++; } D = dNew; }
    for (const x of (k.c >= k.o ? [k.o, k.l, k.h, k.c] : [k.o, k.h, k.l, k.c])) {
      units = units.filter(u => {
        if ((u.d > 0 && x >= u.tp) || (u.d < 0 && x <= u.tp)) { profit += (u.tp - u.e) * q * u.d; fees += q * u.tp * maker / 100; wins++; return false; }
        if ((u.d > 0 && x <= u.sl) || (u.d < 0 && x >= u.sl)) { profit += (u.sl - u.e) * q * u.d; fees += q * u.sl * taker / 100; losses++; return false; }
        return true; });
      if (run) { run.pk = run.d > 0 ? Math.max(run.pk, x) : Math.min(run.pk, x); const st = run.d > 0 ? Math.max(run.sl, run.pk / Math.pow(r, trailSteps)) : Math.min(run.sl, run.pk * Math.pow(r, trailSteps)); run.sl = st;
        if ((run.d > 0 && x <= st) || (run.d < 0 && x >= st)) { profit += (st - run.e) * q * run.d; fees += q * st * taker / 100; (st - run.e) * run.d > 0 ? wins++ : losses++; run = null; } }
      const i = idx(x);
      if (D !== 0) {
        const crossed = []; for (let j = last + 1; j <= i && j <= last + 3; j++) crossed.push([j, 1]); for (let j = last; j > i && j > last - 3; j--) crossed.push([j, -1]);
        for (const [j, way] of crossed) {
          const lvl = way > 0 ? j : j; // the level crossed
          const e = L(lvl);
          if (units.length < maxUnits && !units.some(u => u.j === lvl)) {
            const viaLimit = way !== D;                 // a pullback: the entry is a resting limit (maker); continuation: market (taker)
            units.push({ d: D, j: lvl, e, tp: L(lvl + D), sl: L(lvl - D * stopSteps) }); fees += q * e * (viaLimit ? maker : taker) / 100;
          }
          if (runner && !run && way === D) { run = { d: D, e, pk: e, sl: L(lvl - D * trailSteps) }; fees += q * e * taker / 100; }
        }
      }
      last = i;
    }
  }
  const x = c[c.length - 1].c, net = profit - fees + unreal(x);
  return { roi: +(net / investment * 100).toFixed(2), wins, losses, flips };
}
module.exports.simulateB2 = simulateB2;
