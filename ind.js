// Індикатори й рушій угод для лабораторії стратегій.
"use strict";
const BT = require("./backtest");
const MAKER = 0.02, TAKER = 0.055, SLIP = 0.02;   // %
// ---------- indicators ----------
const ema = BT.ema;
function sma(a, n) { const o = new Array(a.length).fill(null); let s = 0; for (let i = 0; i < a.length; i++) { s += a[i]; if (i >= n) s -= a[i - n]; if (i >= n - 1) o[i] = s / n; } return o; }
function std(a, n, m) { const o = new Array(a.length).fill(null); for (let i = n - 1; i < a.length; i++) { let s = 0; for (let j = i - n + 1; j <= i; j++) s += (a[j] - m[i]) ** 2; o[i] = Math.sqrt(s / n); } return o; }
function atr(c, n) { const o = new Array(c.length).fill(null); let a = null; for (let i = 1; i < c.length; i++) { const tr = Math.max(c[i].h - c[i].l, Math.abs(c[i].h - c[i - 1].c), Math.abs(c[i].l - c[i - 1].c)); a = a == null ? tr : (a * (n - 1) + tr) / n; if (i >= n) o[i] = a; } return o; }
function rsi(cl, n) { const o = new Array(cl.length).fill(null); let g = 0, l = 0; for (let i = 1; i < cl.length; i++) { const d = cl[i] - cl[i - 1], up = Math.max(d, 0), dn = Math.max(-d, 0);
  if (i <= n) { g += up / n; l += dn / n; } else { g = (g * (n - 1) + up) / n; l = (l * (n - 1) + dn) / n; } if (i >= n) o[i] = l === 0 ? 100 : 100 - 100 / (1 + g / l); } return o; }

// ---------- one engine for the signal strategies ----------
// sig(i) -> +1 / -1 / 0 at the close of bar i; entry: a limit at that close (maker) on bar i+1 if touched, else skipped.
// exits: stop (taker + slippage), take (maker), trailing (taker + slippage), exitSig (at the close, taker), max bars.
function runSig(c, from, to, sig, o) {
  const A = o.atr, trades = []; let pos = null;
  for (let i = from; i < to; i++) {
    const k = c[i];
    if (pos) {
      let x = null, how = "", fee = TAKER + SLIP;
      if (pos.d > 0) {
        if (k.l <= pos.sl) { x = Math.min(pos.sl, k.o); how = "стоп"; }
        else if (pos.tp && k.h >= pos.tp) { x = pos.tp; how = "тейк"; fee = MAKER; }
      } else {
        if (k.h >= pos.sl) { x = Math.max(pos.sl, k.o); how = "стоп"; }
        else if (pos.tp && k.l <= pos.tp) { x = pos.tp; how = "тейк"; fee = MAKER; }
      }
      if (x == null && o.exitSig && o.exitSig(i, pos.d)) { x = k.c; how = "сигнал"; }
      if (x == null && o.maxBars && i - pos.i >= o.maxBars) { x = k.c; how = "час"; }
      if (x != null) { trades.push({ r: (x / pos.e - 1) * 100 * pos.d - MAKER - fee, how, i }); pos = null; }
      else if (o.trail && A[i]) { const t = pos.d > 0 ? k.c - A[i] * o.trail : k.c + A[i] * o.trail; pos.sl = pos.d > 0 ? Math.max(pos.sl, t) : Math.min(pos.sl, t); }
    }
    if (!pos && i + 1 < to && A[i]) {
      const d = sig(i); if (!d) continue;
      const e = c[i].c, n = c[i + 1];
      if ((d > 0 && n.l <= e) || (d < 0 && n.h >= e)) {    // the limit at the signal close got filled on the next bar
        pos = { d, e, i: i + 1, sl: e - d * A[i] * o.stop, tp: o.take ? (typeof o.take === "function" ? o.take(i, d, e) : e + d * A[i] * o.take) : null };
      }
    }
  }
  return trades;
}


module.exports = { MAKER, TAKER, SLIP, ema, sma, std, atr, rsi, runSig };
