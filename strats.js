// Ще ~25 найвідоміших стратегій світу для лабораторії. Кожна: сигнал на закритті свічки -> лімітний вхід, стоп / тейк / трейлінг від ATR.
"use strict";
const { ema, sma, std, atr, rsi, runSig } = require("./ind");

// ---------- extra indicators ----------
const cls = (c) => c.map(x => x.c);
function rollMax(a, n, i) { let m = -Infinity; for (let j = Math.max(0, i - n + 1); j <= i; j++) m = Math.max(m, a[j]); return m; }
function rollMin(a, n, i) { let m = Infinity; for (let j = Math.max(0, i - n + 1); j <= i; j++) m = Math.min(m, a[j]); return m; }
function macd(cl, f, s, g) { const a = ema(cl, f), b = ema(cl, s), m = a.map((x, i) => x - b[i]); return { m, sg: ema(m, g) }; }
function wma(a, n) { const o = new Array(a.length).fill(null); const w = n * (n + 1) / 2; for (let i = n - 1; i < a.length; i++) { let s = 0; for (let j = 0; j < n; j++) s += a[i - j] * (n - j); o[i] = s / w; } return o; }
function hma(cl, n) { const a = wma(cl, Math.floor(n / 2)), b = wma(cl, n), d = a.map((x, i) => (x == null || b[i] == null ? cl[i] : 2 * x - b[i])); return wma(d, Math.floor(Math.sqrt(n))); }
function supertrend(c, n, k) {
  const A = atr(c, n), dir = new Array(c.length).fill(0); let up = 0, dn = 0, d = 1;
  for (let i = 1; i < c.length; i++) { if (!A[i]) continue; const m = (c[i].h + c[i].l) / 2, bu = m + k * A[i], bd = m - k * A[i];
    if (!up || bu < up || c[i - 1].c > up) up = bu; if (!dn || bd > dn || c[i - 1].c < dn) dn = bd;
    if (d > 0 && c[i].c < dn) d = -1; else if (d < 0 && c[i].c > up) d = 1; dir[i] = d; }
  return dir;
}
function psar(c, st, mx) {
  const dir = new Array(c.length).fill(0); let d = 1, af = st, ep = c[0].h, sar = c[0].l;
  for (let i = 1; i < c.length; i++) { sar = sar + af * (ep - sar);
    if (d > 0) { sar = Math.min(sar, c[i - 1].l); if (c[i].l < sar) { d = -1; sar = ep; ep = c[i].l; af = st; } else if (c[i].h > ep) { ep = c[i].h; af = Math.min(mx, af + st); } }
    else { sar = Math.max(sar, c[i - 1].h); if (c[i].h > sar) { d = 1; sar = ep; ep = c[i].h; af = st; } else if (c[i].l < ep) { ep = c[i].l; af = Math.min(mx, af + st); } }
    dir[i] = d; }
  return dir;
}
function adx(c, n) {
  const P = new Array(c.length).fill(null), M = new Array(c.length).fill(null), X = new Array(c.length).fill(null); let tr = 0, pd = 0, md = 0, ax = null;
  for (let i = 1; i < c.length; i++) { const up = c[i].h - c[i - 1].h, dn = c[i - 1].l - c[i].l, t = Math.max(c[i].h - c[i].l, Math.abs(c[i].h - c[i - 1].c), Math.abs(c[i].l - c[i - 1].c));
    tr = tr - tr / n + t; pd = pd - pd / n + (up > dn && up > 0 ? up : 0); md = md - md / n + (dn > up && dn > 0 ? dn : 0);
    if (i >= n && tr) { P[i] = 100 * pd / tr; M[i] = 100 * md / tr; const dx = P[i] + M[i] ? 100 * Math.abs(P[i] - M[i]) / (P[i] + M[i]) : 0; ax = ax == null ? dx : (ax * (n - 1) + dx) / n; if (i >= 2 * n) X[i] = ax; } }
  return { P, M, X };
}
function stoch(c, n, d) { const H = c.map(z => z.h), L = c.map(z => z.l); const k = c.map((x, i) => { if (i < n) return null; const hh = rollMax(H, n, i), ll = rollMin(L, n, i); return hh > ll ? (x.c - ll) / (hh - ll) * 100 : 50; }); return { k, d: sma(k.map(x => x == null ? 50 : x), d) }; }
function cci(c, n) { const tp = c.map(x => (x.h + x.l + x.c) / 3), m = sma(tp, n); return tp.map((x, i) => { if (m[i] == null) return null; let md = 0; for (let j = i - n + 1; j <= i; j++) md += Math.abs(tp[j] - m[i]); md /= n; return md ? (x - m[i]) / (0.015 * md) : 0; }); }
function vwapRoll(c, n) { return c.map((x, i) => { if (i < n) return null; let pv = 0, v = 0; for (let j = i - n + 1; j <= i; j++) { const tp = (c[j].h + c[j].l + c[j].c) / 3; pv += tp * (c[j].v || 1); v += c[j].v || 1; } return pv / v; }); }
function mfi(c, n) { return c.map((x, i) => { if (i < n + 1) return null; let p = 0, m = 0; for (let j = i - n + 1; j <= i; j++) { const t = (c[j].h + c[j].l + c[j].c) / 3, t0 = (c[j - 1].h + c[j - 1].l + c[j - 1].c) / 3, f = t * (c[j].v || 1); if (t > t0) p += f; else if (t < t0) m += f; } return m ? 100 - 100 / (1 + p / m) : 100; }); }
// common shapes
const trendSig = (c, f, t, sig, stop, trail, exitSig) => runSig(c, f, t, sig, { atr: atr(c, 14), stop, trail, exitSig });
const revSig = (c, f, t, sig, stop, take, maxBars) => runSig(c, f, t, sig, { atr: atr(c, 14), stop, take, maxBars });

module.exports = [
  // ===== trend / momentum =====
  { id: "turtle", name: "Черепахи (пробій 20, вихід 10)", tf: "60", grid: [[20, 10, 2], [55, 20, 2]], run(c, f, t, [n, x, st]) {
      const H = c.map(z => z.h), L = c.map(z => z.l);
      return trendSig(c, f, t, (i) => i <= n ? 0 : c[i].c > rollMax(H, n, i - 1) ? 1 : c[i].c < rollMin(L, n, i - 1) ? -1 : 0, st, 0,
        (i, d) => d > 0 ? c[i].c < rollMin(L, x, i - 1) : c[i].c > rollMax(H, x, i - 1)); } },
  { id: "macd", name: "MACD: перетин сигнальної", tf: "15", grid: [[12, 26, 9, 2], [12, 26, 9, 3]], run(c, f, t, [a, b, g, st]) {
      const { m, sg } = macd(cls(c), a, b, g);
      return trendSig(c, f, t, (i) => i > b && m[i - 1] <= sg[i - 1] && m[i] > sg[i] ? 1 : i > b && m[i - 1] >= sg[i - 1] && m[i] < sg[i] ? -1 : 0, st, st, (i, d) => d > 0 ? m[i] < sg[i] : m[i] > sg[i]); } },
  { id: "macd0", name: "MACD: перетин нуля", tf: "60", grid: [[12, 26, 2], [12, 26, 3]], run(c, f, t, [a, b, st]) {
      const { m } = macd(cls(c), a, b, 9);
      return trendSig(c, f, t, (i) => i > b && m[i - 1] <= 0 && m[i] > 0 ? 1 : i > b && m[i - 1] >= 0 && m[i] < 0 ? -1 : 0, st, st, (i, d) => d * m[i] < 0); } },
  { id: "super", name: "Supertrend", tf: "15", grid: [[10, 3], [10, 2], [14, 4]], run(c, f, t, [n, k]) {
      const D = supertrend(c, n, k);
      return trendSig(c, f, t, (i) => D[i] !== D[i - 1] && D[i - 1] ? D[i] : 0, k, k, (i, d) => D[i] === -d); } },
  { id: "psar", name: "Parabolic SAR", tf: "15", grid: [[0.02, 0.2], [0.01, 0.1]], run(c, f, t, [a, m]) {
      const D = psar(c, a, m);
      return trendSig(c, f, t, (i) => D[i] !== D[i - 1] && D[i - 1] ? D[i] : 0, 3, 0, (i, d) => D[i] === -d); } },
  { id: "adx", name: "ADX + DI (тільки сильний тренд)", tf: "15", grid: [[14, 25, 2], [14, 20, 3]], run(c, f, t, [n, lvl, st]) {
      const { P, M, X } = adx(c, n);
      return trendSig(c, f, t, (i) => X[i] == null || X[i] < lvl ? 0 : P[i - 1] <= M[i - 1] && P[i] > M[i] ? 1 : P[i - 1] >= M[i - 1] && P[i] < M[i] ? -1 : 0, st, st); } },
  { id: "ichi", name: "Ішимоку (Тенкан/Кіджун + хмара)", tf: "60", grid: [[2], [3]], run(c, f, t, [st]) {
      const H = c.map(z => z.h), L = c.map(z => z.l), mid = (n, i) => (rollMax(H, n, i) + rollMin(L, n, i)) / 2;
      return trendSig(c, f, t, (i) => { if (i < 80) return 0; const tk = mid(9, i), kj = mid(26, i), tk0 = mid(9, i - 1), kj0 = mid(26, i - 1), sa = (mid(9, i - 26) + mid(26, i - 26)) / 2, sb = mid(52, i - 26), top = Math.max(sa, sb), bot = Math.min(sa, sb);
        return tk0 <= kj0 && tk > kj && c[i].c > top ? 1 : tk0 >= kj0 && tk < kj && c[i].c < bot ? -1 : 0; }, st, st); } },
  { id: "ribbon", name: "Стрічка EMA 8/21/55 + відкат", tf: "15", grid: [[2], [3]], run(c, f, t, [st]) {
      const cl = cls(c), a = ema(cl, 8), b = ema(cl, 21), d = ema(cl, 55);
      return trendSig(c, f, t, (i) => i < 60 ? 0 : a[i] > b[i] && b[i] > d[i] && c[i].l <= b[i] && c[i].c > b[i] ? 1 : a[i] < b[i] && b[i] < d[i] && c[i].h >= b[i] && c[i].c < b[i] ? -1 : 0, st, st, (i, dd) => dd > 0 ? a[i] < b[i] : a[i] > b[i]); } },
  { id: "kelt", name: "Пробій каналу Кельтнера", tf: "15", grid: [[20, 2, 2], [20, 1.5, 3]], run(c, f, t, [n, k, st]) {
      const m = ema(cls(c), n), A = atr(c, n);
      return trendSig(c, f, t, (i) => !A[i] ? 0 : c[i].c > m[i] + k * A[i] && c[i - 1].c <= m[i - 1] + k * A[i - 1] ? 1 : c[i].c < m[i] - k * A[i] && c[i - 1].c >= m[i - 1] - k * A[i - 1] ? -1 : 0, st, st, (i, d) => d > 0 ? c[i].c < m[i] : c[i].c > m[i]); } },
  { id: "ha", name: "Хейкін-Аші (3 свічки в один бік)", tf: "15", grid: [[2], [3]], run(c, f, t, [st]) {
      const h = []; for (let i = 0; i < c.length; i++) { const cc = (c[i].o + c[i].h + c[i].l + c[i].c) / 4, oo = i ? (h[i - 1].o + h[i - 1].c) / 2 : c[i].o; h.push({ o: oo, c: cc }); }
      const col = (i) => Math.sign(h[i].c - h[i].o);
      return trendSig(c, f, t, (i) => i < 4 ? 0 : col(i) === col(i - 1) && col(i) === col(i - 2) && col(i - 3) !== col(i) ? col(i) : 0, st, st, (i, d) => col(i) === -d && col(i - 1) === -d); } },
  { id: "roc", name: "Імпульс ROC (різкий рух → за ним)", tf: "15", grid: [[12, 1.5, 2], [24, 3, 2]], run(c, f, t, [n, th, st]) {
      const cl = cls(c);
      return trendSig(c, f, t, (i) => { if (i < n + 1) return 0; const r = (cl[i] / cl[i - n] - 1) * 100, r0 = (cl[i - 1] / cl[i - 1 - n] - 1) * 100; return r > th && r0 <= th ? 1 : r < -th && r0 >= -th ? -1 : 0; }, st, st); } },
  { id: "tsm", name: "Часовий імпульс (знак доби, тримати добу)", tf: "60", grid: [[24, 24], [72, 24]], run(c, f, t, [lb, hold]) {
      const cl = cls(c);
      return runSig(c, f, t, (i) => i < lb || i % hold ? 0 : Math.sign(cl[i] - cl[i - lb]), { atr: atr(c, 14), stop: 4, maxBars: hold }); } },
  { id: "hull", name: "Hull MA (зміна нахилу)", tf: "15", grid: [[21, 2], [55, 3]], run(c, f, t, [n, st]) {
      const H = hma(cls(c), n), s = (i) => (H[i] != null && H[i - 1] != null ? Math.sign(H[i] - H[i - 1]) : 0);
      return trendSig(c, f, t, (i) => i < n + 3 ? 0 : s(i) !== s(i - 1) && s(i - 1) ? s(i) : 0, st, st, (i, d) => s(i) === -d); } },
  { id: "orb", name: "Пробій діапазону першої години доби (UTC)", tf: "15", grid: [[4, 2], [8, 2]], run(c, f, t, [bars, st]) {
      const day = (i) => Math.floor((c[i].t || i * 900000) / 86400e3), first = {};
      for (let i = 0; i < c.length; i++) { const d = day(i); if (!first[d]) first[d] = { s: i, hi: -Infinity, lo: Infinity }; const F = first[d]; if (i - F.s < bars) { F.hi = Math.max(F.hi, c[i].h); F.lo = Math.min(F.lo, c[i].l); } }
      const used = {};
      return trendSig(c, f, t, (i) => { const F = first[day(i)]; if (!F || i - F.s < bars || used[day(i)]) return 0; const d = c[i].c > F.hi ? 1 : c[i].c < F.lo ? -1 : 0; if (d) used[day(i)] = 1; return d; }, st, st); } },
  // ===== mean reversion =====
  { id: "stoch", name: "Стохастик (вихід із перепроданості)", tf: "15", grid: [[14, 20, 2], [14, 10, 2]], run(c, f, t, [n, lv, st]) {
      const { k, d } = stoch(c, n, 3);
      return revSig(c, f, t, (i) => k[i] == null || k[i - 1] == null ? 0 : k[i - 1] < d[i - 1] && k[i] > d[i] && k[i] < lv ? 1 : k[i - 1] > d[i - 1] && k[i] < d[i] && k[i] > 100 - lv ? -1 : 0, st, 1.5, 32); } },
  { id: "willr", name: "Williams %R", tf: "15", grid: [[14, 2], [28, 2]], run(c, f, t, [n, st]) {
      const H = c.map(z => z.h), L = c.map(z => z.l), w = (i) => { const hh = rollMax(H, n, i), ll = rollMin(L, n, i); return hh > ll ? (hh - c[i].c) / (hh - ll) * -100 : -50; };
      return revSig(c, f, t, (i) => i < n + 1 ? 0 : w(i - 1) < -80 && w(i) >= -80 ? 1 : w(i - 1) > -20 && w(i) <= -20 ? -1 : 0, st, 1.5, 32); } },
  { id: "cci", name: "CCI (повернення з ±200)", tf: "15", grid: [[20, 200, 2], [20, 100, 2]], run(c, f, t, [n, lv, st]) {
      const C = cci(c, n);
      return revSig(c, f, t, (i) => C[i] == null || C[i - 1] == null ? 0 : C[i - 1] < -lv && C[i] >= -lv ? 1 : C[i - 1] > lv && C[i] <= lv ? -1 : 0, st, 1.5, 32); } },
  { id: "vwap", name: "Повернення до VWAP", tf: "15", grid: [[96, 1.5, 2], [96, 2.5, 3]], run(c, f, t, [n, k, st]) {
      const V = vwapRoll(c, n), A = atr(c, 14);
      return runSig(c, f, t, (i) => !V[i] || !A[i] ? 0 : c[i].c < V[i] - k * A[i] ? 1 : c[i].c > V[i] + k * A[i] ? -1 : 0, { atr: A, stop: st, take: (i, d, e) => ((V[i] - e) * d > 0 ? V[i] : e + d * A[i]), maxBars: 48 }); } },
  { id: "rsi2", name: "RSI-2 Коннорса (з фільтром EMA200)", tf: "15", grid: [[5, 2], [10, 2]], run(c, f, t, [lv, st]) {
      const cl = cls(c), R = rsi(cl, 2), E = ema(cl, 200), M = sma(cl, 5);
      return runSig(c, f, t, (i) => i < 200 || R[i] == null ? 0 : cl[i] > E[i] && R[i] < lv ? 1 : cl[i] < E[i] && R[i] > 100 - lv ? -1 : 0, { atr: atr(c, 14), stop: st, exitSig: (i, d) => d > 0 ? cl[i] > M[i] : cl[i] < M[i], maxBars: 24 }); } },
  { id: "z", name: "Z-score повернення до середнього", tf: "15", grid: [[48, 2, 2], [96, 2.5, 3]], run(c, f, t, [n, z, st]) {
      const cl = cls(c), M = sma(cl, n), D = std(cl, n, M);
      return runSig(c, f, t, (i) => !D[i] ? 0 : (cl[i] - M[i]) / D[i] < -z ? 1 : (cl[i] - M[i]) / D[i] > z ? -1 : 0, { atr: atr(c, 14), stop: st, take: (i, d, e) => ((M[i] - e) * d > 0 ? M[i] : e + d * D[i]), maxBars: n }); } },
  { id: "keltr", name: "Відскок від каналу Кельтнера", tf: "15", grid: [[20, 2.5, 2], [20, 2, 2]], run(c, f, t, [n, k, st]) {
      const m = ema(cls(c), n), A = atr(c, n);
      return runSig(c, f, t, (i) => !A[i] ? 0 : c[i].c < m[i] - k * A[i] ? 1 : c[i].c > m[i] + k * A[i] ? -1 : 0, { atr: A, stop: st, take: (i, d, e) => ((m[i] - e) * d > 0 ? m[i] : e + d * A[i]), maxBars: 32 }); } },
  { id: "mfi", name: "MFI (індекс грошового потоку)", tf: "15", grid: [[14, 20, 2], [14, 10, 2]], run(c, f, t, [n, lv, st]) {
      const M = mfi(c, n);
      return revSig(c, f, t, (i) => M[i] == null || M[i - 1] == null ? 0 : M[i - 1] < lv && M[i] >= lv ? 1 : M[i - 1] > 100 - lv && M[i] <= 100 - lv ? -1 : 0, st, 1.5, 32); } },
  // ===== volatility / volume =====
  { id: "nr7", name: "NR7: найвужча свічка за 7 → пробій", tf: "60", grid: [[2], [3]], run(c, f, t, [st]) {
      const nr = (i) => { const r = c[i].h - c[i].l; for (let j = i - 6; j < i; j++) if (c[j].h - c[j].l <= r) return false; return true; };
      return trendSig(c, f, t, (i) => i < 8 || !nr(i - 1) ? 0 : c[i].c > c[i - 1].h ? 1 : c[i].c < c[i - 1].l ? -1 : 0, st, st); } },
  { id: "atrb", name: "Пробій на ATR (свічка > k·ATR)", tf: "15", grid: [[1.5, 2], [2.5, 3]], run(c, f, t, [k, st]) {
      const A = atr(c, 14);
      return trendSig(c, f, t, (i) => !A[i - 1] ? 0 : c[i].c - c[i - 1].c > k * A[i - 1] ? 1 : c[i - 1].c - c[i].c > k * A[i - 1] ? -1 : 0, st, st); } },
  { id: "obv", name: "OBV: новий максимум обсягу разом із ціною", tf: "15", grid: [[50, 2], [100, 3]], run(c, f, t, [n, st]) {
      const O = [0]; for (let i = 1; i < c.length; i++) O.push(O[i - 1] + Math.sign(c[i].c - c[i - 1].c) * (c[i].v || 1));
      const H = c.map(z => z.h), L = c.map(z => z.l);
      return trendSig(c, f, t, (i) => i <= n ? 0 : O[i] > rollMax(O, n, i - 1) && c[i].c > rollMax(H, n, i - 1) ? 1 : O[i] < rollMin(O, n, i - 1) && c[i].c < rollMin(L, n, i - 1) ? -1 : 0, st, st); } },
  { id: "vspike", name: "Сплеск обсягу + сильна свічка → за нею", tf: "15", grid: [[3, 2], [5, 2]], run(c, f, t, [k, st]) {
      const V = sma(c.map(z => z.v || 1), 48);
      return trendSig(c, f, t, (i) => { if (!V[i - 1]) return 0; const body = Math.abs(c[i].c - c[i].o), rng = c[i].h - c[i].l; return (c[i].v || 1) > k * V[i - 1] && rng && body / rng > 0.6 ? Math.sign(c[i].c - c[i].o) : 0; }, st, st); } },
];
