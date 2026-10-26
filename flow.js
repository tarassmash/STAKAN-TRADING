// 📊 ПОТІК РИНКУ для монети бота: стакан (orderbook.50), стрічка (publicTrade), швидкість стрічки, індикатори, відкритий інтерес.
// snapshot() -> усе в цифрах + score від -1 (тиснуть продавці) до +1 (тиснуть покупці).
"use strict";
const BT = require("./backtest");

function create(host) {
  const { bx } = host;
  let sym = "", bids = new Map(), asks = new Map(), trades = [], ind = null, indAt = 0, oi = null;
  const now = () => Date.now();
  function reset(s) { sym = s; bids = new Map(); asks = new Map(); trades = []; ind = null; indAt = 0; oi = null; }
  function onBook(msg) {
    const d = msg.data; if (!d || d.s !== sym) return;
    if (msg.type === "snapshot") { bids = new Map(); asks = new Map(); }
    for (const [p, q] of d.b || []) { if (Number(q) === 0) bids.delete(p); else bids.set(p, Number(q)); }
    for (const [p, q] of d.a || []) { if (Number(q) === 0) asks.delete(p); else asks.set(p, Number(q)); }
  }
  function onTrades(list) {
    for (const t of list || []) if (t.s === sym || !t.s) trades.push({ t: Number(t.T) || now(), buy: t.S === "Buy", v: Number(t.v), p: Number(t.p) });
    const cut = now() - 15 * 60000; let i = 0; while (i < trades.length && trades[i].t < cut) i++; if (i) trades.splice(0, i);
  }
  async function refresh() {   // indicators on 1-minute candles + open interest, once a minute
    if (!sym || now() - indAt < 60000) return; indAt = now();
    try {
      const k = await bx("/v5/market/kline", { query: { category: "linear", symbol: sym, interval: "1", limit: "100" } });
      const cs = (k.list || []).map(x => ({ h: Number(x[2]), l: Number(x[3]), c: Number(x[4]), v: Number(x[5]) })).reverse();
      if (cs.length >= 30) {
        const cl = cs.map(x => x.c), n = cl.length - 1, e9 = BT.ema(cl, 9), e21 = BT.ema(cl, 21);
        let g = 0, l = 0; for (let i = n - 13; i <= n; i++) { const d = cl[i] - cl[i - 1]; if (d > 0) g += d; else l -= d; }
        const rsi = l === 0 ? 100 : 100 - 100 / (1 + g / l);
        const atr = cs.slice(-14).reduce((a, x) => a + (x.h - x.l) / x.c, 0) / 14 * 100;
        const last60 = cs.slice(-60), vw = last60.reduce((a, x) => a + (x.h + x.l + x.c) / 3 * x.v, 0) / Math.max(1e-12, last60.reduce((a, x) => a + x.v, 0));
        ind = { rsi: +rsi.toFixed(1), emaDiff: +((e9[n] / e21[n] - 1) * 100).toFixed(3), atr: +atr.toFixed(3), vwapDist: +((cl[n] / vw - 1) * 100).toFixed(2), move15: +((cl[n] / cl[Math.max(0, n - 15)] - 1) * 100).toFixed(2) };
      }
      const o = await bx("/v5/market/open-interest", { query: { category: "linear", symbol: sym, intervalTime: "5min", limit: "7" } });
      const L = (o.list || []).map(x => Number(x.openInterest)); if (L.length >= 7 && L[6] > 0) oi = { ch30: +((L[0] / L[6] - 1) * 100).toFixed(2), ch5: +((L[0] / L[1] - 1) * 100).toFixed(2) };
    } catch (e) { /* next minute */ }
  }
  function snapshot() {
    const t = now(), w = (ms) => trades.filter(x => x.t >= t - ms);
    const m1 = w(60000), m5 = w(300000);
    const vol = (a) => a.reduce((s, x) => s + x.v, 0), cvd = (a) => a.reduce((s, x) => s + (x.buy ? x.v : -x.v), 0);
    const v1 = vol(m1), v15 = vol(trades), span = Math.max(60000, Math.min(15 * 60000, t - (trades[0] ? trades[0].t : t)));
    const rate1 = m1.length / 60, rate15 = trades.length / (span / 1000);
    const vSpeed = v15 > 0 ? (v1 / 60) / (v15 / (span / 1000)) : 1;
    // the book near the price: within 0.3 % of the mid
    let bb = -Infinity, ba = Infinity; for (const p of bids.keys()) bb = Math.max(bb, Number(p)); for (const p of asks.keys()) ba = Math.min(ba, Number(p));
    let imb = 0, spreadBps = null, wallBid = null, wallAsk = null;
    if (bids.size && asks.size && isFinite(bb) && isFinite(ba)) {
      const mid = (bb + ba) / 2; spreadBps = +((ba - bb) / mid * 1e4).toFixed(1);
      let bv = 0, av = 0, mb = 0, ma = 0;
      for (const [p, q] of bids) { const x = Number(p); if (x >= mid * 0.997) { bv += q; if (q > mb) { mb = q; wallBid = x; } } }
      for (const [p, q] of asks) { const x = Number(p); if (x <= mid * 1.003) { av += q; if (q > ma) { ma = q; wallAsk = x; } } }
      imb = bv + av ? +((bv - av) / (bv + av)).toFixed(2) : 0;
      const avg = (bv + av) / Math.max(1, [...bids.keys()].length + [...asks.keys()].length);
      if (mb < avg * 4) wallBid = null; if (ma < avg * 4) wallAsk = null;   // a wall = a level at least 4× the average
    }
    const big = m1.length ? m1.filter(x => x.v >= vol(m1) / m1.length * 8) : [];
    const cvd1 = v1 ? +(cvd(m1) / v1).toFixed(2) : 0, cvd5 = vol(m5) ? +(cvd(m5) / vol(m5)).toFixed(2) : 0;
    const trend = ind ? Math.max(-1, Math.min(1, ind.emaDiff / 0.2)) : 0;
    // the flow: the tape weighs most, then the book, then the 1-minute trend
    const score = +Math.max(-1, Math.min(1, 0.45 * cvd1 + 0.15 * cvd5 + 0.25 * imb + 0.15 * trend)).toFixed(2);
    return { sym, score, cvd1, cvd5, imb, spreadBps, wallBid, wallAsk, tps: +rate1.toFixed(2), speedX: rate15 ? +(rate1 / rate15).toFixed(2) : 1, volSpeedX: +vSpeed.toFixed(2),
      bigBuys: big.filter(x => x.buy).length, bigSells: big.filter(x => !x.buy).length, ind, oi, trades1m: m1.length };
  }
  return { reset, onBook, onTrades, refresh, snapshot, sym: () => sym };
}
module.exports = { create };
