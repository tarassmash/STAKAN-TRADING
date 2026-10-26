// ⚡ Лента сделок Bybit в реальном времени (WebSocket publicTrade): скорость цены, агрессия покупателей / продавцов, ускорение ленты.
// Детектор начала движения реагирует на КАЖДУЮ сделку — без ожидания цикла.
"use strict";
const WS = (() => { try { return require("ws"); } catch (e) { return null; } })();

function create(host) {
  const T = {};                 // symbol -> { trades: [{t,p,v,buy}], last }
  let ws = null, subs = new Set(), url = "", ping = null, up = false;
  const now = () => Date.now();

  function connect() {
    url = host.url(); if (!WS || !url) return;
    try { ws = new WS(url); } catch (e) { setTimeout(connect, 3000); return; }
    ws.on("open", () => { up = true; const a = [...subs].map(s => "publicTrade." + s); if (a.length) ws.send(JSON.stringify({ op: "subscribe", args: a })); clearInterval(ping); ping = setInterval(() => { try { ws.send(JSON.stringify({ op: "ping" })); } catch (e) { /* closed */ } }, 20000); });
    ws.on("message", (raw) => {
      let j; try { j = JSON.parse(String(raw)); } catch (e) { return; }
      if (!j.topic || !j.topic.startsWith("publicTrade.") || !Array.isArray(j.data)) return;
      const sym = j.topic.slice(12), s = T[sym]; if (!s) return;
      for (const x of j.data) s.trades.push({ t: Number(x.T) || now(), p: Number(x.p), v: Number(x.v) * Number(x.p), buy: x.S === "Buy" });
      const cut = now() - 5 * 60000; let i = 0; while (i < s.trades.length && s.trades[i].t < cut) i++; if (i) s.trades.splice(0, i);
      if (s.trades.length) s.last = s.trades[s.trades.length - 1].p;
      if (host.onTrade) host.onTrade(sym);
    });
    ws.on("close", () => { up = false; clearInterval(ping); setTimeout(connect, 2000); });
    ws.on("error", () => {});
  }
  // keep exactly these symbols subscribed
  function watch(list) {
    const want = new Set(list.map(s => s.toUpperCase()).filter(Boolean));
    if (!ws) connect();
    const add = [...want].filter(s => !subs.has(s)), del = [...subs].filter(s => !want.has(s));
    for (const s of add) T[s] = T[s] || { trades: [], last: 0 };
    for (const s of del) delete T[s];
    subs = want;
    if (up && ws) { try { if (add.length) ws.send(JSON.stringify({ op: "subscribe", args: add.map(s => "publicTrade." + s) })); if (del.length) ws.send(JSON.stringify({ op: "unsubscribe", args: del.map(s => "publicTrade." + s) })); } catch (e) { /* reconnect handles it */ } }
  }
  // the state of the tape over the last ms
  function flow(sym, ms) {
    const s = T[sym]; if (!s || s.trades.length < 3) return null;
    const t0 = now() - ms, w = s.trades.filter(x => x.t >= t0); if (w.length < 3) return null;
    const first = s.trades.find(x => x.t >= t0) || w[0], prev = s.trades[s.trades.indexOf(first) - 1] || first;
    const bv = w.reduce((a, x) => a + (x.buy ? x.v : 0), 0), tv = w.reduce((a, x) => a + x.v, 0);
    const span5 = Math.max(60000, now() - s.trades[0].t), rateNow = w.length / (ms / 1000), rateAvg = s.trades.length / (span5 / 1000);
    return { mv: +((s.last / prev.p - 1) * 100).toFixed(3), buyShare: tv ? +(bv / tv).toFixed(2) : 0.5, speedX: rateAvg ? +(rateNow / rateAvg).toFixed(2) : 1, n: w.length, last: s.last };
  }
  return { watch, flow, up: () => up };
}
module.exports = { create };
