// SELL GRID — SHORT (режим Щукина) 24/7 на Railway: торгует сервер, страница только показывает и управляет.
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 8080);
const PASSWORD = (process.env.UI_PASSWORD || "").trim();
const DATA_DIR = process.env.DATA_DIR || (fs.existsSync("/data") ? "/data" : path.join(__dirname, "data"));
const STORE = path.join(DATA_DIR, "storage.json");
const ENV = (process.env.BYBIT_ENV || "mainnet").trim().toLowerCase() === "testnet" ? "testnet" : "mainnet";
const KEY = (process.env.BYBIT_API_KEY || "").trim();
const SECRET = (process.env.BYBIT_API_SECRET || "").trim();
const HAS_KEYS = !!(KEY && SECRET);
const REST = (process.env.TEST_BYBIT_URL || "").trim() || (ENV === "testnet" ? "https://api-testnet.bybit.com" : "https://api.bybit.com");
const AUTOSTART = /^(1|true|yes|on|ai)$/i.test((process.env.AUTOSTART || "").trim());
const UI = fs.readFileSync(path.join(__dirname, "ui.html"), "utf8");

const ts = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const log = (...a) => console.log(ts(), ...a);

// ---------------- storage: one JSON file on the Railway Volume (/data) ----------------
fs.mkdirSync(DATA_DIR, { recursive: true });
let DB = {};
try { DB = JSON.parse(fs.readFileSync(STORE, "utf8")); } catch (e) { DB = {}; }
let dirty = false;
const lsGet = (k, d) => (Object.prototype.hasOwnProperty.call(DB, k) ? JSON.parse(JSON.stringify(DB[k])) : d);
const lsSet = (k, v) => { DB[k] = v; dirty = true; };
function dump() {
  if (!dirty) return;
  dirty = false;
  try { fs.writeFileSync(STORE + ".tmp", JSON.stringify(DB)); fs.renameSync(STORE + ".tmp", STORE); } catch (e) { log("storage write failed", e.message); }
}
setInterval(dump, 2000);
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => { dirty = true; dump(); process.exit(0); });

// ---------------- event log shown on the page ----------------
const EVENTS = lsGet("events", []);
function addLog(text, cls) {
  EVENTS.unshift([Date.now(), String(text), cls || ""]);
  EVENTS.length = Math.min(EVENTS.length, 300);
  lsSet("events", EVENTS);
  log("·", text);
}

// ---------------- Bybit REST ----------------
let OFFSET = 0;
class BybitErr extends Error { constructor(m, code) { super(m); this.code = code; } }
async function bx(p, { method = "GET", query = null, body = null, auth = false } = {}, retry = true) {
  const qs = query ? new URLSearchParams(Object.entries(query).filter(([, v]) => v != null).map(([k, v]) => [k, String(v)])).toString() : "";
  const bodyStr = method === "POST" ? JSON.stringify(body || {}) : "";
  const headers = {};
  if (method === "POST") headers["Content-Type"] = "application/json";
  if (auth) {
    if (!HAS_KEYS) throw new BybitErr("немає BYBIT_API_KEY / BYBIT_API_SECRET у Railway → Variables");
    const t = String(Date.now() + OFFSET), recv = "10000";
    const sign = crypto.createHmac("sha256", SECRET).update(t + KEY + recv + (method === "GET" ? qs : bodyStr)).digest("hex");
    Object.assign(headers, { "X-BAPI-API-KEY": KEY, "X-BAPI-TIMESTAMP": t, "X-BAPI-RECV-WINDOW": recv, "X-BAPI-SIGN": sign });
  }
  let res;
  try { res = await fetch(REST + p + (qs ? "?" + qs : ""), { method, headers, body: method === "POST" ? bodyStr : undefined, signal: AbortSignal.timeout(15000) }); }
  catch (e) { throw new BybitErr("немає зв'язку з Bybit: " + (e.cause && e.cause.code || e.message)); }
  const j = await res.json().catch(() => ({}));
  if ((res.status === 403 || res.status === 451) && j.retCode === undefined)
    throw new BybitErr(`Bybit блокує регіон сервера (HTTP ${res.status}). Railway → Settings → Region: оберіть Europe (EU West / Amsterdam) або Asia, не US.`, 403);
  if (j.retCode === 10002 && retry) { await syncTime(); return bx(p, { method, query, body, auth }, false); }
  if (j.retCode !== 0) throw new BybitErr((j.retMsg || "HTTP " + res.status) + (j.retCode ? ` (${j.retCode})` : ""), j.retCode);
  return j.result || {};
}
async function syncTime() { try { const r = await bx("/v5/market/time"); OFFSET = Math.round(Number(r.timeNano) / 1e6) - Date.now(); } catch (e) { /* keep the old offset */ } }
const SPEC = {};
async function spec(sym) {
  if (SPEC[sym]) return SPEC[sym];
  const r = await bx("/v5/market/instruments-info", { query: { category: "linear", symbol: sym } });
  const row = (r.list || [])[0];
  if (!row) throw new BybitErr("немає монети " + sym);
  return (SPEC[sym] = { tick: row.priceFilter.tickSize, step: row.lotSizeFilter.qtyStep, minQty: Number(row.lotSizeFilter.minOrderQty || 0),
    minNotional: Number(row.lotSizeFilter.minNotionalValue || 5), maxLev: Number((row.leverageFilter || {}).maxLeverage || 10) });
}
const dec = (s) => { s = String(s); return s.includes(".") ? s.split(".")[1].length : 0; };
function align(v, step, up) {
  const d = dec(step), k = 10 ** d, st = Math.max(1, Math.round(Number(step) * k));
  const n = up ? Math.ceil(Math.ceil(v * k - 1e-6) / st) * st : Math.floor(Math.floor(v * k + 1e-6) / st) * st;
  return (n / k).toFixed(d);
}

// ---------------- account ----------------
const ACC = { equity: null, available: null, err: HAS_KEYS ? "" : "немає BYBIT_API_KEY / BYBIT_API_SECRET у Railway → Variables", at: 0 };
async function refreshAccount() {
  if (!HAS_KEYS) return;
  try {
    const w = await bx("/v5/account/wallet-balance", { query: { accountType: "UNIFIED" }, auth: true });
    const a = (w.list || [])[0] || {};
    ACC.equity = Number(a.totalEquity || 0); ACC.available = Number(a.totalAvailableBalance || 0); ACC.err = ""; ACC.at = Date.now();
    const eq = lsGet("eq", []);
    if (ACC.equity > 0 && (!eq.length || Date.now() - eq[eq.length - 1][0] >= 600000)) { eq.push([Date.now(), +ACC.equity.toFixed(4)]); lsSet("eq", eq.slice(-3000)); }
  } catch (e) { ACC.err = e.message; }
}


// ---------------- the engine ----------------
// AI (fal.ai) for the lightning learning: FAL_KEY in Railway -> the AI chooses among the tested candidates
const FAL_KEY = (process.env.FAL_KEY || process.env.FAL_API_KEY || "").trim();
const FAL_MODEL = (process.env.FAL_MODEL || "google/gemini-2.5-flash").trim();
const FAL_URL = (process.env.TEST_FAL_URL || process.env.FAL_URL || "https://fal.run/fal-ai/any-llm").trim();
async function fal(prompt) {
  const r = await fetch(FAL_URL, { method: "POST", headers: { Authorization: "Key " + FAL_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ model: FAL_MODEL, system_prompt: "You optimise a crypto trading bot. Answer only with one JSON object.", prompt, temperature: 0.2, max_tokens: 400 }), signal: AbortSignal.timeout(60000) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error(`fal ${r.status}: ${j.error || "ошибка"}`);
  const m = String(j.output || "").match(/\{[\s\S]*\}/); if (!m) throw new Error("AI не вернул JSON");
  return JSON.parse(m[0]);
}
let SG = null;
const wsUrl = () => (process.env.TEST_WS_URL || "").trim() || (ENV === "testnet" ? "wss://stream-testnet.bybit.com/v5/public/" : "wss://stream.bybit.com/v5/public/") + (SG && SG.params.category === "inverse" ? "inverse" : "linear");
SG = require("./engine").create({ bx, lsGet, lsSet, keys: () => HAS_KEYS, log: (m) => log("sg", m), fal: FAL_KEY ? fal : null, wsUrl });

function state() {
  const ws = wsUrl();
  return { env: ENV, ws, keys: HAS_KEYS, password: !!PASSWORD, acc: { equity: ACC.equity, available: ACC.available, err: ACC.err }, sg: SG.view() };
}
async function act(b) {
  const what = String(b.what || "");
  if (b.o && typeof b.o === "object") SG.setParams(b.o);   // the page always sends its current settings with a command
  if (what === "save") return "Сохранено";
  if (what === "ai") return await SG.aiLearn("по кнопке");
  if (what === "start") return await SG.start();
  if (what === "stop") return await SG.stop(true);
  if (what === "once") { await SG.runCycle(); return "1 цикл выполнен"; }
  if (what === "shukin") return SG.toggleShukin();
  if (what === "scan") return await SG.scanPump();
  if (what === "ultra") return await SG.scanUltraVol1m();
  if (what === "clearTargets") return SG.clearTargets();
  if (what === "sync") { if (!HAS_KEYS) return "Нет API ключей в Railway → Variables"; return await SG.sync(); }
  if (what === "close") { if (!HAS_KEYS) return "Нет API ключей"; return await SG.closePos(b.symbol); }
  if (what === "position") { if (!HAS_KEYS) return "Нет API ключей"; return JSON.stringify(await SG.position(b.symbol)); }
  if (what === "test") { await bx("/v5/market/time"); if (!HAS_KEYS) return "Public OK (нет ключей)"; await bx("/v5/account/wallet-balance", { query: { accountType: "UNIFIED" }, auth: true }); return "Public + Private OK"; }
  return "неизвестная команда";
}
// chart data for the page (via the server: no CORS / region problems in the browser)
async function chart(q) {
  const sym = String(q.get("symbol") || SG.params.symbol).toUpperCase(), iv = String(q.get("interval") || SG.params.interval), cat = SG.params.category;
  const candles = await SG.getKlines(sym, cat, iv, 200);
  let oi = []; try { oi = await SG.getOpenInterest(sym, cat, iv, 200); } catch (e) { /* no OI */ }
  return { symbol: sym, candles, oi, sig: SG.analyzeShukinContext(candles, oi) };
}

// ---------------- web ----------------
function authorised(req) {
  if (!PASSWORD) return true;
  const h = req.headers.authorization || "";
  if (!h.startsWith("Basic ")) return false;
  const pass = Buffer.from(h.slice(6), "base64").toString().split(":").slice(1).join(":");
  const a = Buffer.from(pass), b2 = Buffer.from(PASSWORD);
  return a.length === b2.length && crypto.timingSafeEqual(a, b2);
}
function readBody(req) {
  return new Promise((resolve) => { let s = ""; req.on("data", c => { s += c; if (s.length > 1e5) req.destroy(); }); req.on("end", () => { try { resolve(JSON.parse(s || "{}")); } catch (e) { resolve({}); } }); });
}
http.createServer(async (req, res) => {
  const json = (code, o) => { res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }); res.end(JSON.stringify(o)); };
  try {
    const url = new URL(req.url, "http://x");
    if (url.pathname === "/healthz") return json(200, { ok: true });
    if (!authorised(req)) { res.writeHead(401, { "WWW-Authenticate": 'Basic realm="Sell Grid"' }); return res.end("Потрібен пароль (UI_PASSWORD)"); }
    if (url.pathname === "/api/state") return json(200, state());
    if (url.pathname === "/api/chart") { try { return json(200, await chart(url.searchParams)); } catch (e) { return json(200, { error: e.message }); } }
    if (req.method === "POST" && url.pathname.startsWith("/api/")) {
      if (!PASSWORD) return json(200, { error: "Задайте UI_PASSWORD у Railway → Variables, щоб керувати ботом зі сторінки" });
      const b = await readBody(req);
      try {
        if (url.pathname === "/api/act") { const text = await act(b); dump(); return json(200, { text }); }
      } catch (e) { return json(200, { error: String(e && e.message || e) }); }
    }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    return res.end(UI);
  } catch (e) { return json(500, { error: String(e && e.message || e) }); }
}).listen(PORT, "0.0.0.0", () => log(`SELL GRID :${PORT} · ${ENV} · Bybit keys ${HAS_KEYS ? "yes" : "NO"} · ${PASSWORD ? "password on" : "NO UI_PASSWORD (buttons locked)"} · storage ${STORE}`));

// 24/7: a stray error is logged, the server keeps running (and Railway restarts it if it ever stops)
process.on("unhandledRejection", (e) => log("unhandledRejection", e && e.message || e));
process.on("uncaughtException", (e) => log("uncaughtException", e && e.stack || e));

// ---------------- one-time: the previous bot of this project («Пробій після стиснення») is retired ----------------
// its pending entries are cancelled and its positions closed, otherwise the sell grid would take them over
async function retireSqueeze() {
  if (!HAS_KEYS || lsGet("retired.sqz", false)) return;
  const pos = lsGet("sq.pos", {}) || {};
  for (const [sym, p] of Object.entries(pos)) {
    try {
      const open = ((await bx("/v5/order/realtime", { query: { category: "linear", symbol: sym }, auth: true })).list || []).filter(o => /^sq/.test(o.orderLinkId || ""));
      for (const o of open) await bx("/v5/order/cancel", { method: "POST", auth: true, body: { category: "linear", symbol: sym, orderId: o.orderId } }).catch(() => {});
      if (p.state === "open") {
        const r = await bx("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true });
        for (const x of r.list || []) if (Number(x.size) > 0 && (x.side === "Buy" ? 1 : -1) === p.d)
          await bx("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: sym, side: x.side === "Buy" ? "Sell" : "Buy", orderType: "Market", qty: String(x.size), reduceOnly: true, positionIdx: Number(x.positionIdx || 0), timeInForce: "IOC" } });
      }
      log(`retired squeeze bot on ${sym}`);
    } catch (e) { log(`retire ${sym}: ${e.message}`); }
  }
  lsSet("sq.pos", {}); const sp = lsGet("sq.p", null); if (sp) lsSet("sq.p", Object.assign(sp, { on: false }));
  lsSet("retired.sqz", true);
}

// ---------------- start ----------------
(async () => {
  await syncTime();
  await refreshAccount();
  await retireSqueeze();
  if (ACC.err) log("Bybit: " + ACC.err);
  setInterval(refreshAccount, 15000);
  setInterval(syncTime, 30 * 60000);
  if (AUTOSTART && HAS_KEYS && !lsGet("autostarted", false)) { lsSet("autostarted", true); SG.start(); }
})();
