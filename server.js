// SELL-сетка на самых волатильных за 1 мин монетах · 24/7 на Railway: торгует сервер, страница только показывает и управляет.
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


// ---------------- fal.ai: the AI that analyses the days (FAL_KEY in Railway) ----------------
const FAL_KEY = (process.env.FAL_KEY || process.env.FAL_API_KEY || "").trim();
const FAL_MODEL = (process.env.FAL_MODEL || "google/gemini-2.5-flash").trim();
const FAL_URL = (process.env.TEST_FAL_URL || process.env.FAL_URL || "https://fal.run/fal-ai/any-llm").trim();
async function fal(prompt) {
  const r = await fetch(FAL_URL, { method: "POST", headers: { Authorization: "Key " + FAL_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ model: FAL_MODEL, system_prompt: "You analyse a crypto trading bot's day and tune it. Answer only with one JSON object.", prompt, temperature: 0.2, max_tokens: 1500 }), signal: AbortSignal.timeout(120000) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error(`fal ${r.status}: ${j.error || j.detail || "ошибка"}`);
  const m = String(j.output || "").match(/\{[\s\S]*\}/); if (!m) throw new Error("AI не вернул JSON");
  return JSON.parse(m[0]);
}

// ---------------- the bot ----------------
const BOT = require("./bot").create({ bx, lsGet, lsSet, keys: () => HAS_KEYS, log: (m) => log("bot", m), dataDir: DATA_DIR, fal: FAL_KEY ? fal : null });

function state() {
  return { env: ENV, keys: HAS_KEYS, password: !!PASSWORD, acc: { equity: ACC.equity, available: ACC.available, err: ACC.err }, bot: BOT.view() };
}
async function act(b) {
  const what = String(b.what || "");
  if (b.cfg && typeof b.cfg === "object") BOT.setCfg(b.cfg);   // the page sends its settings with every command
  if (what === "save") return "Сохранено";
  if (what === "start") return await BOT.start();
  if (what === "stop") return await BOT.stop();
  if (what === "reset") return BOT.resetStats();
  if (what === "btcOnly") { const on = !BOT.view().cfg.btcOnly; BOT.setCfg({ btcOnly: on }); return on ? "₿ SELL только когда BTC в локальном тренде вниз" : "₿ Фильтр BTC выключен"; }
  if (what === "analyze") return await BOT.analyze(String(b.day || ""), "по кнопке");
  if (what === "applyRec") return BOT.applyRec(String(b.day || ""));
  if (what === "test") { await bx("/v5/market/time"); if (!HAS_KEYS) return "Public OK (нет ключей — только симуляция)"; await refreshAccount(); if (ACC.err) throw new Error(ACC.err); return `Связь OK · баланс ${(ACC.equity || 0).toFixed(2)} USDT, свободно ${(ACC.available || 0).toFixed(2)}`; }
  return "неизвестная команда";
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
    if (!authorised(req)) { res.writeHead(401, { "WWW-Authenticate": 'Basic realm="SELL Grid"' }); return res.end("Потрібен пароль (UI_PASSWORD)"); }
    if (url.pathname === "/api/state") return json(200, state());
    if (url.pathname === "/api/days") return json(200, { days: BOT.days() });
    if (url.pathname === "/api/journal") {   // the detailed journal of a day: one JSON per line
      const f = BOT.journalPath(String(url.searchParams.get("day") || ""));
      if (!f || !fs.existsSync(f)) return json(404, { error: "нет журнала" });
      res.writeHead(200, { "Content-Type": "application/x-ndjson; charset=utf-8", "Content-Disposition": `attachment; filename="journal-${path.basename(f)}"` });
      return fs.createReadStream(f).pipe(res);
    }
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

// ---------------- start ----------------
(async () => {
  await syncTime();
  await refreshAccount();
  if (ACC.err) log("Bybit: " + ACC.err);
  setInterval(refreshAccount, 15000);
  setInterval(syncTime, 30 * 60000);
})();
