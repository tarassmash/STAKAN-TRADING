// SQUEEZE BOT 24/7: «Пробій після стиснення (Боллінджер)» на ф'ючерсах Bybit.
// Сторінка лише показує стан і надсилає команди; торгує сервер, навіть коли браузер закритий.
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
const TEST_WS = (process.env.TEST_WS_URL || "").trim();
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

// ---------------- why the minus / strategy lab ----------------
let SQ = null;
const DG = require("./diag").create({ bx, BG: { tradesSince: (t) => (SQ ? SQ.tradesSince(t) : []) }, keys: () => HAS_KEYS });
const LAB = require("./lab").create({ bx, lsGet, lsSet, engineLog: (t, cls) => addLog(t, cls), coins: 15 });

// ---------------- the only strategy: breakout after a Bollinger squeeze ----------------
SQ = require("./sqz").create({ bx, spec, align, engineLog: (t, cls) => addLog(t, cls), lsGet, lsSet, keys: () => HAS_KEYS,
  equity: () => ACC.equity || 0, lab: () => LAB.view(), flush: () => { dirty = true; dump(); } });

// ---------------- one-time cleanup: the old bots (grid / breakout grid) are gone -> their orders and positions are closed ----------------
async function retireOldBots() {
  if (!HAS_KEYS || lsGet("retired.v1", false)) return;
  const syms = new Set(), b = lsGet("bg.b", null), g = lsGet("grid.g", null), bk = lsGet("grid.bk", null);
  if (b && b.sym && (b.units || []).length) syms.add(b.sym);
  if (g && g.sym) syms.add(g.sym);
  if (bk && bk.sym) syms.add(bk.sym);
  for (const sym of syms) {
    try {
      await bx("/v5/order/cancel-all", { method: "POST", auth: true, body: { category: "linear", symbol: sym } });
      const r = await bx("/v5/position/list", { query: { category: "linear", symbol: sym }, auth: true });
      for (const x of r.list || []) if (Number(x.size) > 0)
        await bx("/v5/order/create", { method: "POST", auth: true, body: { category: "linear", symbol: sym, side: x.side === "Buy" ? "Sell" : "Buy", orderType: "Market", qty: String(x.size), reduceOnly: true, positionIdx: Number(x.positionIdx || 0), timeInForce: "IOC" } });
      addLog(`🧹 Старий бот прибраний: ${sym} — ордери скасовано, позицію закрито ринком`, "mute");
    } catch (e) { addLog(`🧹 ${sym}: не вдалося прибрати старого бота — ${e.message} (закрийте позицію вручну на Bybit)`, "down"); }
  }
  for (const k of ["bg.b", "grid.g", "grid.bk"]) lsSet(k, null);
  for (const k of ["bg.p", "grid.p", "brain.p"]) { const v = lsGet(k, null); if (v) lsSet(k, Object.assign(v, { on: false })); }
  lsSet("retired.v1", true);
}

function state() {
  return { env: ENV, keys: HAS_KEYS, password: !!PASSWORD, acc: { equity: ACC.equity, available: ACC.available, err: ACC.err }, sq: SQ.view(), diag: DG.view(), lab: LAB.view(), eq: lsGet("eq", []).slice(-400), events: EVENTS.slice(0, 100) };
}
async function act(b) {
  const what = String(b.what || "");
  if (what === "on") {
    if (!HAS_KEYS) return "Додайте BYBIT_API_KEY і BYBIT_API_SECRET у Railway → Variables";
    await syncTime(); await refreshAccount(); if (ACC.err) return "Bybit: " + ACC.err;
    if (!(ACC.equity > 0)) return "На рахунку 0 USDT — поповніть ф'ючерсний (Unified) рахунок";
    SQ.setOn(true); return "Запущено: наступна перевірка монет — на закритті 15-хв свічки";
  }
  if (what === "off") { SQ.setOn(false); return "Зупинено: нових входів немає, відкриті угоди лишаються зі стопом на біржі"; }
  if (what === "close") return await SQ.closeAll();
  if (what === "lab") { LAB.run(); return "Перевірка стратегії на історії запущена (кілька хвилин)"; }
  if (what === "diag") { await DG.run(); return DG.view().err ? "Bybit: " + DG.view().err : "Розбір оновлено"; }
  if (what === "settings" && b.o && typeof b.o === "object") { const ch = SQ.setParams(b.o); return ch.length ? "Збережено: " + ch.join(", ") : "Без змін"; }
  return "невідома дія";
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
    if (!authorised(req)) { res.writeHead(401, { "WWW-Authenticate": 'Basic realm="Stakan scalper"' }); return res.end("Потрібен пароль (UI_PASSWORD)"); }
    if (url.pathname === "/api/state") return json(200, state());
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
}).listen(PORT, "0.0.0.0", () => log(`SQUEEZE BOT :${PORT} · ${ENV} · Bybit keys ${HAS_KEYS ? "yes" : "NO"} · ${PASSWORD ? "password on" : "NO UI_PASSWORD (buttons locked)"} · storage ${STORE}`));

// ---------------- start ----------------
(async () => {
  await syncTime();
  await refreshAccount();
  if (ACC.err) addLog("Bybit: " + ACC.err, "down");
  setInterval(refreshAccount, 15000);
  setInterval(syncTime, 30 * 60000);
  await retireOldBots();
  if (AUTOSTART && HAS_KEYS && !lsGet("autostarted", false)) { lsSet("autostarted", true); addLog("AUTOSTART: вмикаю бота", "up"); SQ.setOn(true); }
  else if (SQ.params.on) addLog("Сервер перезапущено — бот продовжує роботу", "up");
  else addLog("Сервер запущено. Натисніть «▶ ЗАПУСТИТИ».", "mute");
})();
