// Стакан-скальпер 24/7: торгівля по стакану Bybit на сервері (Railway) + AI-автопілот fal.ai з повним керуванням.
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
const FAL_KEY = (process.env.FAL_KEY || process.env.FAL_API_KEY || "").trim();
const FAL_MODEL = (process.env.FAL_MODEL || "google/gemini-2.5-flash").trim();
const FAL_URL = (process.env.TEST_FAL_URL || process.env.FAL_URL || "https://fal.run/fal-ai/any-llm").trim();
const REST = (process.env.TEST_BYBIT_URL || "").trim() || (ENV === "testnet" ? "https://api-testnet.bybit.com" : "https://api.bybit.com");
const TEST_WS = (process.env.TEST_WS_URL || "").trim();
const AUTOSTART = /^(1|true|yes|on|ai)$/i.test((process.env.AUTOSTART || "").trim());
const ENV_CAP_MARGIN = Number(process.env.AI_MAX_MARGIN) || 0;
const ENV_CAP_LEV = Number(process.env.AI_MAX_LEVERAGE) || 0;
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
setInterval(dump, 5000);
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
const ACC = { equity: null, available: null, busy: [], err: HAS_KEYS ? "" : "немає BYBIT_API_KEY / BYBIT_API_SECRET у Railway → Variables", at: 0 };
async function refreshAccount() {
  if (!HAS_KEYS) return;
  try {
    const w = await bx("/v5/account/wallet-balance", { query: { accountType: "UNIFIED" }, auth: true });
    const a = (w.list || [])[0] || {};
    ACC.equity = Number(a.totalEquity || 0); ACC.available = Number(a.totalAvailableBalance || 0); ACC.err = ""; ACC.at = Date.now();
    const eq = lsGet("eq", []);
    if (ACC.equity > 0 && (!eq.length || Date.now() - eq[eq.length - 1][0] >= 600000)) { eq.push([Date.now(), +ACC.equity.toFixed(4)]); lsSet("eq", eq.slice(-3000)); }
    const p = await bx("/v5/position/list", { query: { category: "linear", settleCoin: "USDT", limit: "200" }, auth: true });
    const own = new Set((SS.view().open || []).map(o => o.sym));
    ACC.busy = (p.list || []).filter(x => Number(x.size) > 0 && !own.has(x.symbol)).map(x => x.symbol);
  } catch (e) { ACC.err = e.message; }
}

// ---------------- the order-book scalper ----------------
const SS = require("./superscalp").create({
  ev: () => { throw new Error("no engine"); },
  log: (...a) => log("scalper", ...a),
  engineLog: (t, cls) => addLog(t, cls),
  lsGet, lsSet, env: ENV, testWs: TEST_WS,
  host: { bx, spec, align, reserve: () => {}, freeCoins: async () => 0, equity: () => ACC.equity || 0, busy: () => ACC.busy },
});
if (!lsGet("styleChosen", false)) { SS.setStyle("pressure"); lsSet("styleChosen", true); }

// ---------------- AI autopilot (fal.ai) with full control ----------------
const AI = Object.assign({ on: false, last: 0, every: Math.max(2, Number(process.env.AI_EVERY_MIN || 10)), maxMargin: null, maxLev: null, history: [], error: "", notes: [] }, lsGet("ai", {}));
AI.busy = false;
const saveAI = () => lsSet("ai", { on: AI.on, last: AI.last, every: AI.every, maxMargin: AI.maxMargin, maxLev: AI.maxLev, history: AI.history.slice(-400), error: AI.error, notes: AI.notes.slice(-20) });
const capMargin = () => ENV_CAP_MARGIN || AI.maxMargin || SS.params.margin;
const capLev = () => ENV_CAP_LEV || AI.maxLev || SS.params.leverage;
function aiSet(on) {
  if (on && !FAL_KEY) return "Для AI додайте FAL_KEY у Railway → Variables";
  if (on && !AI.maxMargin) AI.maxMargin = SS.params.margin;       // the AI never risks more than the owner set, unless the owner raises the caps
  if (on && !AI.maxLev) AI.maxLev = SS.params.leverage;
  if (on === AI.on) return on ? "AI вже керує" : "AI вже вимкнено";
  AI.on = on; AI.error = ""; saveAI();
  addLog(on ? `🧠 AI автопілот увімкнено: керує всім, рішення кожні ${AI.every} хв (маржа до ${capMargin()} USDT, плече до x${capLev()})` : "🧠 AI автопілот вимкнено", on ? "up" : "mute");
  if (on) { AI.last = Date.now(); setTimeout(() => aiTick(true), 300); }
  return on ? "AI увімкнено" : "AI вимкнено";
}
const SYSTEM = `You have FULL control of an order-book scalping bot on Bybit USDT perpetuals. The owner gave you the steering wheel 100%: there is no human in the loop.
The owner only presses Start/Stop. Your only job is to grow the account. Every period you get the bot state and decide how it trades next.
Your results are measured: "yourTrackRecord" lists your previous decisions and the profit/loss made in the period after each of them.
Learn from it: repeat what worked, stop what lost money, and say what you learned in "lesson". If your periods keep losing, the owner
will switch you off, so do everything possible to make each period profitable.
The bot ("superscalp") trades only the order book and the tape. It enters with LIMIT (PostOnly) orders at the best price, takes a short LIMIT
take and keeps a stop on the exchange. Data you get: params, stats, results grouped by symbol (bySymbol), signal (bySignal), take size (byTp),
exit type (byExit), normal vs inverted (byMode), the exit optimizer ("optimizer": current vs best take/stop on recorded price paths:
avg % per trade, win rate, profit factor), live signals per coin (imb = book imbalance 0..1, delta = tape buy-sell share, burst = 5 s volume
vs normal, iceberg, absorption, spoof, press = top-level pressure and best-price queue, long/short scores, "why" = why there is no entry now,
peakScore10min) and "volatilityTop" (rng1h = range of the last hour, %), plus the last trades.
"trail" (default true): trailing take - reaching tpPct does not close the trade; the stop moves into profit and follows the best point,
the trade ends on a pull-back of trailPct (0 = auto, half of the take) from the best point, so the whole impulse is taken; tpPct is then
the level where trailing starts. Trades closed this way have how = "трейлінг" and "peak" = the best point in %.
Styles: "flow" (book + tape + icebergs/absorption/spoof + impulse, the take follows the impulse) or "pressure" (book pressure only: heavier side
for the last 2 s + longer best-price queue, tiny limit takes ~0.08%, exit as soon as the pressure goes away; high win rate, small wins).
"invert": "off"|"on"|"auto" - mirror mode trades the opposite side with take and stop swapped; "auto" switches by itself when the current mode
loses over invertMin trades. "learn" (filter that skips setups whose most similar past trades lost) and "optExits" (auto take/stop) - keep on.
With autoCoins the bot itself picks the most volatile liquid coins; send "symbols" only to override (max 12 liquid coins).
Rules: tpPct must beat maker fees (0.04% round trip); never remove stops; "on": false pauses new entries (open trades finish) - use it only when
the market is clearly bad for scalping; with few trades change one or two settings at a time; prefer values that made money with n >= 10;
if the bot makes no trades for a long time, lower scoreIn towards the typical peakScore10min or pick more active coins.
Answer with ONE JSON object only, no text around it:
{"regime":"calm|range|trend|volatile|chaotic","superscalp":{"on":true|false,"symbols":[optional],"params":{optional: style,tpPct,slPct,scoreIn,bandPct,
burstX,wallX,spoofSec,icebergX,entryWaitSec,holdMaxSec,maxPositions,cooldownSec,margin,leverage,ride,useIceberg,useAbsorption,useSpoof,needAgree,
cancelOnFade,flipExit,timeStopSec,adaptive,autoPrune,autoCoins,coinCount,minTurnoverM,pickEveryMin,invert,invertMin,learn,optExits,pressLevels,trail,trailPct}},
"reason":"short explanation in Ukrainian (1-3 sentences)","lesson":"one sentence in Ukrainian"}`;
const CMD_SYSTEM = `You are the voice assistant of the owner's Bybit order-book scalping app. The owner speaks (or types) an instruction in Ukrainian,
Russian or English. The text may come from speech recognition with mistakes, or be typed on a Ukrainian/Russian keyboard while meaning Latin
transliteration (e.g. "ыефке" = "start") - decode it. Turn it into actions the app can do now. Available actions (use only what was asked):
 "scalper": "start" | "stop"     (stop also switches the AI off)
 "ai": true | false              (AI autopilot on/off)
 "superscalp": {"on":true|false, "symbols":[...], "params":{style:"flow"|"pressure",tpPct,slPct,scoreIn,maxPositions,margin,leverage,autoCoins,coinCount,minTurnoverM,invert,learn,optExits,trail,trailPct,...}}
 "limits": {"maxLeverage": n, "maxMargin": n}   (caps for the AI autopilot - only when the owner asks)
 "aiEveryMin": n                  (how often the AI decides)
 "optimize": true                 (re-tune take/stop on recorded trades now)
 "remember": "a standing instruction for the AI autopilot, in Ukrainian" (rules about the future, e.g. "be more careful", "trade only BTC")
 "forget": true                   (clear remembered instructions)
Answer with ONE JSON object: {"actions":{...}, "summary":"what will be done, in Ukrainian, short list", "question":"ask only if unclear, else empty"}`;
async function falJSON(system, prompt, maxTokens) {
  if (!FAL_KEY) throw new Error("немає FAL_KEY у Railway → Variables");
  let r;
  try {
    r = await fetch(FAL_URL, { method: "POST", headers: { Authorization: "Key " + FAL_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ model: FAL_MODEL, system_prompt: system, prompt, temperature: 0.2, max_tokens: maxTokens || 900 }), signal: AbortSignal.timeout(90000) });
  } catch (e) { throw new Error("немає зв'язку з fal.ai: " + (e.cause && e.cause.code || e.message)); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error(`fal ${r.status}: ${j.error || (j.detail && JSON.stringify(j.detail).slice(0, 200)) || "помилка"}`);
  const m = String(j.output || "").match(/\{[\s\S]*\}/);
  if (!m) throw new Error("AI не повернув JSON");
  return JSON.parse(m[0]);
}
const TRADES = () => SS._test.ST.trades;
function closePeriod() {
  const h = AI.history[AI.history.length - 1];
  if (!h || h.closed) return;
  const t = TRADES().filter(x => x.closedAt >= h.at);
  h.periodPnl = +t.reduce((a, x) => a + Number(x.pnl || 0), 0).toFixed(4); h.periodTrades = t.length; h.periodWins = t.filter(x => x.pnl > 0).length;
  if (h.equity && ACC.equity) h.periodEquity = +(ACC.equity - h.equity).toFixed(4);
  h.closed = true;
}
function slimView() {
  const v = SS.aiView();
  return Object.assign({}, v, { last: (v.last || []).map(t => { const o = Object.assign({}, t); delete o.path; delete o.feat; return o; }) });
}
function applyDecision(d, caps) {
  const ch = [], s = d && d.superscalp;
  if (!s || typeof s !== "object") return ch;
  const p = Object.assign({}, s.params || {}); delete p.on;
  if (Array.isArray(s.symbols) && s.symbols.length) p.symbols = s.symbols;
  ch.push(...SS.setParams(p, caps));
  if (typeof s.on === "boolean" && s.on !== !!SS.params.on) { SS.setOn(s.on); ch.push(s.on ? "входи увімкнено" : "нові входи на паузі"); }
  return ch;
}
async function aiTick(force) {
  if (!AI.on || AI.busy) return;
  if (!force && Date.now() - AI.last < AI.every * 60000) return;
  AI.busy = true; AI.last = Date.now();
  try {
    closePeriod();
    const ctx = { time: new Date().toISOString(), account: { equity: ACC.equity, available: ACC.available },
      limits: { maxMarginPerTrade: capMargin(), maxLeverage: capLev() }, superscalp: slimView(),
      yourTrackRecord: AI.history.slice(-12).map(h => ({ at: new Date(h.at).toISOString(), regime: h.regime, changes: h.changes, lesson: h.lesson || "",
        periodPnlUsdt: h.periodPnl ?? null, periodTrades: h.periodTrades ?? null, periodWins: h.periodWins ?? null })) };
    const sys = SYSTEM + (AI.notes.length ? `\nThe OWNER's standing instructions (follow them; they have priority over your own preferences, but never remove stops):\n- ${AI.notes.map(n => n.text).join("\n- ")}\n` : "");
    const d = await falJSON(sys, "State:\n" + JSON.stringify(ctx), 900);
    const changes = applyDecision(d, { margin: capMargin(), leverage: capLev() });
    const item = { at: Date.now(), regime: String(d.regime || ""), reason: String(d.reason || "").slice(0, 400), lesson: String(d.lesson || "").slice(0, 300), changes, equity: ACC.equity };
    AI.history.push(item); AI.error = "";
    addLog(`🧠 AI: ${item.regime} — ${item.reason}${changes.length ? " · зміни: " + changes.join("; ") : " · без змін"}`, "up");
  } catch (e) {
    AI.error = String(e && e.message || e);
    addLog(`🧠 AI: помилка — ${AI.error}. Бот торгує з поточними налаштуваннями.`, "down");
  } finally { AI.busy = false; saveAI(); }
}
setInterval(() => aiTick(false), 30000);

// ---------------- owner's voice / text commands ----------------
const PLANS = new Map();
async function command(text) {
  const ctx = { instruction: String(text).slice(0, 1500), now: { scalperOn: !!SS.params.on, aiOn: AI.on, aiEveryMin: AI.every, params: SS.params,
    limits: { maxLeverage: capLev(), maxMargin: capMargin() }, remembered: AI.notes.map(n => n.text), open: (SS.view().open || []).map(o => o.sym + " " + o.side) } };
  const d = await falJSON(CMD_SYSTEM, JSON.stringify(ctx), 700);
  const id = crypto.randomBytes(6).toString("hex");
  PLANS.set(id, { at: Date.now(), text, d });
  for (const [k, v] of PLANS) if (Date.now() - v.at > 600000) PLANS.delete(k);
  return { id, summary: d.summary || "", question: d.question || "" };
}
function applyCommand(id) {
  const plan = PLANS.get(id);
  if (!plan) throw new Error("План застарів — скажіть ще раз");
  PLANS.delete(id);
  const a = (plan.d && plan.d.actions) || {}, done = [];
  if (a.limits && typeof a.limits === "object") {
    if (Number(a.limits.maxLeverage) > 0) { AI.maxLev = Math.min(100, Math.round(Number(a.limits.maxLeverage))); done.push(`межа плеча для AI x${AI.maxLev}`); }
    if (Number(a.limits.maxMargin) > 0) { AI.maxMargin = Number(a.limits.maxMargin); done.push(`межа маржі для AI ${AI.maxMargin} USDT`); }
  }
  if (Number(a.aiEveryMin) > 0) { AI.every = Math.max(2, Math.min(120, Math.round(Number(a.aiEveryMin)))); done.push(`AI вирішує кожні ${AI.every} хв`); }
  if (a.superscalp && typeof a.superscalp === "object") {
    const p = a.superscalp.params || {};
    // the owner's own command may raise margin/leverage; the AI caps follow it
    if (Number(p.margin) > 0) AI.maxMargin = Math.max(AI.maxMargin || 0, Number(p.margin));
    if (Number(p.leverage) > 0) AI.maxLev = Math.max(AI.maxLev || 0, Math.round(Number(p.leverage)));
    done.push(...applyDecision({ superscalp: a.superscalp }, {}));
  }
  if (a.scalper === "start" && !SS.params.on) { if (HAS_KEYS) { SS.setOn(true); done.push("скальпер запущено"); } else done.push("немає ключів Bybit"); }
  if (a.scalper === "stop") { SS.setOn(false); aiSet(false); done.push("скальпер зупинено, AI вимкнено"); }
  if (typeof a.ai === "boolean" && a.ai !== AI.on) done.push(aiSet(a.ai));
  if (a.optimize) { const r = SS.optimizeNow(); done.push(r && r.applied ? `оптимізатор: тейк ${r.best.tp}% / стоп ${r.best.sl}%` : "оптимізатор: змін немає"); }
  if (a.forget) { AI.notes = []; done.push("вказівки AI очищено"); }
  if (a.remember) { AI.notes.push({ at: Date.now(), text: String(a.remember).slice(0, 300) }); done.push(`AI запам'ятав: «${String(a.remember).slice(0, 120)}»`); }
  saveAI();
  const text = done.length ? done.join("; ") : "Нічого не змінено";
  addLog(`🎙 Команда: «${String(plan.text).slice(0, 120)}» → ${text}`, "up");
  return text;
}

// ---------------- progress chart data ----------------
function progress() {
  const eq = lsGet("eq", []), step = Math.max(1, Math.ceil(eq.length / 400));
  const pts = eq.filter((_, i) => i % step === 0 || i === eq.length - 1);
  const decs = AI.history.slice(-200).map(h => ({ at: h.at, regime: h.regime, pnl: h.periodPnl ?? null, trades: h.periodTrades ?? null }));
  const done = decs.filter(x => x.pnl != null), l10 = done.slice(-10), p10 = done.slice(-20, -10);
  const avg = (a) => a.length ? a.reduce((x, y) => x + y.pnl, 0) / a.length : null;
  return { eq: pts, dec: decs, quality: l10.length ? Math.round(l10.filter(x => x.pnl > 0).length / l10.length * 100) : null,
    avgLast: avg(l10), avgPrev: avg(p10), periods: done.length, totalPnl: +done.reduce((a, x) => a + x.pnl, 0).toFixed(4) };
}

function state() {
  const X = SS.view();
  X.trades = (X.trades || []).map(t => { const o = Object.assign({}, t); delete o.path; delete o.feat; return o; });
  return {
    env: ENV, keys: HAS_KEYS, fal: !!FAL_KEY, model: FAL_MODEL, password: !!PASSWORD, server: true,
    acc: { equity: ACC.equity, available: ACC.available, err: ACC.err, at: ACC.at },
    ss: X,
    ai: { on: AI.on, busy: AI.busy, every: AI.every, last: AI.last, error: AI.error, maxMargin: capMargin(), maxLev: capLev(), envCaps: !!(ENV_CAP_MARGIN || ENV_CAP_LEV), notes: AI.notes,
      history: AI.history.slice(-50).map(h => ({ at: h.at, regime: h.regime, reason: h.reason, lesson: h.lesson, changes: h.changes, periodPnl: h.periodPnl ?? null, periodTrades: h.periodTrades ?? null })) },
    prog: progress(),
    events: EVENTS.slice(0, 100),
  };
}

async function act(b) {
  const what = String(b.what || "");
  if (what === "start") {
    if (!HAS_KEYS) return "Додайте BYBIT_API_KEY і BYBIT_API_SECRET у Railway → Variables";
    await syncTime(); await refreshAccount();
    if (ACC.err) return "Bybit: " + ACC.err;
    SS.setOn(true);
    return FAL_KEY ? "Запущено; " + aiSet(true) : "Запущено (без AI: немає FAL_KEY)";
  }
  if (what === "stop") { SS.setOn(false); aiSet(false); return "Зупинено: нові входи не відкриваються, відкриті угоди доведе до тейку/стопу"; }
  if (what === "ai-on") return aiSet(true);
  if (what === "ai-off") return aiSet(false);
  if (what === "ai-now") { if (!AI.on) return "Спершу увімкніть AI"; aiTick(true); return "AI думає…"; }
  if (what === "style" && ["pressure", "flow"].includes(b.style)) { const c = SS.setStyle(b.style); lsSet("styleChosen", true); return c.join(", ") || "без змін"; }
  if (what === "invert" && ["off", "on", "auto"].includes(b.inv)) { SS.setParams({ invert: b.inv }); addLog(`🔄 Усе навпаки: ${b.inv === "on" ? "увімкнено" : b.inv === "off" ? "вимкнено" : "авто"}`, "mute"); return "ok"; }
  if (what === "settings" && b.o && typeof b.o === "object") { const ch = SS.setParams(b.o); if (ch.length) addLog("⚙ Налаштування: " + ch.join(", "), "mute"); return ch.length ? "Збережено: " + ch.join(", ") : "Без змін"; }
  if (what === "limits") {
    const m = Number(b.maxMargin), l = Math.round(Number(b.maxLev)), e = Number(b.every);
    if (e > 0) AI.every = Math.max(2, Math.min(120, e)); if (m > 0) AI.maxMargin = m; if (l > 0) AI.maxLev = Math.min(100, l); saveAI();
    return `Збережено: кожні ${AI.every} хв, маржа до ${capMargin()} USDT, плече до x${capLev()}` + (ENV_CAP_MARGIN || ENV_CAP_LEV ? " (жорсткі межі з Railway Variables мають пріоритет)" : "");
  }
  if (what === "optimize") {
    const r = SS.optimizeNow();
    return !r ? "Ще немає угод із записаною траєкторією" : !r.best ? `З ${r.n} угод прибуткової пари не знайдено` :
      r.applied ? `Застосовано: тейк ${r.best.tp}% / стоп ${r.best.sl}% (PF ${r.cur.pf} → ${r.best.pf})` : r.n < 30 ? `Найкраще з ${r.n} угод: тейк ${r.best.tp}% / стоп ${r.best.sl}%, застосую після 30 угод` : "Поточні тейк і стоп уже найкращі";
  }
  if (what === "note-del") { AI.notes.splice(Number(b.i), 1); saveAI(); return "ok"; }
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
        if (url.pathname === "/api/command") return json(200, await command(b.text || ""));
        if (url.pathname === "/api/command/apply") { const text = applyCommand(String(b.id || "")); dump(); return json(200, { text }); }
      } catch (e) { return json(200, { error: String(e && e.message || e) }); }
    }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    return res.end(UI);
  } catch (e) { return json(500, { error: String(e && e.message || e) }); }
}).listen(PORT, "0.0.0.0", () => log(`Stakan scalper :${PORT} · ${ENV} · Bybit keys ${HAS_KEYS ? "yes" : "NO"} · fal ${FAL_KEY ? "yes" : "no"} · ${PASSWORD ? "password on" : "NO UI_PASSWORD (buttons locked)"} · storage ${STORE}`));

// ---------------- start ----------------
(async () => {
  await syncTime();
  await refreshAccount();
  if (ACC.err) addLog("Bybit: " + ACC.err, "down");
  setInterval(refreshAccount, 15000);
  setInterval(syncTime, 30 * 60000);
  if (AUTOSTART && HAS_KEYS && !lsGet("autostarted", false)) {
    lsSet("autostarted", true);
    addLog("AUTOSTART: перший запуск сервера — вмикаю скальпер" + (FAL_KEY ? " і AI" : ""), "up");
    SS.setOn(true); if (FAL_KEY) aiSet(true);
  } else if (SS.params.on || AI.on) addLog(`Сервер перезапущено — продовжую: скальпер ${SS.params.on ? "увімкнено" : "на паузі"}, AI ${AI.on ? "керує" : "вимкнено"}`, "up");
  else addLog("Сервер запущено. Натисніть «▶ Запустити» на сторінці.", "mute");
})();
