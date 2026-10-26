// 🤖 AI-автопілот (fal.ai) над пробій-ботом: кожні 15 хв і після мінусового розвороту бачить угоди, стрічку, стакан, швидкість стрічки,
// індикатори й відкритий інтерес і сам вирішує: напрям, пауза, закрити все, монета, плече, розворот, тейк, юніти, фільтр потоку.
"use strict";
const SYSTEM = `You are the autonomous trader in charge of a live crypto futures bot on Bybit. Your one goal: make money and not lose it.
How the bot trades ("flip breakout grid"):
- it follows the price: each price level crossed opens a small unit in the current direction with a take one level further (takePct %), plus one runner without a take;
- NO stops: when the price pulls back flipSteps levels from its best point, the bot closes everything and reverses;
- each reversal realises the losses of units opened near the extreme; a saw (noise) makes it reverse often and lose.
- the market flow steers it (useFlow): new entries are skipped when the flow is against the direction by more than flowGate; at a turn, if the flow still pushes the old way,
  it closes but waits for confirmation (a "hold"; gate.saved counts holds where the old direction went on and a losing reversal was avoided); a strong opposite flow turns it early.
You see everything: real recent trades, settings, the flow snapshot of the coin - tape (cvd1/cvd5 = buy-sell delta share over 1/5 min, -1..1; tps = trades per second; speedX/volSpeedX = tape speed vs the last 15 min; bigBuys/bigSells = big prints in the last minute),
book (imb = bid-ask imbalance within 0.3% of the price, -1..1; spreadBps; wallBid/wallAsk = big walls), indicators on 1-min candles (rsi, emaDiff = EMA9 vs EMA21 %, atr %, vwapDist %, move15 %), open interest change (oi.ch5/ch30 %),
score = combined flow -1..1; the fast learning result on the last hours (learned), and the backtest of candidate coins (history).
You may do anything below (omit = keep):
 "direction": "long" | "short" - enter that direction now (closes the opposite units),
 "pauseMin": 0..120 - no new entries for that long (open units stay to take / turn),
 "closeAll": true - close everything now,
 "changeCoin": true and optional "coin": "SYMBOLUSDT" - leave this coin (not allowed when manualCoin is true),
 "leverage": integer minLev..maxLev, "flipSteps": 3..10, "takePct": 0.2..1.0, "maxUnits": 1..10,
 "useFlow": true/false, "flowGate": 0.05..0.95.
Use the flow: e.g. a strong tape + book + rising OI in one direction is a real move; a fast tape with the price going nowhere is a saw (wider flipSteps, pause, or another coin);
RSI extremes against a weak flow warn of a pullback. Prefer doing nothing over guessing. The server enforces: leverage can rise only after a proven plus (4+ reversals in profit since the last change) and never above levCap.
Answer with ONE JSON object only, e.g. {"direction":null,"pauseMin":0,"leverage":null,"flipSteps":null,"takePct":null,"maxUnits":null,"useFlow":null,"flowGate":null,"changeCoin":false,"coin":null,"closeAll":false,"reason":"коротко українською, на які дані спираєшся"}`;

function create(host) {
  const { BG, engineLog, lsGet, lsSet } = host;
  const KEY = (process.env.FAL_KEY || process.env.FAL_API_KEY || "").trim();
  const MODEL = (process.env.FAL_MODEL || "google/gemini-2.5-flash").trim();
  const URL = (process.env.TEST_FAL_URL || process.env.FAL_URL || "https://fal.run/fal-ai/any-llm").trim();
  const ST = Object.assign({ at: 0, last: null, err: "" }, lsGet("ai.st", {}));
  let busy = false;

  async function ask(prompt) {
    const r = await fetch(URL, { method: "POST", headers: { Authorization: "Key " + KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ model: MODEL, system_prompt: SYSTEM, prompt, temperature: 0.2, max_tokens: 700 }), signal: AbortSignal.timeout(90000) });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || j.error) throw new Error(`fal ${r.status}: ${j.error || (j.detail && JSON.stringify(j.detail).slice(0, 160)) || "помилка"}`);
    const m = String(j.output || "").match(/\{[\s\S]*\}/);
    if (!m) throw new Error("AI не повернув JSON");
    return JSON.parse(m[0]);
  }
  async function review(why) {
    if (!KEY || busy) return;
    const b = BG.brief(); if (!b || !BG.params.on || !BG.params.flip) return;
    busy = true; ST.at = Date.now();
    try {
      const d = await ask(`Why now: ${why}\nBalance: ${host.equity().toFixed(4)} USDT\nBot state:\n${JSON.stringify(b)}`);
      const o = {}, num = (v) => v != null && v !== "" && Number.isFinite(Number(v));
      if (num(d.leverage)) o.lev = Number(d.leverage);
      if (num(d.flipSteps)) o.flipSteps = Number(d.flipSteps);
      if (num(d.takePct)) o.takePct = Math.max(0.2, Math.min(1, Number(d.takePct)));
      if (num(d.maxUnits)) o.maxUnits = Number(d.maxUnits);
      if (typeof d.useFlow === "boolean") o.useFlow = d.useFlow;
      if (num(d.flowGate)) o.flowGate = Number(d.flowGate);
      // hard rule (the only one): more leverage only after a proven plus and never above the history cap
      const proven = b.sinceLastChange.flips >= 4 && b.sinceLastChange.pnl > 0;
      if (o.lev != null && o.lev > b.lev && (!proven || o.lev > (b.levCap || b.lev))) delete o.lev;
      const reason = String(d.reason || "").slice(0, 400);
      let ch = [];
      if (d.closeAll === true) { await BG.closeAll(); ch.push("закрито все"); }
      else if (d.changeCoin === true && !b.manualCoin) { await BG.changeCoin(typeof d.coin === "string" ? d.coin.toUpperCase().replace(/[^A-Z0-9]/g, "") : "", reason); ch.push("інша монета" + (d.coin ? " " + d.coin : "")); }
      else {
        ch = ch.concat(await BG.tune(o, reason || "рішення AI", "ai"));
        if (d.direction === "long" || d.direction === "short") { const dd = d.direction === "long" ? 1 : -1; if (dd !== b.dir || !b.openUnits) { await BG.forceDir(dd, reason); ch.push(d.direction === "long" ? "лонг зараз" : "шорт зараз"); } }
        if (num(d.pauseMin) && Number(d.pauseMin) > 0) { BG.pause(Number(d.pauseMin), reason); ch.push(`пауза ${Math.round(d.pauseMin)} хв`); }
      }
      ST.last = { at: Date.now(), why, ch, reason }; ST.err = "";
      if (!ch.length) engineLog(`🤖 AI: ${b.sym} — без змін${reason ? " (" + reason + ")" : ""}`, "mute");
    } catch (e) { ST.err = e.message; engineLog("🤖 AI: " + e.message, "down"); }
    finally { busy = false; lsSet("ai.st", ST); }
  }
  // every hour; after the bot's own risk cut - a second opinion (no more often than every 20 min)
  const EVERY = Number(process.env.AI_EVERY_MIN || 15);
  setInterval(() => { if (Date.now() - ST.at >= EVERY * 60000) review("плановий огляд"); }, 30000);
  function onLoss() { if (Date.now() - ST.at >= 5 * 60000) setTimeout(() => review("мінусовий розворот"), 8000); }
  function view() { return { on: !!KEY, model: MODEL, last: ST.last, err: ST.err, every: EVERY, next: KEY ? Math.max(0, Math.round((ST.at + EVERY * 60000 - Date.now()) / 60000)) : null }; }
  return { review, onLoss, view };
}
module.exports = { create };
