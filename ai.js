// 🤖 AI-автопілот (fal.ai) над пробій-ботом: щогодини і після мінусової серії дивиться реальні угоди
// і вирішує, що змінити — плече, відстань розвороту, тейк або монету. Усе в безпечних межах; рішення пишуться в журнал.
"use strict";
const SYSTEM = `You tune a live crypto futures bot on Bybit. The bot ("flip breakout grid"):
- follows the price: each price level crossed opens a small unit in the current direction with a take one level further (takePct %), plus one runner without a take;
- there are NO stops: when the price pulls back flipSteps levels from its best point, the bot closes everything and reverses direction;
- every reversal realises the losses of the units opened near the extreme; noise (saw) makes it reverse often and lose;
- leverage multiplies both profit and the cost of each reversal. The account is small; liquidation must never come close.
Your job: from the REAL recent trades decide what to change so that the series of trades becomes positive.
Allowed changes (omit or null = keep):
 "leverage": integer between minLev and maxLev (minLev = smallest leverage at which the exchange minimum order still fits; lower = safer),
 "flipSteps": integer 3..10 (wider = fewer noise reversals but each costs more),
 "takePct": number 0.2..1.0 (must stay well above fee x2),
 "changeCoin": true to leave this coin (it is a saw or keeps losing) - only if manualCoin is false,
Rules: losing since the last change with several reversals -> reduce risk (lower leverage and/or wider flipSteps) or leave the coin;
clearly positive with few reversals -> you may raise leverage one step but never above levCap; with too little data (< 2 reversals since the last change) change nothing.
Answer with ONE JSON object only: {"leverage":..,"flipSteps":..,"takePct":..,"changeCoin":false,"reason":"short reason in Ukrainian"}`;

function create(host) {
  const { BG, engineLog, lsGet, lsSet } = host;
  const KEY = (process.env.FAL_KEY || process.env.FAL_API_KEY || "").trim();
  const MODEL = (process.env.FAL_MODEL || "google/gemini-2.5-flash").trim();
  const URL = (process.env.TEST_FAL_URL || process.env.FAL_URL || "https://fal.run/fal-ai/any-llm").trim();
  const ST = Object.assign({ at: 0, last: null, err: "" }, lsGet("ai.st", {}));
  let busy = false;

  async function ask(prompt) {
    const r = await fetch(URL, { method: "POST", headers: { Authorization: "Key " + KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ model: MODEL, system_prompt: SYSTEM, prompt, temperature: 0.2, max_tokens: 500 }), signal: AbortSignal.timeout(90000) });
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
      const o = {};
      if (d.leverage != null) o.lev = d.leverage;
      if (d.flipSteps != null) o.flipSteps = d.flipSteps;
      if (d.takePct != null) o.takePct = Math.max(0.2, Math.min(1, Number(d.takePct)));
      // hard rule: the AI may add risk only after a confirmed plus, and never above the history cap
      const proven = b.sinceLastChange.flips >= 4 && b.sinceLastChange.pnl > 0;
      if (o.lev != null && Number(o.lev) > b.lev && (!proven || Number(o.lev) > (b.levCap || b.lev))) delete o.lev;
      if (o.flipSteps != null && Number(o.flipSteps) < b.flipSteps && !proven) delete o.flipSteps;
      const reason = String(d.reason || "").slice(0, 300);
      let ch = await BG.tune(o, reason || "рішення AI", "ai");
      if (d.changeCoin === true && !b.manualCoin) { await BG.changeCoin("", reason); ch = ch.concat("інша монета"); }
      ST.last = { at: Date.now(), why, ch, reason }; ST.err = "";
      if (!ch.length) engineLog(`🤖 AI: ${b.sym} — без змін${reason ? " (" + reason + ")" : ""}`, "mute");
    } catch (e) { ST.err = e.message; engineLog("🤖 AI: " + e.message, "down"); }
    finally { busy = false; lsSet("ai.st", ST); }
  }
  // every hour; after the bot's own risk cut - a second opinion (no more often than every 20 min)
  setInterval(() => { if (Date.now() - ST.at >= 60 * 60000) review("щогодинний огляд"); }, 60000);
  function onLoss() { if (Date.now() - ST.at >= 20 * 60000) setTimeout(() => review("мінусова серія розворотів"), 10000); }
  function view() { return { on: !!KEY, model: MODEL, last: ST.last, err: ST.err, next: KEY ? Math.max(0, Math.round((ST.at + 60 * 60000 - Date.now()) / 60000)) : null }; }
  return { review, onLoss, view };
}
module.exports = { create };
