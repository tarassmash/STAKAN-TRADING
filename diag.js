// 🔍 ЧОМУ МІНУС: розбір результату з двох джерел —
//  1) записи самої біржі Bybit (виконання й закриті позиції): до комісій / комісії / після — правда для всіх угод, навіть старих версій;
//  2) журнал угод бота: тейки проти розворотів, монети, плече, інші виходи.
"use strict";
function create(host) {
  const { bx, BG } = host;
  const ST = { at: 0, busy: false, res: null, err: "" };
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  async function pages(path, q, max) {   // Bybit cursor pages, at most 7 days per request
    const out = []; let cursor = "";
    for (let i = 0; i < max; i++) {
      const r = await bx(path, { query: Object.assign({}, q, cursor ? { cursor } : {}), auth: true });
      out.push(...(r.list || [])); cursor = r.nextPageCursor || ""; if (!cursor || !(r.list || []).length) break; await sleep(150);
    }
    return out;
  }
  async function exchange(days) {
    const end = Date.now(), start = end - days * 86400e3, q = { category: "linear", startTime: String(start), endTime: String(end), limit: "100" };
    const ex = await pages("/v5/execution/list", q, 30), cp = await pages("/v5/position/closed-pnl", q, 30);
    const trade = ex.filter(x => !x.execType || x.execType === "Trade"), funding = ex.filter(x => x.execType === "Funding");
    const fees = trade.reduce((a, x) => a + Number(x.execFee || 0), 0), fund = funding.reduce((a, x) => a + Number(x.execFee || 0), 0);
    const net = cp.reduce((a, x) => a + Number(x.closedPnl || 0), 0), vol = trade.reduce((a, x) => a + Number(x.execValue || 0), 0);
    const taker = trade.filter(x => String(x.isMaker) === "false" || x.isMaker === false).length;
    const by = {}; for (const x of cp) { const s = x.symbol; by[s] = by[s] || { sym: s, n: 0, net: 0 }; by[s].n++; by[s].net += Number(x.closedPnl || 0); }
    for (const x of trade) { const s = x.symbol; by[s] = by[s] || { sym: s, n: 0, net: 0 }; by[s].fees = (by[s].fees || 0) + Number(x.execFee || 0); }
    const r4 = (v) => +Number(v || 0).toFixed(4);
    return { days, fills: trade.length, takerShare: trade.length ? Math.round(taker / trade.length * 100) : null, volume: Math.round(vol), net: r4(net), fees: r4(fees), funding: r4(fund), gross: r4(net + fees),
      feePctOfVolume: vol ? +(fees / vol * 100).toFixed(4) : null, coins: Object.values(by).map(c => ({ sym: c.sym, n: c.n, net: r4(c.net), fees: r4(c.fees) })).sort((a, b) => a.net - b.net).slice(0, 8), capped: ex.length >= 3000 };
  }
  function bot(ms) {
    const t = BG.tradesSince(Date.now() - ms), r4 = (v) => +Number(v || 0).toFixed(4), sum = (a) => r4(a.reduce((s, x) => s + x.pnl, 0));
    const takes = t.filter(x => /^тейк/.test(x.how)), turns = t.filter(x => /розворот/.test(x.how)), mirror = t.filter(x => /дзеркало: вихід/.test(x.how));
    const other = t.filter(x => !takes.includes(x) && !turns.includes(x) && !mirror.includes(x));
    const g = (a) => ({ n: a.length, sum: sum(a), avg: a.length ? r4(sum(a) / a.length) : 0 });
    const byLev = {}; for (const x of t) { const k = x.lev ? "x" + x.lev : "?"; byLev[k] = byLev[k] || []; byLev[k].push(x); }
    const byCoin = {}; for (const x of t) { byCoin[x.sym] = byCoin[x.sym] || []; byCoin[x.sym].push(x); }
    const fees = t.filter(x => x.fee != null);
    const runner = t.filter(x => x.runner);
    const reasons = {}; for (const x of other) reasons[x.how] = (reasons[x.how] || 0) + x.pnl;
    return { n: t.length, net: sum(t), takes: g(takes), turns: g(turns), mirror: g(mirror), other: g(other), runner: g(runner),
      otherWhy: Object.entries(reasons).map(([how, v]) => ({ how, sum: r4(v) })).sort((a, b) => a.sum - b.sum).slice(0, 5),
      takesPerTurn: takes.length && turns.length && sum(takes) > 0 ? +(Math.abs(sum(turns) / turns.length) / (sum(takes) / takes.length)).toFixed(1) : null,
      fees: fees.length ? { n: fees.length, sum: r4(fees.reduce((s, x) => s + x.fee, 0)), net: sum(fees) } : null,
      byLev: Object.entries(byLev).map(([k, a]) => ({ lev: k, n: a.length, sum: sum(a) })).sort((a, b) => a.sum - b.sum),
      byCoin: Object.entries(byCoin).map(([k, a]) => ({ sym: k, n: a.length, sum: sum(a) })).sort((a, b) => a.sum - b.sum).slice(0, 6) };
  }
  // the main cause in plain words
  function verdict(ex, b) {
    const out = [];
    if (ex && ex.fills) {
      if (ex.gross >= 0 && ex.net < 0) out.push(`Головна причина — комісії: до комісій угоди дали ${ex.gross >= 0 ? "+" : ""}${ex.gross} USDT, а комісії забрали ${ex.fees} USDT. Тобто стратегія сама по собі не губить — губить кількість угод.`);
      else if (ex.net < 0 && ex.fees > Math.abs(ex.gross) * 0.5) out.push(`Мінус із двох частин: угоди до комісій ${ex.gross} USDT і ще комісії ${ex.fees} USDT (${Math.round(ex.fees / Math.abs(ex.net) * 100)}% усього мінусу).`);
      else if (ex.net < 0) out.push(`Угоди програють і до комісій (${ex.gross} USDT), комісії додають ще ${ex.fees} USDT.`);
      if (ex.net >= 0) out.push(`Зараз у плюсі: ${ex.net >= 0 ? "+" : ""}${ex.net} USDT після комісій (до комісій +${ex.gross}, комісії ${ex.fees} = ${ex.gross > 0 ? Math.round(ex.fees / ex.gross * 100) : 0}% заробленого).`);
      if (ex.takerShare != null && ex.takerShare > 60) out.push(`${ex.takerShare}% виконань — ринкові (taker, дорожча комісія).`);
      if (ex.funding < -0.001 || ex.funding > 0.001) out.push(`Фандинг: ${ex.funding > 0 ? "−" : "+"}${Math.abs(ex.funding)} USDT.`);
    }
    if (b && b.n) {
      if (b.turns.n && b.takes.sum > 0 && Math.abs(b.turns.sum) > b.takes.sum) out.push(`Розвороти з'їдають більше, ніж дають тейки: ${b.takes.n} тейків принесли +${b.takes.sum}, а ${b.turns.n} розвороти забрали ${b.turns.sum}${b.takesPerTurn ? ` — один розворот ≈ ${b.takesPerTurn} тейків` : ""}.`);
      if (b.takes.n && b.takes.avg <= 0) out.push(`Навіть тейки в середньому ${b.takes.avg} USDT — після комісій вони не заробляють (тейк замалий проти комісій або вхід із проковзуванням).`);
      if (b.byLev.length > 1 && b.byLev[0].sum < 0) out.push(`Найбільше втрачено з плечем ${b.byLev[0].lev}: ${b.byLev[0].sum} USDT.`);
      if (b.byCoin.length && b.byCoin[0].sum < 0) out.push(`Найгірша монета: ${b.byCoin[0].sym} ${b.byCoin[0].sum} USDT (${b.byCoin[0].n} угод).`);
      if (b.other.n && b.other.sum < 0) out.push(`Інші виходи (зміна монети, вимкнення, ручні) дали ${b.other.sum} USDT${b.otherWhy[0] ? ` — найбільше «${b.otherWhy[0].how}» ${b.otherWhy[0].sum}` : ""}.`);
    }
    return out.length ? out : ["Даних поки мало — потрібні угоди."];
  }
  async function run() {
    if (ST.busy || !host.keys()) return; ST.busy = true;
    try {
      const [d1, d7] = [await exchange(1), await exchange(7)];
      const b1 = bot(86400e3), b7 = bot(7 * 86400e3);
      ST.res = { at: Date.now(), d1, d7, b1, b7, why: verdict(d1.fills ? d1 : d7, b1.n ? b1 : b7) }; ST.err = "";
    } catch (e) { ST.err = e.message; }
    finally { ST.busy = false; ST.at = Date.now(); }
  }
  setInterval(() => { if (Date.now() - ST.at > 5 * 60000) run(); }, 30000);
  setTimeout(run, 8000);
  return { run, view: () => ({ busy: ST.busy, err: ST.err, res: ST.res }) };
}
module.exports = { create };
