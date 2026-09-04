// Rules test for the ICM cost report's pricing math — the numbers Ron reads
// as dollars. Run:  node test/icm-cost-report.test.js
//
// Extracts the real pure block out of index.js rather than copying it, so the
// test can never drift from shipped behaviour (same approach as cron-liveness
// — index.js boots the Slack app on require).
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const ICM_MODEL_RATES'),
  SRC.indexOf('// ── end ICM pure block'),
);
if (!block) { console.error('FAIL: could not extract the ICM pure block'); process.exit(1); }

const { icmRates, icmCostUsd, icmSummarize, fmtUsd, fmtPct } =
  new Function(`${block}; return { icmRates, icmCostUsd, icmSummarize, fmtUsd, fmtPct };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const near = (a, b) => Math.abs(a - b) < 1e-9;

// ── 1. Prefix matching. Dated ids must resolve; unknown models must never
// price at $0 (that would silently hide spend from the report).
check('1a  dated haiku id resolves to haiku rates', icmRates('claude-haiku-4-5-20251001').in, 1);
check('1b  sonnet-4-6 exact', icmRates('claude-sonnet-4-6').out, 15);
check('1c  sonnet-5 (AXON) resolves', icmRates('claude-sonnet-5').write, 6);
check('1d  unknown model falls back to sonnet rates, not $0', icmRates('claude-opus-9').in, 3);
check('1e  null model does not throw', icmRates(null).in, 3);

// ── 2. Cost math against hand-computed dollars (rates: sonnet $3/$15,
// write 1h $6, read $0.3 per MTok).
const M = 1e6;
check('2a  1M plain input = $3 actual and uncached',
  (() => { const c = icmCostUsd({ model: 'claude-sonnet-4-6', tin: M, tout: 0, cw: 0, cr: 0 }); return near(c.actual, 3) && near(c.uncached, 3); })(), true);
check('2b  1M cache WRITE costs $6 actual vs $3 uncached (premium, not a saving)',
  (() => { const c = icmCostUsd({ model: 'claude-sonnet-4-6', tin: 0, tout: 0, cw: M, cr: 0 }); return near(c.actual, 6) && near(c.uncached, 3); })(), true);
check('2c  1M cache READ costs $0.30 actual vs $3 uncached (the 90% saving)',
  (() => { const c = icmCostUsd({ model: 'claude-sonnet-4-6', tin: 0, tout: 0, cw: 0, cr: M }); return near(c.actual, 0.3) && near(c.uncached, 3); })(), true);
check('2d  output tokens identical on both sides',
  (() => { const c = icmCostUsd({ model: 'claude-sonnet-4-6', tin: 0, tout: M, cw: 0, cr: 0 }); return near(c.actual, 15) && near(c.uncached, 15); })(), true);

// ── 3. Summarize over the REAL first post-ICM production turn (verified live
// 2026-09-03, agent_activity): 3 calls, and the cold write must make the cold
// call MORE expensive than uncached — savings only appear on reads.
const liveTurn = [
  { model: 'claude-sonnet-4-6', calls: 1, tin: 3, tout: 63,  cw: 19262, cr: 0 },
  { model: 'claude-sonnet-4-6', calls: 1, tin: 1, tout: 125, cw: 359,   cr: 19262 },
  { model: 'claude-sonnet-4-6', calls: 1, tin: 3, tout: 6,   cw: 1764,  cr: 17228 },
];
const t = icmSummarize(liveTurn);
check('3a  call count', t.calls, 3);
check('3b  hit rate = reads / total context', near(t.hitRate, (19262 + 17228) / (7 + 21385 + 36490)), true);
check('3c  the two read-heavy calls net a saving overall', t.saved > 0, true);
check('3d  cold-cache-only rows cost MORE than uncached (write premium is honest)',
  icmSummarize([liveTurn[0]]).saved < 0, true);

// ── 4. Formatting. Slack lines Ron reads.
check('4a  usd', fmtUsd(1.005), '$1.00');
check('4b  pct rounds', fmtPct(0.666), '67%');
check('4c  empty summarize is all zeros, no NaN', (() => { const z = icmSummarize([]); return z.actual === 0 && z.hitRate === 0; })(), true);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall checks passed');
