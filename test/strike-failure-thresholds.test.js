// Tests for the strike-mover failure-spike thresholds and the 401 single-retry.
//
// Runs offline with plain node — no install, no network. The threshold functions are
// pure, so they are extracted from index.js the same way test/ghl-retry.test.js
// extracts ghlFetch: read the source slice and eval it, rather than requiring the
// module (which would boot Slack, crons and Supabase clients on import).

const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');

function slice(startMarker, endMarker) {
  const a = SRC.indexOf(startMarker);
  const b = SRC.indexOf(endMarker);
  if (a === -1 || b === -1 || b <= a) {
    throw new Error(`could not slice ${startMarker} → ${endMarker} (index.js moved?)`);
  }
  return SRC.slice(a, b);
}

const src = slice('const STRIKE_FAILURE_LIMITS', 'async function strikeBuildDailyDigest');
const {
  STRIKE_FAILURE_LIMITS,
  STRIKE_FAILURE_WARMUP_SWEEPS,
  strikeFailureStatus,
  strikeBucketFailures,
  strikeAssessFailures,
} = eval(`(function () { ${src}; return {
  STRIKE_FAILURE_LIMITS, STRIKE_FAILURE_WARMUP_SWEEPS,
  strikeFailureStatus, strikeBucketFailures, strikeAssessFailures }; })()`);

let failed = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok   ${name}`); return; }
  failed++;
  console.error(`  FAIL ${name}\n       expected ${e}\n       actual   ${a}`);
}

/** n sweeps, each carrying the given failure strings. */
const sweeps = (n, failures = []) =>
  Array.from({ length: n }, () => ({ metadata: { failures: [...failures] } }));
const fail = (status, who = 'Someone') => `${who}: GHL messages fetch ${status}`;

console.log('strikeFailureStatus');
check('parses a trailing status', strikeFailureStatus('Ana: GHL messages fetch 429'), '429');
// All three real message shapes must parse — a mid-string 401 was previously bucketed
// as "other", which would have hidden a dead token from the 401 threshold entirely.
check('parses the conversations/search shape', strikeFailureStatus('Bo: GHL 401 on conversations/search'), '401');
check('parses the opp PUT arrow shape', strikeFailureStatus('Cy: opp PUT abc123 → 429: Too Many Requests'), '429');
check('ignores digits in the contact name', strikeFailureStatus('Agent 007: GHL messages fetch 503'), '503');
check('null on garbage', strikeFailureStatus('no status here'), null);
check('null on empty', strikeFailureStatus(''), null);
check('ignores a 2xx/3xx that is not an error', strikeFailureStatus('Dee: weird 200 thing'), null);

console.log('strikeBucketFailures');
check('buckets by class',
  strikeBucketFailures([fail(401), fail(429), fail(500), fail(503), 'weird']),
  { '401': 1, '429': 1, '5xx': 2, other: 1 });
check('empty input', strikeBucketFailures([]), { '401': 0, '429': 0, '5xx': 0, other: 0 });

console.log('warmup');
{
  // Below warmup we must not judge, even with an obviously alarming burst.
  const r = strikeAssessFailures(sweeps(STRIKE_FAILURE_WARMUP_SWEEPS - 1, Array(50).fill(fail(401))));
  check('under warmup → no verdict', r.level, 'warmup');
  check('under warmup → no reasons', r.reasons.length, 0);
}
{
  const r = strikeAssessFailures(sweeps(STRIKE_FAILURE_WARMUP_SWEEPS, []));
  check('at warmup with no failures → ok', r.level, 'ok');
}

console.log('per-sweep burst vs spread — the distinction the thresholds exist for');
{
  // 10 × 429 in ONE sweep is the observed real burst shape → must alert.
  const burst = sweeps(9, []);
  burst[0] = { metadata: { failures: Array(10).fill(fail(429)) } };
  const r = strikeAssessFailures(burst);
  check('10 × 429 in one sweep → alert', r.level, 'alert');
  check('names the 429 class', /429/.test(r.reasons.join()), true);
}
{
  // The same 10 spread one-per-sweep is background noise → must stay quiet.
  const spread = sweeps(10, [fail(429)]);
  const r = strikeAssessFailures(spread);
  check('10 × 429 spread one-per-sweep → ok', r.level, 'ok');
}

console.log('401 — the observed baseline must NOT alert, a dead token MUST');
{
  // Measured baseline: max 2 per sweep, ~1 per sweep across 17 of 95 sweeps.
  const baseline = sweeps(10, []);
  baseline[0] = { metadata: { failures: [fail(401), fail(401)] } };
  check('2 × 401 in a sweep (observed max) → ok', strikeAssessFailures(baseline).level, 'ok');
}
{
  const dead = sweeps(9, []);
  dead[0] = { metadata: { failures: Array(200).fill(fail(401)) } };
  const r = strikeAssessFailures(dead);
  check('dead token (200 × 401) → alert', r.level, 'alert');
  check('says it is auth', /auth/.test(r.reasons.join()), true);
}

console.log('window totals');
{
  // Under the per-sweep limit every time, but the day total crosses perDay.
  const drip = sweeps(12, [fail(401)]);
  const r = strikeAssessFailures(drip);
  check('12 × 401 across window (perDay 10) → alert', r.level, 'alert');
  check('reason cites the window, not a single sweep', /across the window/.test(r.reasons.join()), true);
}

console.log('mixed classes are reported independently');
{
  const mixed = sweeps(9, []);
  mixed[0] = { metadata: { failures: [...Array(8).fill(fail(429)), ...Array(8).fill(fail(500))] } };
  const r = strikeAssessFailures(mixed);
  check('both classes trip → two reasons', r.reasons.length, 2);
}

console.log('thresholds sit above the measured 30-day maxima (no crying wolf)');
check('401 perSweep > observed max 2', STRIKE_FAILURE_LIMITS['401'].perSweep > 2, true);
check('429 perSweep > 0', STRIKE_FAILURE_LIMITS['429'].perSweep > 0, true);
check('5xx perSweep > observed max 3', STRIKE_FAILURE_LIMITS['5xx'].perSweep > 3, true);

if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log('\nAll strike failure-threshold checks passed.');
