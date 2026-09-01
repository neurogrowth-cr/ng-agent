// Rules test for the cron next-fire arithmetic used by the liveness audit.
//   Run:  node test/cron-next-fire.test.js
//
// Extracts the real block out of index.js (same region cron-liveness.test.js
// reads) so the test cannot drift from shipped behaviour.
//
// The motivating case is case 1: on 2026-08-21 a session shipped the
// weekdays-only open-deal sweep, told Ron the first run was "tomorrow", and
// scheduled its own verification for the next morning — which was a SATURDAY.
// It then spent ten minutes diagnosing a cron that had correctly not run.
// Every expectation below is an epoch instant (CR is UTC−6, no DST), so these
// assertions hold regardless of the machine's local timezone.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const STATIC_CRON_SCHEDULES'),
  SRC.indexOf('const VANISHED_LOOKBACK_MS'),
);
if (!block || block.length < 100) { console.error('FAIL: could not extract the cron block'); process.exit(1); }

const { nextCronFire, prevCronFire, parseCronExpr, formatCronFire, STATIC_CRON_SCHEDULES } =
  new Function(`${block}; return { nextCronFire, prevCronFire, parseCronExpr, formatCronFire, STATIC_CRON_SCHEDULES };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const at = iso => Date.parse(iso);
const iso = ms => (ms === null ? null : new Date(ms).toISOString());

// ── 1. THE SATURDAY CASE — the whole reason this code exists ─────────────────
// Standing on Sat 2026-08-22 17:33 CR, asking a weekdays-only 10:00 sweep when
// it fires. The answer is Monday, and its last chance was Friday — before that
// Friday-evening deploy, which is why "never logged" was health, not failure.
const SAT = at('2026-08-22T23:33:00Z'); // Sat 17:33 CR
check('1a weekdays-only sweep next fire is MONDAY, not "tomorrow"',
  iso(nextCronFire('0 10 * * 1-5', SAT)), '2026-08-24T16:00:00.000Z'); // Mon 10:00 CR
check('1b its previous chance was Friday 10:00 CR',
  iso(prevCronFire('0 10 * * 1-5', SAT)), '2026-08-21T16:00:00.000Z');
check('1c that previous chance predates the Fri 16:08 CR deploy → not due yet',
  prevCronFire('0 10 * * 1-5', SAT) < at('2026-08-21T22:08:00Z'), true);
check('1d the same sweep made daily (PR #112) fires the very next morning',
  iso(nextCronFire('0 10 * * *', SAT)), '2026-08-23T16:00:00.000Z'); // Sun 10:00 CR
check('1e Monday-only zombie digest',
  iso(nextCronFire('45 8 * * 1', SAT)), '2026-08-24T14:45:00.000Z'); // Mon 08:45 CR

// ── 2. Every shape the registry actually uses ────────────────────────────────
check('2a step minutes inside an hour window, past the window',
  iso(nextCronFire('*/30 7-20 * * *', at('2026-08-23T02:45:00Z'))), '2026-08-23T13:00:00.000Z'); // 20:45 → 07:00 CR
check('2b step minutes mid-window',
  iso(nextCronFire('*/30 7-20 * * *', at('2026-08-22T14:05:00Z'))), '2026-08-22T14:30:00.000Z');
check('2c stepped hour range (7-21/2)',
  iso(nextCronFire('0 7-21/2 * * *', at('2026-08-22T14:00:00Z'))), '2026-08-22T15:00:00.000Z'); // 08:00 → 09:00 CR
check('2d hour list (9,17)',
  iso(nextCronFire('0 9,17 * * *', at('2026-08-22T16:00:00Z'))), '2026-08-22T23:00:00.000Z'); // 10:00 → 17:00 CR
check('2e weekday range starting Tuesday, asked on a Sunday',
  iso(nextCronFire('30 6 * * 2-6', at('2026-08-23T18:00:00Z'))), '2026-08-25T12:30:00.000Z'); // Tue 06:30 CR
check('2f day-of-month (monthly) crosses into the next month',
  iso(nextCronFire('0 7 15 * *', SAT)), '2026-09-15T13:00:00.000Z'); // Sep 15 07:00 CR
check('2g cron accepts both 0 and 7 for Sunday',
  [iso(nextCronFire('0 9 * * 0', SAT)), iso(nextCronFire('0 9 * * 7', SAT))],
  ['2026-08-23T15:00:00.000Z', '2026-08-23T15:00:00.000Z']);
check('2h a fire exactly now is not "next" — strictly after',
  iso(nextCronFire('0 10 * * *', at('2026-08-22T16:00:00Z'))), '2026-08-23T16:00:00.000Z');
check('2i consecutive */30 fires are 30 minutes apart',
  nextCronFire('*/30 * * * *', nextCronFire('*/30 * * * *', SAT)) - nextCronFire('*/30 * * * *', SAT), 1800000);

// ── 3. Unreadable expressions degrade to null, never to a confident wrong time ─
check('3a non-standard macro', nextCronFire('@daily', SAT), null);
check('3b wrong field count',  nextCronFire('0 10 * *', SAT), null);
check('3c out-of-range hour',  nextCronFire('0 99 * * *', SAT), null);
check('3d nonsense',           [nextCronFire('abc', SAT), prevCronFire('', SAT)], [null, null]);
check('3e formatter says so instead of printing a fake date',
  formatCronFire(nextCronFire('@daily', SAT)), 'unknown (unreadable expression)');

// ── 4. The live registry — this is the regression guard ──────────────────────
// If someone adds an expression shape the parser can't read, CI fails here
// instead of the audit quietly rendering "unknown" to Ron every morning.
const unparseable = Object.entries(STATIC_CRON_SCHEDULES).filter(([, e]) => !parseCronExpr(e)).map(([a]) => a);
check('4a every declared expression parses', unparseable, []);
const NOW = at('2026-08-23T23:51:00Z'); // Sun 17:51 CR
const unbracketed = Object.entries(STATIC_CRON_SCHEDULES)
  .filter(([, e]) => !(prevCronFire(e, NOW) < NOW && nextCronFire(e, NOW) > NOW))
  .map(([a]) => a);
check('4b prev/next bracket the asking instant for every declared job', unbracketed, []);
check('4c the audit is declared and its next run is computable',
  formatCronFire(nextCronFire(STATIC_CRON_SCHEDULES.runCronLivenessAudit, NOW)).endsWith('CR'), true);

// ── 5. Formatting contract ───────────────────────────────────────────────────
const fmt = formatCronFire(at('2026-08-24T16:00:00Z'));
check('5a names the weekday, the date and the time in CR', /Mon.*Aug.*24.*10:00 CR$/.test(fmt), true);
check('5b never renders undefined/NaN', /undefined|NaN/.test(fmt), false);

// ── 6. The audit no longer asserts its own schedule in prose ─────────────────
const auditSrc = SRC.slice(SRC.indexOf('const SELF_ACTION'), SRC.indexOf('// ─── START ─'));
check('6a the hardcoded "next run tomorrow" is gone', /next run tomorrow/.test(auditSrc), false);
check('6b the footer computes its next run instead',
  /Audit alive · next run \$\{formatCronFire\(nextCronFire\(/.test(auditSrc), true);
// The triage moved into evaluateCronLiveness, where it now also decides whether
// the audit speaks at all rather than only how it words a bullet. The promise
// checked here is unchanged: every never-logged job says which side of due it is
// on, and a not-yet-due one names the date that will settle it.
check('6c never-logged jobs are triaged, not just listed',
  [/not due yet this deploy/.test(auditSrc),
   /was due \$\{formatCronFire\(s\.prevFire\)\} and logged nothing/.test(auditSrc),
   /prevCronFire\(s\.expr, now\)/.test(auditSrc)], [true, true, true]);
check('6d a not-yet-due job still names its first fire',
  /first fire \$\{formatCronFire\(nextCronFire\(s\.expr, now\)\)\}/.test(auditSrc), true);

console.log(failures ? `\n${failures} check(s) failed.` : '\nAll cron next-fire checks passed.');
process.exit(failures ? 1 : 0);
