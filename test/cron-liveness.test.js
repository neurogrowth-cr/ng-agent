// Rules test for the cron liveness audit — the thing that watches the watchers.
// Run:  node test/cron-liveness.test.js
//
// Extracts the real block out of index.js rather than copying it, so the test can
// never drift from shipped behaviour (same approach as make-watchdog and
// gmail-alert-quality — index.js boots the Slack app on require).
//
// Fixtures use real agent_activity shapes and real cron expressions from
// STATIC_CRON_SCHEDULES. The vanished-run cases are the ones actually found in
// production on 2026-08-19.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const STATIC_CRON_SCHEDULES'),
  SRC.indexOf('const VANISHED_LOOKBACK_MS'),
);
if (!block) { console.error('FAIL: could not extract the cron liveness block'); process.exit(1); }

const { expectedMaxGapHours, evaluateCronLiveness, STATIC_CRON_SCHEDULES } =
  new Function(`${block}; return { expectedMaxGapHours, evaluateCronLiveness, STATIC_CRON_SCHEDULES };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const HOUR = 3600000;
const NOW  = Date.parse('2026-08-19T13:30:00Z'); // 07:30 CR, the audit's own slot
const agoH = h => new Date(NOW - h * HOUR).toISOString();

// ── 1. Tolerances. Every one must EXCEED the largest legitimate gap for that
// expression, or the audit cries wolf on a perfectly healthy cron.
check('1a  */10 every 10 min',        expectedMaxGapHours('*/10 * * * *') >= 0.5, true);
check('1b  */30 every 30 min',        expectedMaxGapHours('*/30 * * * *') >= 1.5, true);
check('1c  hourly',                   expectedMaxGapHours('0 * * * *'), 3);
check('1d  once daily',               expectedMaxGapHours('30 5 * * *'), 26);
check('1e  twice daily 9,17 spans a 16h night', expectedMaxGapHours('0 9,17 * * *') > 16, true);
check('1f  windowed */30 7-20 spans a 10.5h night', expectedMaxGapHours('*/30 7-20 * * *') > 10.5, true);
check('1g  windowed 0 7-21/2 spans a 10h night',    expectedMaxGapHours('0 7-21/2 * * *') > 10, true);
check('1h  20 7-19/3 spans a 12h night',            expectedMaxGapHours('20 7-19/3 * * *') > 12, true);
check('1i  weekdays 9am spans a 72h weekend',       expectedMaxGapHours('0 9 * * 1-5') > 72, true);
check('1j  hourly on weekdays also spans the weekend', expectedMaxGapHours('0 * * * 1-5') > 72, true);
check('1k  Tue-Sat spans a 72h gap',                expectedMaxGapHours('30 6 * * 2-6') > 72, true);
check('1l  weekly (single day) allows a full week + margin', expectedMaxGapHours('0 17 * * 5') > 168, true);
// The flat 76h rule cried wolf on both of these. Mon/Wed has a real 5-day Wed→Mon
// gap; Tue/Sat has a real 4-day Tue→Sat gap. Both are healthy schedules.
check('1p  Mon/Wed tolerates its real 5-day gap',  expectedMaxGapHours('0 18 * * 1,3') > 120, true);
check('1q  Tue/Sat tolerates its real 4-day gap',  expectedMaxGapHours('0 7 * * 2,6') > 96, true);
check('1r  but Mon/Wed is not absurdly lenient',   expectedMaxGapHours('0 18 * * 1,3') < 168, true);
check('1s  a contiguous weekday range is unchanged at 76h', expectedMaxGapHours('0 9 * * 1-5'), 76);
check('1m  monthly allows 32 days',                 expectedMaxGapHours('0 7 15 * *'), 768);

// Every declared schedule must produce a finite, positive tolerance. A typo that
// yielded NaN would silently never alarm.
const bad = Object.entries(STATIC_CRON_SCHEDULES)
  .filter(([, e]) => !(expectedMaxGapHours(e) > 0) || !Number.isFinite(expectedMaxGapHours(e)))
  .map(([a]) => a);
check('1n  every declared schedule yields a usable tolerance', bad, []);
check('1o  the audit declares itself', 'runCronLivenessAudit' in STATIC_CRON_SCHEDULES, true);

// ── 2. Stale detection ──
const EXP = { checkMakeScenarioHealth: '*/10 * * * *', runSalesStandup: '0 9 * * 1-5' };

check('2a healthy cron is not stale',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: {
    checkMakeScenarioHealth: agoH(0.2), runSalesStandup: agoH(24),
  }, recentRuns: [], now: NOW }).stale.map(s => s.action), []);

check('2b a 10-minute cron silent for 3h is stale',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: {
    checkMakeScenarioHealth: agoH(3), runSalesStandup: agoH(24),
  }, recentRuns: [], now: NOW }).stale.map(s => s.action), ['checkMakeScenarioHealth']);

check('2c a weekday cron silent over the weekend is NOT stale',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: {
    checkMakeScenarioHealth: agoH(0.2), runSalesStandup: agoH(70),
  }, recentRuns: [], now: NOW }).stale.map(s => s.action), []);

check('2d a weekday cron silent for 5 days IS stale',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: {
    checkMakeScenarioHealth: agoH(0.2), runSalesStandup: agoH(120),
  }, recentRuns: [], now: NOW }).stale.map(s => s.action), ['runSalesStandup']);

// ── 3. Silent — declared but never logged. This is the shape of the 6 AM anomaly
// detector, the one cron never wrapped in wrapCronJob and therefore invisible.
check('3a a declared cron with no successful run ever is reported',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: { checkMakeScenarioHealth: agoH(0.2) },
    recentRuns: [], now: NOW }).silent.map(s => s.action), ['runSalesStandup']);

check('3b the audit does not open by reporting itself as broken',
  evaluateCronLiveness({ expectations: { runCronLivenessAudit: '30 7 * * *', runSalesStandup: '0 9 * * 1-5' },
    lastOkByAction: {}, recentRuns: [], now: NOW }).silent.map(s => s.action), ['runSalesStandup']);

// ── 4. Vanished — started, no terminal row. The real 2026-08-19 production case.
const runs = [
  { action: 'runAutoStrikeMover',              status: 'started', created_at: agoH(5), correlation_id: 'c1' },
  { action: 'runAutoStrikeMover',              status: 'ok',      created_at: agoH(5), correlation_id: 'c1' },
  { action: 'dynamic_cron:Fulfillment EOD Pulse', status: 'started', created_at: agoH(30), correlation_id: 'c2' },
  { action: 'checkMakeScenarioHealth',         status: 'started', created_at: agoH(1), correlation_id: 'c3' },
  { action: 'checkMakeScenarioHealth',         status: 'error',   created_at: agoH(1), correlation_id: 'c3' },
];
const v = evaluateCronLiveness({ expectations: { ...EXP, runAutoStrikeMover: '0 7-21/2 * * *',
  'dynamic_cron:Fulfillment EOD Pulse': '0 18 * * 1,3' }, lastOkByAction: {}, recentRuns: runs, now: NOW });
check('4a only the run with no terminal row is vanished',
  v.vanished.map(x => x.action), ['dynamic_cron:Fulfillment EOD Pulse']);
check('4b an errored run is finished, not vanished',
  v.vanished.some(x => x.action === 'checkMakeScenarioHealth'), false);

// ── 5. Drift — an action logging that nobody declared. Five crons appeared this
// way in a single day without the inventory noticing.
check('5a an undeclared action is reported as drift',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: {}, recentRuns: [
    { action: 'runBrandNewThing', status: 'ok', created_at: agoH(1), correlation_id: 'z1' },
  ], now: NOW }).drifted, ['runBrandNewThing']);

check('5b a declared action is not drift',
  evaluateCronLiveness({ expectations: EXP, lastOkByAction: {}, recentRuns: [
    { action: 'runSalesStandup', status: 'ok', created_at: agoH(1), correlation_id: 'z2' },
  ], now: NOW }).drifted, []);

// ── 6. A totally healthy fleet produces nothing at all, so the audit stays quiet.
const clean = evaluateCronLiveness({
  expectations: EXP,
  lastOkByAction: { checkMakeScenarioHealth: agoH(0.2), runSalesStandup: agoH(24) },
  recentRuns: [
    { action: 'checkMakeScenarioHealth', status: 'started', created_at: agoH(1), correlation_id: 'k1' },
    { action: 'checkMakeScenarioHealth', status: 'ok',      created_at: agoH(1), correlation_id: 'k1' },
  ],
  now: NOW,
});
check('6a a healthy fleet reports nothing',
  [clean.stale.length, clean.vanished.length, clean.drifted.length, clean.silent.length], [0, 0, 0, 0]);

// ── 7. No expiry. This is the clause that killed the previous audit fleet: it
// could disable itself and nothing could re-arm it.
const auditSrc = SRC.slice(SRC.indexOf('const STATIC_CRON_SCHEDULES'), SRC.indexOf('// ─── START ─'));
check('7a the audit contains no self-termination',
  /disable\s+THIS\s+task|enabled\s*=\s*false|update_scheduled_task/i.test(auditSrc), false);
check('7b the audit states its own liveness',
  /Audit alive/.test(auditSrc), true);

// ── 8. Never-logged splits into a finding and a non-finding.
//
// The 2026-09-01 report is the case: three crons shipped the day before, none of
// them due yet, all three posted under a ⚠️ header — and two of them would have
// re-posted every morning for a week while nothing at all was wrong.
const WEEKLY_SUN = '0 20 * * 0';   // runWinningAdsSweep — first fire is days away
const DAILY_6AM  = '0 6 * * *';    // due at 06:00 CR, an hour before this audit
const silentOf = (expr, opts) => evaluateCronLiveness({
  expectations: { theJob: expr }, lastOkByAction: {}, recentRuns: [], now: NOW, ...opts,
});

check('8a a cron declared hours ago is pending, not a finding',
  (() => { const r = silentOf(WEEKLY_SUN, { bootMs: NOW - 2 * HOUR });
    return [r.missed.length, r.pending.map(x => x.action)]; })(), [0, ['theJob']]);

check('8b a cron that was due since boot and logged nothing IS a finding',
  (() => { const r = silentOf(DAILY_6AM, { bootMs: NOW - 4 * HOUR });
    return [r.missed.map(x => x.action), r.missed[0].reason]; })(),
  [['theJob'], 'due-since-boot']);

// The hole boot-scoping alone leaves. This job has NEVER succeeded, so it can
// never be `stale` — and every redeploy landing after Sunday 20:00 renews the
// "not due yet this deploy" excuse. On Railway a deploy usually does.
check('8c a job silent since declaration is caught even when boot excuses it',
  (() => { const r = silentOf(WEEKLY_SUN, {
      bootMs: NOW - 2 * HOUR,
      firstSeenSilentByAction: { theJob: new Date(NOW - 30 * 24 * HOUR).toISOString() },
    });
    return [r.missed.map(x => x.action), r.missed[0].reason]; })(),
  [['theJob'], 'watched-past-tolerance']);

check('8d but the backstop waits out the job\'s own tolerance before firing',
  (() => { const r = silentOf(WEEKLY_SUN, {
      bootMs: NOW - 2 * HOUR,
      firstSeenSilentByAction: { theJob: new Date(NOW - 48 * HOUR).toISOString() },
    });
    return [r.missed.length, r.pending.length]; })(), [0, 1]);

check('8e no bootMs and no history means nothing is claimed missed',
  silentOf(WEEKLY_SUN, {}).missed.length, 0);

check('8f missed and pending together account for every silent job',
  (() => { const r = evaluateCronLiveness({
      expectations: { weekly: WEEKLY_SUN, daily: DAILY_6AM }, lastOkByAction: {},
      recentRuns: [], now: NOW, bootMs: NOW - 4 * HOUR });
    return [r.silent.length, r.missed.length + r.pending.length]; })(), [2, 2]);

// ── 9. Two invariants the message builder must hold, guarded at the source
// because breaking either is silent — the audit keeps posting and keeps looking
// like coverage.
check('9a the alarm total counts missed, never merely-pending jobs',
  /const total = stale\.length \+ vanished\.length \+ drifted\.length \+ missed\.length;/.test(auditSrc), true);
check('9b pending jobs are reported, but only as a footnote',
  /if \(pending\.length\) lines\.push\(`\\n_/.test(auditSrc), true);
// The backstop reads its own history out of output.silent. Narrowing that field
// to just the findings would disable check 8c without failing anything.
check('9c the activity row still records EVERY silent job, not just the findings',
  /silent: silent\.map\(s => s\.action\), silent_missed: missed\.map\(s => s\.action\)/.test(auditSrc), true);

console.log(failures ? `\n${failures} check(s) failed.` : '\nAll cron-liveness checks passed.');
process.exit(failures ? 1 : 0);
