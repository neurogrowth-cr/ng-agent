// Rules test for the Kai health watch.  Run:  node test/kai-health-watch.test.js
//
// lib/kaiHealth.js is pure (no Slack, no DB), so it is required directly.
// Fixtures come from real Kai incidents in prosp_reply_queue / prosp_reply_log:
//   2026-09-16  11 replies failed on the Anthropic spending limit
//   2026-09-14  channel_not_found on placeholder Slack channel IDs
//   2026-09-23  Palantier auto-send held back because the client replied first
const kai = require('../lib/kaiHealth');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MIN = 60 * 1000;
// Tue 2026-09-23 10:00 CR = 16:00 UTC (inside the intake window).
const T0 = new Date('2026-09-23T16:00:00Z');
const at = (m) => new Date(T0.getTime() + m * MIN);

const HEALTHY = {
  errors: [], stuck: { n: 0, oldest: null }, received: 12, threadCheckFails: 0,
  autoSend: { sent: 4, skipped: 0 }, doubleSends: [],
};
const USAGE_LIMIT_ROW = {
  id: 'l1', event: 'error', created_at: '2026-09-16T15:02:00Z', client_name: 'Buena Vista Solutions',
  error: '400 {"type":"error","error":{"type":"invalid_request_error","message":"You have reached your specified API usage limits. You will regain access on 2026-10-01 at 00:00 UTC."}}',
};
const SLACK_ROW = { id: 'l2', event: 'error', created_at: '2026-09-14T15:00:00Z', client_name: 'Ripple Effect | Mario Cardona ', error: 'An API error occurred: channel_not_found' };

const failingIds = (results) => results.filter((r) => r.failing).map((r) => r.id).sort();
function poll(state, { health = { ok: true }, snapshot = HEALTHY, snapshotError = null, now = T0 } = {}) {
  const results = kai.evaluateKaiChecks({ health, snapshot, snapshotError, now });
  return { results, ...kai.stepKaiWatch(state, results, now, { blind: !snapshot }) };
}

// ── 1. Error classification
check('1a  spending limit', kai.classifyKaiError(USAGE_LIMIT_ROW), 'usage_limit');
check('1b  dead Slack channel', kai.classifyKaiError(SLACK_ROW), 'slack');
check('1c  slack_notify_failed event', kai.classifyKaiError({ event: 'slack_notify_failed', error: 'boom' }), 'slack');
check('1d  Prosp send failure', kai.classifyKaiError({ event: 'send_failed', error: 'Prosp send failed at POST' }), 'prosp_send');
check('1e  anything else', kai.classifyKaiError({ event: 'error', error: 'Cannot read properties of undefined' }), 'pipeline');

// ── 2. Intake window (CR business hours, weekdays)
check('2a  Tue 10:00 CR is inside', kai.isIntakeWindow(T0), true);
check('2b  Tue 21:00 CR is outside', kai.isIntakeWindow(new Date('2026-09-24T03:00:00Z')), false);
check('2c  Sat 10:00 CR is outside', kai.isIntakeWindow(new Date('2026-09-26T16:00:00Z')), false);
check('2d  Mon 08:00 CR is inside', kai.isIntakeWindow(new Date('2026-09-28T14:00:00Z')), true);

// ── 3. A healthy poll is silent
{
  const r = poll({});
  check('3a  nothing failing', failingIds(r.results), []);
  check('3b  no alerts', r.alerts.length, 0);
  check('3c  no state left behind', r.state, {});
}

// ── 4. Spending limit alerts on the FIRST poll and mentions Ron
{
  const r = poll({}, { snapshot: { ...HEALTHY, errors: [USAGE_LIMIT_ROW] } });
  check('4a  one alert', r.alerts.map((a) => a.id), ['errors:usage_limit']);
  const text = kai.formatKaiAlert(r.alerts[0], 'U05HXGX18H3');
  check('4b  mentions Ron', text.includes('<@U05HXGX18H3>'), true);
  check('4c  names the fix', /Anthropic console/.test(text), true);
  check('4d  names the client in caps', text.includes('BUENA VISTA SOLUTIONS'), true);
  check('4e  no em dash in alert copy', text.includes('—'), false);

  // Same class next poll: no repeat inside 6h.
  const r2 = poll(r.state, { snapshot: { ...HEALTHY, errors: [USAGE_LIMIT_ROW] }, now: at(15) });
  check('4f  no repeat 15 min later', r2.alerts.length, 0);
  const r3 = poll(r2.state, { snapshot: { ...HEALTHY, errors: [USAGE_LIMIT_ROW] }, now: at(6 * 60) });
  check('4g  repeats after 6h, marked as repeat', r3.alerts.map((a) => a.repeat), [true]);

  // Errors stop: the class vanishes from the results, so it recovers once.
  const r4 = poll(r3.state, { now: at(6 * 60 + 15) });
  check('4h  recovery posted', r4.recoveries.map((x) => x.id), ['errors:usage_limit']);
  check('4i  recovery copy', kai.formatKaiRecovery(r4.recoveries[0]).startsWith('✅'), true);
  const r5 = poll(r4.state, { now: at(6 * 60 + 30) });
  check('4j  recovery posted only once', r5.recoveries.length, 0);
}

// ── 5. Two error classes at once are two separate alerts
{
  const r = poll({}, { snapshot: { ...HEALTHY, errors: [USAGE_LIMIT_ROW, SLACK_ROW] } });
  check('5a  both classes alert', r.alerts.map((a) => a.id).sort(), ['errors:slack', 'errors:usage_limit']);
}

// ── 6. Health needs 3 consecutive failures (a deploy restart never pages)
{
  const down = { ok: false, error: 'HTTP 502' };
  let s = {};
  let r = poll(s, { health: down }); s = r.state;
  check('6a  1st failure silent', r.alerts.length, 0);
  r = poll(s, { health: down, now: at(15) }); s = r.state;
  check('6b  2nd failure silent', r.alerts.length, 0);
  r = poll(s, { health: down, now: at(30) }); s = r.state;
  check('6c  3rd failure alerts', r.alerts.map((a) => a.id), ['health']);
  r = poll(s, { now: at(45) }); s = r.state;
  check('6d  recovers', r.recoveries.map((x) => x.id), ['health']);

  // A blip in the middle resets the streak.
  let b = poll({}, { health: down }).state;
  b = poll(b, { now: at(15) }).state;
  const after = poll(poll(b, { health: down, now: at(30) }).state, { health: down, now: at(45) });
  check('6e  streak resets after a good poll', after.alerts.length, 0);
}

// ── 7. Fail closed: a blind poll alerts on itself and invents no recoveries
{
  let s = poll({}, { snapshot: { ...HEALTHY, errors: [SLACK_ROW] } }).state; // slack alerted
  let r = poll(s, { snapshot: null, snapshotError: 'connection refused', now: at(15) }); s = r.state;
  check('7a  1st blind poll silent', r.alerts.length, 0);
  check('7b  no false recovery of the open Slack alert', r.recoveries.length, 0);
  r = poll(s, { snapshot: null, snapshotError: 'connection refused', now: at(30) }); s = r.state;
  check('7c  2nd blind poll alerts', r.alerts.map((a) => a.id), ['watcher_read']);
  check('7d  alert quotes the read error', r.alerts[0].reason.includes('connection refused'), true);
  check('7e  Slack alert still open', Boolean(s['errors:slack'] && s['errors:slack'].alerted), true);
}

// ── 8. Intake silence: only during business hours, needs 2 polls
{
  const quiet = { ...HEALTHY, received: 0 };
  let r = poll({}, { snapshot: quiet });
  check('8a  1st quiet poll silent', r.alerts.length, 0);
  r = poll(r.state, { snapshot: quiet, now: at(15) });
  check('8b  2nd quiet poll alerts', r.alerts.map((a) => a.id), ['intake']);
  const night = new Date('2026-09-24T04:00:00Z'); // 22:00 CR
  const n1 = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: quiet, now: night });
  check('8c  not evaluated at night', n1.find((x) => x.id === 'intake').skip, true);
  const kept = kai.stepKaiWatch(r.state, n1, night);
  check('8d  night skip neither recovers nor re-alerts', [kept.alerts.length, kept.recoveries.length], [0, 0]);
}

// ── 9. Auto-send held back too often (thread-check ordering broke)
{
  const res = (sent, skipped) => kai.evaluateKaiChecks({ health: { ok: true }, snapshot: { ...HEALTHY, autoSend: { sent, skipped } }, now: T0 })
    .find((x) => x.id === 'skip_share').failing;
  check('9a  Palantier today: 4 sent, 0 held', res(4, 0), false);
  check('9b  2 of 4 held: too few to judge', res(2, 2), false);
  check('9c  3 of 5 held: red', res(2, 3), true);
  check('9d  1 of 5 held: fine', res(4, 1), false);
}

// ── 10. Thread check failing and double sends
{
  const tc = (n) => kai.evaluateKaiChecks({ health: { ok: true }, snapshot: { ...HEALTHY, threadCheckFails: n }, now: T0 })
    .find((x) => x.id === 'thread_check').failing;
  check('10a 2 fails in an hour: fine', tc(2), false);
  check('10b 3 fails in an hour: red', tc(3), true);

  const dbl = { ...HEALTHY, doubleSends: [{ client_name: 'Palantier AI - Carlos Jimenez', prospect_name: 'Miguel Paredes, PhD', first_at: '2026-09-23T13:31:57Z', second_at: '2026-09-23T13:40:00Z' }] };
  const r = poll({}, { snapshot: dbl });
  check('10c double send alerts on first poll', r.alerts.map((a) => a.id), ['double_send']);
  check('10d names client and prospect', /PALANTIER AI.*Miguel Paredes/.test(r.alerts[0].reason), true);
}

// ── 11. Stuck replies need 2 polls
{
  const stuck = { ...HEALTHY, stuck: { n: 2, oldest: '2026-09-23T15:30:00Z' } };
  let r = poll({}, { snapshot: stuck });
  check('11a 1st poll silent', r.alerts.length, 0);
  r = poll(r.state, { snapshot: stuck, now: at(15) });
  check('11b 2nd poll alerts', r.alerts.map((a) => a.id), ['stuck']);
}

// ── 12. Costa Rica day boundaries (UTC-6, no DST)
check('12a CR midday maps to 06:00 UTC same day', kai.crDayStartUtc(new Date('2026-09-23T13:00:00Z')).toISOString(), '2026-09-23T06:00:00.000Z');
check('12b 23:00 CR on the 22nd is still the 22nd', kai.crDayStartUtc(new Date('2026-09-23T05:00:00Z')).toISOString(), '2026-09-22T06:00:00.000Z');

// ── 13. Digest: baseline, direction-only flags, warmup
function digestData({ yesterday, baseline, extra = {} }) {
  const now = new Date('2026-09-23T14:00:00Z'); // Wed... Tue 08:00 CR
  const todayStart = kai.crDayStartUtc(now);
  const yStart = new Date(todayStart.getTime() - 24 * 3600 * 1000);
  const daily = [];
  for (let i = 1; i <= 29; i += 1) {
    const day = new Date(todayStart.getTime() - i * 24 * 3600 * 1000 - 6 * 3600 * 1000).toISOString().slice(0, 10);
    const dow = new Date(`${day}T12:00:00Z`).getUTCDay();
    const weekend = dow === 0 || dow === 6;
    const row = i === 1 ? yesterday : (weekend ? { received: 2 } : baseline(i));
    if (row) daily.push({ day, sent: 0, escalated: 0, failed: 0, skipped: 0, drafted: 0, ...row });
  }
  return { now, todayStart, yStart, daily, backlog: 540, silence: [], divergence: 0,
    autoSend: [{ client_name: 'Palantier AI - Carlos Jimenez', sent: 4, held: 1, fallback: 0 }], ...extra };
}
{
  const wobble = (i) => ({ received: 25 + (i % 5) * 4, escalated: 2 + (i % 3) });
  const normal = kai.formatKaiDigest(digestData({ yesterday: { received: 28, escalated: 3, sent: 4, drafted: 16 }, baseline: wobble }));
  check('13a normal day is ALL GREEN', normal.includes('✅ ALL GREEN'), true);
  check('13b backlog reported, not flagged', normal.includes('Drafts waiting over 24h: 540 (reported only, not a problem)'), true);
  check('13c auto-send line', normal.includes('PALANTIER AI - CARLOS JIMENEZ: 4 sent · 1 held back (client replied first) · 0 sent to review instead'), true);
  check('13d header uses Max report format', normal.startsWith('`KAI DAILY HEALTH: 2026-09-22`'), true);
  check('13e no em dash in digest copy', normal.includes('—'), false);

  const low = kai.formatKaiDigest(digestData({ yesterday: { received: 3 }, baseline: wobble }));
  check('13f intake collapse flagged', /⚠️ ISSUES: received 3 \(usual \d+\)/.test(low), true);

  const busy = kai.formatKaiDigest(digestData({ yesterday: { received: 60 }, baseline: wobble }));
  check('13g a busy day is NOT a problem', busy.includes('✅ ALL GREEN'), true);

  const failed = kai.formatKaiDigest(digestData({ yesterday: { received: 28, failed: 11 }, baseline: wobble }));
  check('13h 2026-09-16 style failure spike flagged', failed.includes('failed 11'), true);

  const blip = kai.formatKaiDigest(digestData({ yesterday: { received: 28, failed: 2 }, baseline: () => ({ received: 28 }) }));
  check('13i a 2-reply wobble never flags, even at sd 0', blip.includes('✅ ALL GREEN'), true);

  // Days with no rows are real zeros: a month of silence makes 3 replies normal, not low.
  const quietMonth = kai.formatKaiDigest(digestData({ yesterday: { received: 3 }, baseline: () => null }));
  check('13j days with no rows count as zero replies', quietMonth.includes('✅ ALL GREEN') && quietMonth.includes('usual 0 to 0'), true);

  const open = kai.formatKaiDigest(digestData({ yesterday: { received: 28 }, baseline: wobble }), ['errors:usage_limit']);
  check('13k open alerts make the digest ISSUES', open.includes('⚠️ ISSUES') && open.includes('Anthropic spending limit'), true);

  const div = kai.formatKaiDigest(digestData({ yesterday: { received: 28 }, baseline: wobble, extra: { divergence: 2 } }));
  check('13l replies that never reached Slack flagged', div.includes('2 never reached Slack'), true);
}

// ── 14. Silence: flagged once when new, then listed quietly
{
  const base = digestData({ yesterday: { received: 28 }, baseline: () => ({ received: 28 }) });
  const now = base.now; // Tuesday
  const daysAgo = (d) => new Date(now.getTime() - d * 24 * 3600 * 1000).toISOString();
  const fresh = kai.formatKaiDigest({ ...base, silence: [{ client_name: 'Strategia - Andrés Morera', last_reply: daysAgo(5.2), prior: 14, recent: 0 }] });
  check('14a newly quiet client flagged', fresh.includes('Went quiet') && fresh.includes('STRATEGIA') && fresh.includes('⚠️ ISSUES'), true);
  const old = kai.formatKaiDigest({ ...base, silence: [{ client_name: 'Strategia - Andrés Morera', last_reply: daysAgo(9), prior: 12, recent: 0 }] });
  check('14b still-quiet client listed, not flagged', old.includes('Still quiet: STRATEGIA') && old.includes('✅ ALL GREEN'), true);
  const monday = new Date('2026-09-28T14:00:00Z');
  const mon = kai.formatKaiDigest({ ...base, now: monday, silence: [{ client_name: 'X', last_reply: new Date(monday.getTime() - 7 * 24 * 3600 * 1000).toISOString(), prior: 12, recent: 0 }] });
  check('14c Monday catches a client that went quiet over the weekend', mon.includes('Went quiet'), true);
}

// ── 15. The snapshot reader runs every query with the right parameters
(async () => {
  const seen = [];
  const pg = { query: async (sql, params) => { seen.push({ sql, params }); return { rows: [{ n: 0, sent: 0, skipped: 0 }] }; } };
  await kai.readKaiSnapshot(pg, T0);
  check('15a six queries', seen.length, 6);
  check('15b every query gets the poll time', seen.every((q) => q.params[0] === T0.toISOString()), true);
  check('15c intake window passed as hours', seen.find((q) => q.sql === kai.SQL.received).params[1], kai.INTAKE_WINDOW_HOURS);
  check('15d read-only: no writes in any SQL', [...Object.values(kai.SQL), ...Object.values(kai.DIGEST_SQL)].some((s) => /\b(insert|update|delete|drop|alter)\b/i.test(s)), false);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall kai health watch checks passed');
})();
