// Rules test for the Kai health watch.  Run:  node test/kai-health-watch.test.js
//
// lib/kaiHealth.js is pure (no Slack, no DB), so it is required directly.
// Fixtures come from real Kai incidents in prosp_reply_queue / prosp_reply_log:
//   2026-09-16  11 replies failed on the Anthropic spending limit
//   2026-09-14  channel_not_found on placeholder Slack channel IDs
//   2026-09-23  Palantier auto-send held back because the client replied first
//   2026-10-05  Cosiba: Prosp 401 "authentication has expired", fixed by the client in 6 minutes
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
  errors: [], stuck: { n: 0, oldest: null }, prospEvents: true, missedReplies: [], threadCheckFails: [],
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
check('2d  Mon 08:00 CR is outside (campaigns warm up; 10:00 to 19:00)', kai.isIntakeWindow(new Date('2026-09-28T14:00:00Z')), false);
check('2e  Mon 18:30 CR is inside', kai.isIntakeWindow(new Date('2026-09-29T00:30:00Z')), true);
check('2f  Tue 19:30 CR is outside (the old false alarms fired 18:45 to 19:45)', kai.isIntakeWindow(new Date('2026-09-24T01:30:00Z')), false);

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

// ── 8. Prosp silence: only weekdays 10:00 to 19:00, needs 2 polls
{
  const quiet = { ...HEALTHY, prospEvents: false };
  let r = poll({}, { snapshot: quiet });
  check('8a  1st quiet poll silent', r.alerts.length, 0);
  r = poll(r.state, { snapshot: quiet, now: at(15) });
  check('8b  2nd quiet poll alerts', r.alerts.map((a) => a.id), ['prosp_quiet']);
  const night = new Date('2026-09-24T04:00:00Z'); // 22:00 CR
  const n1 = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: quiet, now: night });
  check('8c  not evaluated at night', n1.find((x) => x.id === 'prosp_quiet').skip, true);
  const kept = kai.stepKaiWatch(r.state, n1, night);
  check('8d  night skip neither recovers nor re-alerts', [kept.alerts.length, kept.recoveries.length], [0, 0]);
}

// ── 8b. Replies Prosp reported that never reached Kai (any hour), needs 2 polls
{
  const lost = { ...HEALTHY, missedReplies: [{ client_name: ' Emmis Capital ', n: 2, oldest: '2026-09-23T14:00:00Z' }] };
  let r = poll({}, { snapshot: lost });
  check('8e  1st poll with lost replies silent', r.alerts.length, 0);
  r = poll(r.state, { snapshot: lost, now: at(15) });
  check('8f  2nd poll alerts and names the client', [r.alerts.map((a) => a.id), /EMMIS CAPITAL 2/.test(r.alerts[0].reason)], [['kai_intake'], true]);
  const night = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: lost, now: new Date('2026-09-24T04:00:00Z') });
  check('8g  judged at night too (a lost reply is lost at any hour)', night.find((x) => x.id === 'kai_intake').failing, true);
  check('8h  a quiet afternoon with nothing lost is green', failingIds(kai.evaluateKaiChecks({ health: { ok: true }, snapshot: HEALTHY, now: new Date('2026-11-04T01:30:00Z') })), []);
  const rec = poll(r.state, { snapshot: HEALTHY, now: at(30) });
  check('8i  recovers once nothing is lost', rec.recoveries.map((x) => x.id), ['kai_intake']);
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
  // A Prosp-side failure (timeout / 5xx) in one workspace, minutes before T0.
  const down = (ws, m) => ({ workspace_id: ws, client_name: ws, created_at: at(-m).toISOString(), error_class: '', error: 'conversation_fetch_error: The operation was aborted due to timeout', last_ok_at: null });
  const tc = (rows) => kai.evaluateKaiChecks({ health: { ok: true }, snapshot: { ...HEALTHY, threadCheckFails: rows }, now: T0 })
    .find((x) => x.id === 'thread_check').failing;
  check('10a 2 fails in an hour, one client: fine', tc([down('a', 5), down('a', 10)]), false);
  check('10b 3 fails in an hour: red', tc([down('a', 5), down('a', 10), down('a', 15)]), true);
  check('10b2 2 clients failing at once: red', tc([down('a', 5), down('b', 10)]), true);
  check('10b3 older than an hour: not an outage', tc([down('a', 70), down('a', 80), down('a', 90)]), false);

  const dbl = { ...HEALTHY, doubleSends: [{ client_name: 'Palantier AI - Carlos Jimenez', prospect_name: 'Miguel Paredes, PhD', first_at: '2026-09-23T13:31:57Z', second_at: '2026-09-23T13:40:00Z' }] };
  const r = poll({}, { snapshot: dbl });
  check('10c double send alerts on first poll', r.alerts.map((a) => a.id), ['double_send']);
  check('10d names client and prospect', /PALANTIER AI.*Miguel Paredes/.test(r.alerts[0].reason), true);
}

// ── 10x. Thread check by workspace and error class (Cosiba 2026-10-05)
{
  const EXPIRED = 'conversation_http_401: {"message":"Your authentication has expired. Please reauthenticate in prosp to continue."}';
  const cosiba = (m, extra = {}) => ({ workspace_id: 'ws-cosiba', client_name: 'Cosiba Inversiones ', created_at: at(-m).toISOString(), error_class: '', error: EXPIRED, last_ok_at: null, ...extra });
  const four = [cosiba(10), cosiba(10.2), cosiba(10.3), cosiba(10.5)];

  check('10e classify expired login', kai.classifyThreadCheckError(EXPIRED), 'login_expired');
  check('10f classify 404 account', kai.classifyThreadCheckError('conversation_http_404: {"message":"Account not found"}'), 'account_not_found');
  check('10g classify timeout', kai.classifyThreadCheckError('conversation_fetch_error: aborted'), 'prosp_unreachable');
  check('10h Kai-stamped class wins over the text', kai.unresolvedThreadFailures([cosiba(1, { error_class: 'rate_limited' })])[0].cls, 'rate_limited');

  // Unfixed: four clicks on one expired login are an account problem, not an outage.
  const r1 = poll({}, { snapshot: { ...HEALTHY, threadCheckFails: four } });
  check('10i expired login is not "Prosp down"', failingIds(r1.results), ['thread_check_account']);
  check('10j first poll waits for confirmation', r1.alerts, []);
  const r2 = poll(r1.state, { snapshot: { ...HEALTHY, threadCheckFails: four }, now: at(15) });
  check('10k second poll alerts the account check', r2.alerts.map((a) => a.id), ['thread_check_account']);
  check('10l names the client, cause and count', /COSIBA INVERSIONES: LinkedIn login expired in Prosp \(4 tries/.test(r2.alerts[0].reason), true);
  check('10m tells who has to act', /client has to fix this in Prosp/.test(r2.alerts[0].reason), true);

  // What really happened: Oscar reconnected and a send went through 6 minutes later.
  const fixed = four.map((r) => ({ ...r, last_ok_at: at(-4).toISOString() }));
  const r3 = poll({}, { snapshot: { ...HEALTHY, threadCheckFails: fixed } });
  check('10n a later good read resolves it: nothing failing', failingIds(r3.results), []);
  const r4 = poll(r2.state, { snapshot: { ...HEALTHY, threadCheckFails: fixed }, now: at(30) });
  check('10o recovery posts once it is fixed', r4.recoveries.map((x) => x.id), ['thread_check_account']);
  check('10p recovery wording does not overclaim', /nobody has tried in 24 hours/.test(kai.formatKaiRecovery(r4.recoveries[0])), true);

  // A good read BEFORE the failure does not hide it.
  const stale = four.map((r) => ({ ...r, last_ok_at: at(-60).toISOString() }));
  check('10q an older good read does not resolve it', failingIds(poll({}, { snapshot: { ...HEALTHY, threadCheckFails: stale } }).results), ['thread_check_account']);

  // Our own key problem goes to its own check.
  const keyRow = { workspace_id: 'ws-x', client_name: 'Mind Lift', created_at: at(-5).toISOString(), error_class: '', error: 'conversation_http_401: {"message":"Invalid API key"}', last_ok_at: null };
  const rk = poll({}, { snapshot: { ...HEALTHY, threadCheckFails: [keyRow] } });
  check('10r rejected key is ours, not the client\'s', failingIds(rk.results), ['thread_check_key']);

  // Blind poll skips all three thread checks.
  const blind = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: null, snapshotError: 'x', now: T0 });
  check('10s blind poll skips the thread checks', ['thread_check', 'thread_check_account', 'thread_check_key'].every((id) => blind.find((b) => b.id === id)?.skip), true);
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
  {
    const asked = [];
    const pgd = { query: async (sql) => { asked.push(sql); return { rows: [{ n: 0 }] }; } };
    const tue = await kai.readKaiDigest(pgd, new Date('2026-11-10T14:00:00Z'));
    check('ap9 approvals read only on Mondays (Tuesday: not read)', [asked.includes(kai.DIGEST_SQL.approvals), tue.approvals], [false, null]);
    asked.length = 0;
    const mon = await kai.readKaiDigest(pgd, new Date('2026-11-09T14:00:00Z'));
    check('ap10 Monday: approvals read', asked.includes(kai.DIGEST_SQL.approvals) && Array.isArray(mon.approvals), true);
  }
  const seen = [];
  const pg = { query: async (sql, params) => { seen.push({ sql, params }); return { rows: [{ n: 0, sent: 0, skipped: 0 }] }; } };
  await kai.readKaiSnapshot(pg, T0);
  check('15a seven queries', seen.length, 7);
  check('15b every query gets the poll time', seen.every((q) => q.params[0] === T0.toISOString()), true);
  check('15c Prosp quiet window passed as hours', seen.find((q) => q.sql === kai.SQL.prospEvents).params[1], kai.PROSP_QUIET_HOURS);
  check('15c2 lost-reply lookback passed as hours', seen.find((q) => q.sql === kai.SQL.missedReplies).params[1], kai.KAI_INTAKE_LOOKBACK_HOURS);
  check('15d read-only: no writes in any SQL', [...Object.values(kai.SQL), ...Object.values(kai.DIGEST_SQL)].some((s) => /\b(insert|update|delete|drop|alter)\b/i.test(s)), false);

  // A prospect's no closed by Kai is the normal path: not a skip, not a missing Slack post.
check('a reply put back by hand after an outage is not a lost reply', [/l\.metadata->>'recovered' IS NULL/.test(kai.SQL.missedReplies), /l\.event_data->'eventData'->>'recoveredFrom' IS NULL/.test(kai.SQL.missedReplies)], [true, true]);
check('decline closes are not counted as skipped', /status = 'skipped' AND COALESCE\(status_reason, ''\) NOT IN \('prospect_declined'/.test(kai.DIGEST_SQL.daily), true);
check('a sent decline close is not a divergence', /status_reason, ''\) <> 'decline_close'/.test(kai.DIGEST_SQL.divergence), true);
check('an auto-reply waiting for its send time is not a divergence', /q\.send_after IS NOT NULL/.test(kai.DIGEST_SQL.divergence), true);
check('an auto-reply waiting for its send time is not waiting for review', /status = 'drafted' AND NOT \(COALESCE\(kind, 'reply'\) = 'reply' AND send_after IS NOT NULL\)/.test(kai.DIGEST_SQL.daily), true);
check('an auto-reply waiting for its send time is not backlog', /send_after IS NOT NULL/.test(kai.DIGEST_SQL.backlog), true);
check('a stand-down during the wait is not auto-send holding back', /detail->>'scheduled'/.test(kai.SQL.autoSend24h), true);

// ── Thread watch liveness (ng-automation lib/thread-watch.js via /health)
{
  const H = 3600 * 1000;
  const tw = (over = {}) => ({ mode: 'on', bootedAt: new Date(T0.getTime() - 10 * H).toISOString(), lastOkAt: new Date(T0.getTime() - 0.5 * H).toISOString(), lastError: null, lastSummary: { candidates: 271, read: 231, unreadable: 23 }, ...over });
  const ids = (r) => r.filter((x) => x.failing).map((x) => x.id);
  check('tw1 recent run, 9% unreadable: green', ids(kai.evaluateThreadWatch(tw(), T0)), []);
  check('tw2 mode off: skipped', kai.evaluateThreadWatch(tw({ mode: 'off' }), T0).every((x) => x.skip), true);
  check('tw3 older Kai with no threadWatch: skipped', kai.evaluateThreadWatch(undefined, T0).every((x) => x.skip), true);
  const stale = kai.evaluateThreadWatch(tw({ lastOkAt: new Date(T0.getTime() - 3 * H).toISOString(), lastError: 'db down' }), T0);
  check('tw4 no finished run in 3 h: failing', ids(stale), ['thread_watch']);
  check('tw4b reason names the last error', /db down/.test(stale[0].reason), true);
  check('tw5 just booted, no run yet: green', ids(kai.evaluateThreadWatch(tw({ bootedAt: new Date(T0.getTime() - 20 * 60 * 1000).toISOString(), lastOkAt: null }), T0)), []);
  check('tw6 booted 3 h ago, never finished: failing', ids(kai.evaluateThreadWatch(tw({ bootedAt: new Date(T0.getTime() - 3 * H).toISOString(), lastOkAt: null }), T0)), ['thread_watch']);
  check('tw7 over 30% unreadable: failing', ids(kai.evaluateThreadWatch(tw({ lastSummary: { candidates: 100, read: 60, unreadable: 40 } }), T0)), ['thread_watch_reads']);
  check('tw8 a tiny run never trips the read alarm', ids(kai.evaluateThreadWatch(tw({ lastSummary: { candidates: 5, read: 2, unreadable: 3 } }), T0)), []);
  const down = kai.evaluateKaiChecks({ health: { ok: false, error: 'x' }, snapshot: HEALTHY, now: T0 });
  check('tw9 Kai down: thread watch skipped, health alert carries it', down.filter((r) => r.id.startsWith('thread_watch')).every((r) => r.skip), true);
  const wired = kai.evaluateKaiChecks({ health: { ok: true, threadWatch: tw({ lastOkAt: new Date(T0.getTime() - 3 * H).toISOString() }) }, snapshot: HEALTHY, now: T0 });
  check('tw10 wired into evaluateKaiChecks', failingIds(wired), ['thread_watch']);
  let st = {};
  const p1 = kai.stepKaiWatch(st, wired, T0); st = p1.state;
  const p2 = kai.stepKaiWatch(st, wired, at(15));
  check('tw11 alerts on the second stale poll, not the first', [p1.alerts.length, p2.alerts.length, p2.alerts[0] && p2.alerts[0].title], [0, 1, 'Later-messages reader stopped']);
  // Firewall-cut runs (Kai /health blockedStopped + blockedRunsInARow, ng-automation 2026-11-10)
  const cutRun = { candidates: 253, read: 77, unreadable: 176, blockedStopped: 176 };
  check('tw12 one firewall-cut run: no read alarm, no block alarm', ids(kai.evaluateThreadWatch(tw({ lastSummary: cutRun, blockedRunsInARow: 1 }), T0)), []);
  check('tw13 cut run with real client failures still trips the read alarm', ids(kai.evaluateThreadWatch(tw({ lastSummary: { candidates: 253, read: 50, unreadable: 103, blockedStopped: 70 }, blockedRunsInARow: 1 }), T0)), ['thread_watch_reads']);
  const streak3 = kai.evaluateThreadWatch(tw({ lastSummary: cutRun, blockedRunsInARow: 3 }), T0);
  check('tw14 three cut runs in a row: block alarm only', ids(streak3), ['thread_watch_blocked']);
  check('tw15 block alarm names the streak and the static IP lever', /3 hourly runs in a row/.test(streak3.find((r) => r.id === 'thread_watch_blocked').reason) && /static outbound IP/.test(streak3.find((r) => r.id === 'thread_watch_blocked').reason), true);
  check('tw16 older Kai without the new fields: unchanged behaviour', ids(kai.evaluateThreadWatch(tw({ lastSummary: { candidates: 100, read: 60, unreadable: 40 } }), T0)), ['thread_watch_reads']);
  check('tw17 Kai down: block check skipped too', kai.evaluateKaiChecks({ health: { ok: false, error: 'x' }, snapshot: HEALTHY, now: T0 }).some((r) => r.id === 'thread_watch_blocked' && r.skip), true);
}

check('a pleasantry Kai closed is not counted as held back', /NOT IN \('prospect_declined', 'pleasantry'\)/.test(kai.DIGEST_SQL.daily), true);

// ── Weekly client approvals (Mondays): who leaves Kai drafts waiting
{
  const rows = [
    { client_name: 'Flor Lizano Bolaños', drafts: 116, waiting: 38, oldest_hours: 330, approved: 0, median_approve_hours: null, expired: 69 },
    { client_name: 'Multiproyectos', drafts: 46, waiting: 12, oldest_hours: 330, approved: 29, median_approve_hours: 75.7, expired: 5 },
    { client_name: 'Mind Lift', drafts: 50, waiting: 1, oldest_hours: 3, approved: 39, median_approve_hours: 1.8, expired: 0 },
    { client_name: 'Buena Vista', drafts: 13, waiting: 1, oldest_hours: 30, approved: 0, median_approve_hours: null, expired: 0 },
  ];
  const slow = kai.slowApprovers(rows);
  check('ap1 flags drafts waiting over 2 days, slow approvals and expired drafts; worst first', slow.map((r) => r.client), ['FLOR LIZANO BOLAÑOS', 'MULTIPROYECTOS']);
  check('ap2 a fast approver and a single waiting draft are not flagged', slow.some((r) => /MIND LIFT|BUENA VISTA/.test(r.client)), false);
  check('ap3 line for a client that never approves', kai.approvalLine(slow[0]), '• FLOR LIZANO BOLAÑOS: 38 waiting, oldest 14 d · 0 of 116 approved · 69 expired unsent');
  check('ap4 line for a slow approver', kai.approvalLine(slow[1]), '• MULTIPROYECTOS: 12 waiting, oldest 14 d · 29 of 46 approved, usually after 3 d · 5 expired unsent');

  const base = digestData({ yesterday: { received: 28 }, baseline: () => ({ received: 28 }) });
  const daily = kai.formatKaiDigest(base);
  check('ap5 no approvals section on other days', daily.includes('CLIENT APPROVALS'), false);
  const monday = kai.formatKaiDigest({ ...base, approvals: rows });
  check('ap6 Monday: section, fulfillment hint and an issue', monday.includes('CLIENT APPROVALS (WEEKLY, LAST 30 DAYS)') && monday.includes('→ Fulfillment') && monday.includes('2 clients slow to approve'), true);
  const clean = kai.formatKaiDigest({ ...base, approvals: [rows[2]] });
  check('ap7 Monday with nobody slow: one green line, no issue', clean.includes('No client is leaving Kai drafts waiting') && clean.includes('✅ ALL GREEN'), true);
  check('ap8 approvals SQL is read-only and leaves out Kai closes and waiting auto-replies',
    !/\b(insert|update|delete)\b/i.test(kai.DIGEST_SQL.approvals) && /'pleasantry'/.test(kai.DIGEST_SQL.approvals) && /send_after IS NULL/.test(kai.DIGEST_SQL.approvals), true);
}

// ── Prosp webhook watch (2026-11-12): Kai hold, dash spool, per-workspace silence
{
  const ago = (min) => new Date(T0.getTime() - min * MIN).toISOString();
  const ids = (r) => (r.failing ? [r.id] : []);

  // Kai hold
  check('wh1 older Kai with no webhookHold: skipped', kai.evaluateKaiHold(undefined, T0).skip, true);
  check('wh2 nothing held: fine', ids(kai.evaluateKaiHold({ held: 0, oldestHeldAt: null }, T0)), []);
  check('wh3 held 10 minutes: fine (the retry usually clears it)', ids(kai.evaluateKaiHold({ held: 2, oldestHeldAt: ago(10) }, T0)), []);
  const hold = kai.evaluateKaiHold({ held: 2, oldestHeldAt: ago(45) }, T0);
  check('wh4 held 45 minutes: failing', ids(hold), ['kai_hold']);
  check('wh5 the text warns about a restart', /2 replies in memory/.test(hold.reason) && /lost if Kai restarts/.test(hold.reason), true);
  const holdWired = kai.evaluateKaiChecks({ health: { ok: true, webhookHold: { held: 1, oldestHeldAt: ago(60) } }, snapshot: HEALTHY, now: T0 });
  check('wh6 wired into evaluateKaiChecks', failingIds(holdWired), ['kai_hold']);
  const holdDown = kai.evaluateKaiChecks({ health: { ok: false, error: 'HTTP 502' }, snapshot: HEALTHY, now: T0 });
  check('wh7 Kai down: hold skipped, health alert carries it', holdDown.find((r) => r.id === 'kai_hold').skip, true);

  // Dash spool
  check('sp1 empty spool: fine', ids(kai.evaluateSpool([], T0)), []);
  check('sp2 an event parked 10 minutes ago: fine', ids(kai.evaluateSpool([{ received_at: ago(10) }], T0)), []);
  const spoolLate = kai.evaluateSpool([{ received_at: ago(50) }, { received_at: ago(40) }, { received_at: ago(5) }], T0);
  check('sp3 events waiting over 30 minutes: failing', ids(spoolLate), ['spool_waiting']);
  check('sp4 counts only the late ones and names the replay job', /^2 Prosp events have been waiting/.test(spoolLate.reason) && /prosp-spool-drain/.test(spoolLate.reason), true);
  const spoolWired = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: HEALTHY, now: T0, spool: { rows: [{ received_at: ago(90) }] } });
  check('sp5 wired into evaluateKaiChecks', failingIds(spoolWired), ['spool_waiting']);
  const spoolBlind = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: null, snapshotError: 'timeout', now: T0, spool: { rows: [{ received_at: ago(90) }] } });
  check('sp6 still judged while the portal is unreadable', failingIds(spoolBlind), ['spool_waiting', 'watcher_read']);
  const spoolUnread = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: HEALTHY, now: T0, spool: null });
  check('sp7 spool unreadable: skipped, nothing else blinded', spoolUnread.find((r) => r.id === 'spool_waiting').skip && failingIds(spoolUnread).length === 0, true);
  check('sp8 without a spool argument (older caller): skipped', kai.evaluateKaiChecks({ health: { ok: true }, snapshot: HEALTHY, now: T0 }).find((r) => r.id === 'spool_waiting').skip, true);

  // Spool notice
  check('sn1 one fresh event: no notice', kai.spoolNotice(false, [{ received_at: ago(2) }], T0), { holding: false, message: null });
  const n5 = kai.spoolNotice(false, Array.from({ length: 5 }, () => ({ received_at: ago(1) })), T0);
  check('sn2 five events: one notice, no mention', n5.holding && /5 held so far, nothing lost/.test(n5.message) && !/<@/.test(n5.message), true);
  check('sn3 one event older than 10 minutes: notice', kai.spoolNotice(false, [{ received_at: ago(12) }], T0).holding, true);
  check('sn4 still holding: silent', kai.spoolNotice(true, [{ received_at: ago(20) }], T0), { holding: true, message: null });
  const nEnd = kai.spoolNotice(true, [], T0);
  check('sn5 spool empty again: one recovery line', !nEnd.holding && /storing Prosp events again/.test(nEnd.message), true);

  // prosp_quiet names the fix
  const quietAll = kai.evaluateKaiChecks({ health: { ok: true }, snapshot: { ...HEALTHY, prospEvents: false }, now: T0 });
  check('pq1 the global check names the fix', /Settings, Webhooks/.test(quietAll.find((r) => r.id === 'prosp_quiet').reason), true);

  // Per-workspace silence. Made-up workspace and campaign ids.
  check('wq1 mode defaults to digest', [kai.workspaceQuietMode(undefined), kai.workspaceQuietMode('alert'), kai.workspaceQuietMode('OFF'), kai.workspaceQuietMode('nonsense')], ['digest', 'alert', 'off', 'digest']);
  let monday = new Date(Date.UTC(2026, 8, 21, 14, 37));
  while (monday.getUTCDay() !== 1) monday = new Date(monday.getTime() + 24 * 60 * MIN);
  const wins = kai.baselineWindows(monday);
  check('wq2 five baseline windows, none on a weekend', [wins.length, wins.every((w) => ![0, 6].includes(w.to.getUTCDay()))], [5, true]);
  check('wq3 Monday looks back to Friday, on the clock hour, 3 hours wide',
    [(kai.utcHourStart(monday).getTime() - wins[0].to.getTime()) / (60 * MIN), wins[0].to.getUTCMinutes(), (wins[0].to - wins[0].from) / (60 * MIN)], [72, 0, 3]);

  const WS = 'ws-quiet-1';
  const day = (n, ws = WS, campaigns = ['camp-1']) => [{ workspace_id: ws, n, campaigns }];
  const regular = [day(40), day(35), day(50), day(0), day(22)];
  check('wq4 regular (4 of 5 days) and quiet: candidate', kai.quietCandidates({ current: [], baseline: regular }).map((c) => [c.workspaceId, c.activeDays, c.usual]), [[WS, 4, 35]]);
  check('wq5 3 of 5 days is not regular', kai.quietCandidates({ current: [], baseline: [day(40), day(35), day(50), day(0), day(3)] }), []);
  check('wq6 still sending: not a candidate', kai.quietCandidates({ current: [{ workspace_id: WS, recent: 4, earlier: 30 }], baseline: regular }), []);

  const W0 = new Date(monday.getTime());
  const wat = (m) => new Date(W0.getTime() + m * MIN);
  const fresh = (t) => new Date(t.getTime() - 20 * MIN).toISOString();
  const stopped = (t, extra = {}) => ({
    current: [{ workspace_id: WS, recent: 0, earlier: 60, last_at: new Date(t.getTime() - 200 * MIN).toISOString() }],
    baseline: regular,
    telemetry: [{ prosp_campaign_id: 'camp-1', is_active: true, status_checked_at: fresh(t) }],
    telemetryNewest: fresh(t),
    names: [{ workspace_id: WS, is_shared: false, clients: ['Acme Example'], only_demo: false }],
    ...extra,
  });
  let q = kai.stepWorkspaceQuiet({}, stopped(W0), W0);
  check('wq7 first quiet poll: nothing yet', [q.started.length, q.state[WS].open], [0, false]);
  q = kai.stepWorkspaceQuiet(q.state, stopped(wat(15)), wat(15));
  check('wq8 second poll, was sending earlier today, Prosp says active: flagged', q.started.map((e) => [e.workspaceId, e.tier, e.campaign, e.label]), [[WS, 'stopped', 'active', 'ACME EXAMPLE']]);
  const alert = kai.formatWorkspaceQuietAlert(q.started[0]);
  check('wq9 the alert carries the link and the fix',
    alert.id === `prosp_workspace_quiet:${WS}` && alert.reason.includes(`https://prosp.ai/settings?workspace=${WS}&tab=Webhooks`)
      && /switch every Error row back on \(Test Webhook, then Update\)/.test(alert.reason) && /real click/.test(alert.reason) && /events of the gap are gone/.test(alert.reason), true);
  check('wq10 no em dash in the alert', alert.reason.includes('—'), false);
  let q2 = kai.stepWorkspaceQuiet(q.state, stopped(wat(30)), wat(30));
  q2 = kai.stepWorkspaceQuiet(q2.state, stopped(wat(60 * 8)), wat(60 * 8));
  check('wq11 one alert per silence, however long', [q2.started.length, q2.ended.length, q2.state[WS].open], [0, 0, true]);
  const night = kai.stepWorkspaceQuiet(q2.state, { current: [], baseline: [day(0), day(0), day(0), day(0), day(0)], telemetry: [], telemetryNewest: null, names: [] }, wat(60 * 10));
  check('wq12 stays open outside its usual hours', [night.ended.length, night.state[WS].open], [0, true]);
  const back = kai.stepWorkspaceQuiet(night.state, { ...stopped(wat(60 * 11)), current: [{ workspace_id: WS, recent: 3, earlier: 0 }] }, wat(60 * 11));
  check('wq13 sends again: one recovery, state cleared', [back.ended.map((e) => e.workspaceId), Object.keys(back.state)], [[WS], []]);
  check('wq14 recovery line', /ACME EXAMPLE is sending Prosp events again/.test(kai.formatWorkspaceQuietRecovery(back.ended[0])), true);

  const paused = (t) => stopped(t, { telemetry: [{ prosp_campaign_id: 'camp-1', is_active: false, status_checked_at: fresh(t) }] });
  let p = kai.stepWorkspaceQuiet({}, paused(W0), W0);
  p = kai.stepWorkspaceQuiet(p.state, paused(wat(15)), wat(15));
  check('wq15 Prosp says the campaign is off: not flagged', [p.started.length, p.state[WS].open], [0, false]);
  const staleTel = (t) => stopped(t, { telemetry: [{ prosp_campaign_id: 'camp-1', is_active: true, status_checked_at: new Date(t.getTime() - 300 * MIN).toISOString() }], telemetryNewest: new Date(t.getTime() - 300 * MIN).toISOString() });
  let st = kai.stepWorkspaceQuiet({}, staleTel(W0), W0);
  st = kai.stepWorkspaceQuiet(st.state, staleTel(wat(15)), wat(15));
  check('wq16 telemetry stale everywhere: flagged, and the text says it could not check', [st.started.map((e) => e.campaign), /could not check whether its campaigns are still running/.test(kai.formatWorkspaceQuietAlert(st.started[0]).reason)], [['unknown'], true]);

  const late = (t) => stopped(t, { current: [] });
  let l = kai.stepWorkspaceQuiet({}, late(W0), W0);
  l = kai.stepWorkspaceQuiet(l.state, late(wat(15)), wat(15));
  l = kai.stepWorkspaceQuiet(l.state, late(wat(60)), wat(60));
  check('wq17 nothing earlier today: a late start waits', [l.started.length, l.state[WS].hours], [0, 2]);
  l = kai.stepWorkspaceQuiet(l.state, late(wat(120)), wat(120));
  check('wq18 quiet three clock hours in a row: flagged as not started', l.started.map((e) => [e.tier, e.since]), [['not_started', null]]);
  check('wq19 its text gives a duration, not a time', /for over 9 hours/.test(kai.formatWorkspaceQuietAlert(l.started[0]).reason), true);
  let gap = kai.stepWorkspaceQuiet({}, late(W0), W0);
  gap = kai.stepWorkspaceQuiet(gap.state, late(wat(120)), wat(120));
  check('wq20 a skipped hour restarts the count', gap.state[WS].hours, 1);

  let saturday = new Date(W0.getTime());
  while (saturday.getUTCDay() !== 6) saturday = new Date(saturday.getTime() + 24 * 60 * MIN);
  check('wq21 weekend: not judged', kai.stepWorkspaceQuiet({}, stopped(saturday), saturday).skipped, true);
  const parked = kai.stepWorkspaceQuiet({}, stopped(W0), W0, { spoolPending: 3 });
  check('wq22 events parked in the spool: not judged', [parked.skipped, Object.keys(parked.state).length], [true, 0]);
  let demo = kai.stepWorkspaceQuiet({}, stopped(W0, { names: [{ workspace_id: WS, is_shared: false, clients: null, only_demo: true }] }), W0);
  demo = kai.stepWorkspaceQuiet(demo.state, stopped(wat(15), { names: [{ workspace_id: WS, is_shared: false, clients: null, only_demo: true }] }), wat(15));
  check('wq23 the demo workspace is never flagged', demo.started.length, 0);
  let shared = kai.stepWorkspaceQuiet({}, stopped(W0, { names: [{ workspace_id: WS, is_shared: true, clients: ['Acme Example'], only_demo: false }] }), W0);
  shared = kai.stepWorkspaceQuiet(shared.state, stopped(wat(15), { names: [{ workspace_id: WS, is_shared: true, clients: ['Acme Example'], only_demo: false }] }), wat(15));
  check('wq24 a shared workspace is named as the agency one', shared.started[0].label, 'The shared agency workspace');
  let unmapped = kai.stepWorkspaceQuiet({}, stopped(W0, { names: [] }), W0);
  unmapped = kai.stepWorkspaceQuiet(unmapped.state, stopped(wat(15), { names: [] }), wat(15));
  check('wq25 no client on record: named by its id', unmapped.started[0].label, `Prosp workspace ${WS}`);

  // Digest section
  check('wq26 off: no section', kai.workspaceQuietDigestLines({ mode: 'off', open: [q.started[0]] }), []);
  const dl = kai.workspaceQuietDigestLines({ mode: 'digest', open: [q.started[0]], recent: [back.ended[0]] });
  check('wq27 digest mode: trial header, the open one with its link, the recovered one', [dl[1], dl.length, dl[2].includes('tab=Webhooks'), /sending again at/.test(dl[3])], ['`PROSP WEBHOOKS (TRIAL, NO ALERTS YET)`', 4, true, true]);
  check('wq28 nothing quiet: one green line', kai.workspaceQuietDigestLines({ mode: 'alert' }).slice(1), ['`PROSP WEBHOOKS`', '• No workspace went quiet in its usual hours. ✅']);
  check('wq29 the workspace reads are short created_at ranges, imports left out',
    [kai.SQL.workspaceNow, kai.SQL.workspaceBaseline].every((s) => /created_at > \$1::timestamptz AND created_at <= \$[23]::timestamptz/.test(s) && /NOT IN \('Contact Added', 'LinkedIn Contact Extracted'\)/.test(s) && !/\b(insert|update|delete)\b/i.test(s)), true);
  check('wq30 client names leave the demo out', /is_demo/.test(kai.SQL.workspaceNames), true);

  const steady = (i) => ({ received: 25 + (i % 5) * 4, escalated: 2 + (i % 3) });
  const dd = digestData({ yesterday: { received: 28, escalated: 3, sent: 4, drafted: 16 }, baseline: steady });
  const withQuiet = kai.formatKaiDigest(dd, [], { workspaceQuiet: { mode: 'digest', open: [q.started[0]], recent: [] } });
  check('wq31 a quiet workspace is an issue in the daily digest', withQuiet.includes('1 Prosp workspace gone quiet') && withQuiet.includes('`PROSP WEBHOOKS (TRIAL, NO ALERTS YET)`') && withQuiet.includes('ACME EXAMPLE: no Prosp event since'), true);
  const noQuiet = kai.formatKaiDigest(dd, [], { workspaceQuiet: { mode: 'digest', open: [], recent: [] } });
  check('wq32 none quiet: still ALL GREEN, with the section', noQuiet.includes('✅ ALL GREEN') && noQuiet.includes('No workspace went quiet in its usual hours'), true);
  check('wq33 digest without the extras is unchanged (no section)', kai.formatKaiDigest(dd).includes('PROSP WEBHOOKS'), false);
  check('wq34 no em dash in the digest section', withQuiet.includes('\u2014'), false);
}

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall kai health watch checks passed');
})();
