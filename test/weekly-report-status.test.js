// Rules test for the weekly performance report status post.  Run:  node test/weekly-report-status.test.js
//
// lib/weeklyReportStatus.js is pure (no Slack, no network), so it is required
// directly. The feeds below are the shape of GET /api/ops/weekly-report-status
// (dash PR 4, 2026-09-25).
const wpr = require('../lib/weeklyReportStatus');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const NOW = new Date('2026-09-28T17:05:00Z'); // Mon 11:05 CR, 5 minutes after the feed
const clone = (o) => JSON.parse(JSON.stringify(o));

const GREEN = {
  contractVersion: 1,
  generatedAt: '2026-09-28T17:00:00.000Z',
  isoWeek: '2026-W39',
  mode: 'live',
  lastRunAt: '2026-09-28T13:02:00.000Z',
  eligibleRecipients: 77,
  sent: 71,
  dryRun: 0,
  retryQueued: 0,
  inFlight: 0,
  missing: [],
  failures: [],
  verdict: 'green',
  line: '✅ WPR 2026-W39 (live): 71 sent, 6 skipped (5 no events, 1 suppressed, 0 no address), 0 failed, 0 missing',
};
const RED = {
  ...clone(GREEN),
  sent: 68,
  retryQueued: 1,
  missing: [{ customerId: 'c2', clientName: 'Two Co - Ana', recipient: 'ana@two.test' }],
  failures: [
    { customerId: 'c3', clientName: 'Three | Bo', recipient: 'bo@three.test', outcome: 'send_failed', attempt: 3, error: '503 from resend' },
    { customerId: 'c4', clientName: 'Four', recipient: 'cy@four.test', outcome: 'retry', attempt: 2, error: 'statement timeout' },
  ],
  verdict: 'red',
  line: '⚠️ WPR 2026-W39 (live): 68 sent · 1 recipient missing · 1 failed · 1 retrying · 0 in flight',
};
const OFF = { contractVersion: 1, generatedAt: '2026-09-28T17:00:00.000Z', isoWeek: '2026-W39', mode: 'off', line: 'WPR 2026-W39: the weekly report is off' };

// ── 1. Feed checks (fail closed)
check('1a  green feed is usable', wpr.checkFeed(GREEN, NOW).ok, true);
check('1b  red feed is usable', wpr.checkFeed(RED, NOW).ok, true);
check('1c  off feed is usable', wpr.checkFeed(OFF, NOW).ok, true);
check('1d  stale feed is refused', wpr.checkFeed(GREEN, new Date('2026-09-28T17:30:00Z')).ok, false);
check('1e  unknown contract version is refused', wpr.checkFeed({ ...GREEN, contractVersion: 2 }, NOW).ok, false);
check('1f  a verdict that disagrees with its line is refused', wpr.checkFeed({ ...GREEN, verdict: 'red' }, NOW).ok, false);
check('1g  a malformed line is refused', wpr.checkFeed({ ...GREEN, line: 'all good' }, NOW).ok, false);
const noCount = clone(GREEN); delete noCount.sent;
check('1h  a missing count is refused', wpr.checkFeed(noCount, NOW).ok, false);
const badMissing = clone(RED); badMissing.missing[0].recipient = '';
check('1i  an incomplete missing entry is refused', wpr.checkFeed(badMissing, NOW).ok, false);
check('1j  garbage is refused', wpr.checkFeed(null, NOW).ok, false);

// ── 2. The post
check('2a  off posts nothing', wpr.formatPost(OFF), null);
check('2b  green is the line alone', wpr.formatPost(GREEN), GREEN.line);
const red = wpr.formatPost(RED);
check('2c  red lists who is missing and who failed, with short names', red.split('\n'), [
  RED.line,
  '• missing: Two Co <ana@two.test>',
  '• send_failed (attempt 3): Three <bo@three.test> · 503 from resend',
  '• retry (attempt 2): Four <cy@four.test> · statement timeout',
]);
const many = clone(RED);
many.missing = Array.from({ length: 9 }, (_, i) => ({ customerId: `m${i}`, clientName: `Client ${i}`, recipient: `p${i}@x.test` }));
check('2d  detail lines are capped with a count of the rest', wpr.formatPost(many).split('\n').length, 1 + 6 + 1);
check('2e  never ran adds the cron hint', wpr.formatPost({ ...RED, lastRunAt: null }).split('\n').pop(), 'The cron never claimed anything this week: check vercel.json and the Vercel cron logs.');

// ── 3. Criteria on the text
check('3a  green post passes', wpr.validatePost(GREEN.line).ok, true);
check('3b  red post passes', wpr.validatePost(red).ok, true);
check('3c  a post without a verdict line fails', wpr.validatePost('WPR went fine').ok, false);
check('3d  a banned token fails', wpr.validatePost(`${GREEN.line}\n• missing: undefined <x>`).problems, ['contains "undefined"']);
check('3e  a placeholder fails', wpr.validatePost(`${GREEN.line}\n{count} more`).ok, false);

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
