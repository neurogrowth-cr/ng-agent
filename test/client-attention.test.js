// Rules test for the client attention report and alerts.  Run:  node test/client-attention.test.js
//
// lib/clientAttention.js is pure (no Slack, no network), so it is required
// directly. The fixture is a real snapshot of the dash feed
// (GET /api/ops/client-attention) taken on 2026-09-24: 21 urgent, 18 watch,
// 10 clients in band.
const ca = require('../lib/clientAttention');
const FEED = require('./fixtures/client-attention-feed.json');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const NOW = new Date('2026-09-24T14:35:00Z'); // Thu 08:35 CR, 5 minutes after the feed
const clone = (o) => JSON.parse(JSON.stringify(o));

// ── 1. Feed checks (fail closed)
check('1a  real feed is usable', ca.checkFeed(FEED, NOW).ok, true);
check('1b  stale feed is refused', ca.checkFeed(FEED, new Date('2026-09-24T15:00:00Z')).ok, false);
check('1c  unknown contract version is refused', ca.checkFeed({ ...FEED, contractVersion: 2 }, NOW).ok, false);
const noReason = clone(FEED); noReason.items[3].reason = '';
check('1d  an incomplete item is refused', ca.checkFeed(noReason, NOW).ok, false);
const newline = clone(FEED); newline.items[0].reason = 'two\nlines';
check('1e  a line break in an item is refused', ca.checkFeed(newline, NOW).ok, false);

// ── 2. The report on the real feed
const report = ca.formatReport(FEED, NOW);
const lines = report.split('\n');
console.log('\n----- report preview -----\n' + report + '\n--------------------------\n');
check('2a  report passes its own criteria', ca.validateReport(report, FEED), { ok: true, problems: [] });
check('2b  header names the CR date', lines[0], '*Client attention · Thu, Sep 24*');
const positives = FEED.items.filter((i) => i.code === 'positive_waiting');
check('2c  positive replies fold into one line', lines.filter((l) => l.startsWith('🔴 Positive replies waiting')).length, 1);
check('2d  that line counts every positive client', new RegExp(`: ${positives.length} clients, `).test(report), true);
const itemLines = lines.filter((l) => /^(🔴|🟠) /.test(l));
check('2e  at most 15 item lines', itemLines.length <= ca.MAX_ITEM_LINES, true);
check('2f  stopped sending is listed, not folded', report.includes('Sending') || /No connection requests or messages sent/.test(report), true);
check('2g  in-band line matches the feed', lines.includes(`✅ ${FEED.inBand} clients in band.`), true);
check('2h  ends with the admin link', lines[lines.length - 1], `Mark items handled or snoozed: <${FEED.adminUrl}|Campaign health>`);

// ── 3. Criteria can fail (a check that cannot go red is not a check)
const dropped = lines.filter((l) => !/^🟠 /.test(l) || l !== itemLines.find((x) => x.startsWith('🟠 '))).join('\n');
check('3a  a missing item line is caught', ca.validateReport(dropped, FEED).ok, false);
check('3b  a wrong in-band count is caught', ca.validateReport(report.replace(`✅ ${FEED.inBand} `, '✅ 99 '), FEED).ok, false);
check('3c  "undefined" is caught', ca.validateReport(report.replace('→', 'undefined →'), FEED).ok, false);
check('3d  a missing admin link is caught', ca.validateReport(report.replace(FEED.adminUrl, 'https://example.com'), FEED).ok, false);

// ── 4. Empty and small feeds
const empty = { ...FEED, items: [], inBand: 12 };
const emptyReport = ca.formatReport(empty, NOW);
check('4a  empty feed says so', emptyReport.includes('Nothing needs attention today.'), true);
check('4b  empty report passes', ca.validateReport(emptyReport, empty).ok, true);
const one = { ...FEED, items: [FEED.items.find((i) => i.code !== 'positive_waiting')], inBand: 1 };
const oneReport = ca.formatReport(one, NOW);
check('4c  one item, no "more" line', /…and/.test(oneReport), false);
check('4d  singular wording', oneReport.includes('✅ 1 client in band.'), true);
check('4e  small report passes', ca.validateReport(oneReport, one).ok, true);

// ── 5. Urgent alerts
const urgent = FEED.items.filter((i) => i.level === 'urgent');
const first = ca.planAlerts(FEED, new Set());
check('5a  capped per run', first.send.length, ca.MAX_ALERTS_PER_RUN);
check('5b  overflow counted', first.overflow, urgent.length - ca.MAX_ALERTS_PER_RUN);
const seen = new Set(urgent.map(ca.alertKey));
check('5c  nothing re-alerts once seen', ca.planAlerts(FEED, seen).send.length, 0);
const changed = clone(FEED); changed.items[0].fingerprint = 'new-evidence';
check('5d  new evidence alerts again', ca.planAlerts(changed, seen).send.map((i) => i.clientId), [changed.items[0].clientId]);
check('5e  watch items never alert', ca.planAlerts({ ...FEED, items: FEED.items.filter((i) => i.level === 'watch') }, new Set()).send.length, 0);
const alert = ca.formatAlert(urgent[0], FEED.adminUrl);
check('5f  alert passes its criteria', ca.validateAlert(alert), { ok: true, problems: [] });
check('5g  a broken alert is caught', ca.validateAlert(alert.replace(' <https', ' <http')).ok, false);
check('5h  overflow line', ca.formatAlertOverflow(3, FEED.adminUrl), `🔴 *Client attention* · 3 more urgent items this run: <${FEED.adminUrl}|see the admin page>`);

// ── 6. Names
check('6a  short name before " - "', ca.shortName('Mind Lift Leadership - Laura Morales'), 'Mind Lift Leadership');
check('6b  short name before " | "', ca.shortName('Ripple Effect | Mario Cardona '), 'Ripple Effect');

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall client attention checks passed');
