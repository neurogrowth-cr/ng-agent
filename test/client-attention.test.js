// Rules test for the client attention report and alerts.  Run:  node test/client-attention.test.js
//
// lib/clientAttention.js is pure (no Slack, no network), so it is required
// directly. Two fixtures: the v1 feed is a real snapshot of the dash feed
// taken on 2026-09-24 (21 urgent, 18 watch, 10 in band); the v2 feed is
// hand-written to the v2 contract (owner on every item, counts.adminOnly, one
// fleet item with members), covering the shapes the formatter must handle.
const ca = require('../lib/clientAttention');
const FEED_V1 = require('./fixtures/client-attention-feed.json');
const FEED = require('./fixtures/client-attention-feed-v2.json');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const NOW_V1 = new Date('2026-09-24T14:35:00Z');
const NOW = new Date('2026-10-08T14:35:00Z'); // Thu 08:35 CR, 5 minutes after the v2 feed
const clone = (o) => JSON.parse(JSON.stringify(o));
const MENTION = '<!subteam^S0BH988QW1K|@fulfillment>';

// ── 1. Feed checks (fail closed). Both contract versions are accepted during
// the rollout; everything else is refused.
check('1a  v2 feed is usable', ca.checkFeed(FEED, NOW).ok, true);
check('1b  v1 feed is still usable', ca.checkFeed(FEED_V1, NOW_V1).ok, true);
check('1c  stale feed is refused', ca.checkFeed(FEED, new Date('2026-10-08T15:00:00Z')).ok, false);
check('1d  unknown contract version is refused', ca.checkFeed({ ...FEED, contractVersion: 3 }, NOW).ok, false);
const noReason = clone(FEED); noReason.items[3].reason = '';
check('1e  an incomplete item is refused', ca.checkFeed(noReason, NOW).ok, false);
const newline = clone(FEED); newline.items[1].reason = 'two\nlines';
check('1f  a line break in an item is refused', ca.checkFeed(newline, NOW).ok, false);
const noOwner = clone(FEED); delete noOwner.items[1].owner;
check('1g  v2 item without an owner is refused', ca.checkFeed(noOwner, NOW).ok, false);
const badOwner = clone(FEED); badOwner.items[1].owner = 'them';
check('1h  v2 item with an unknown owner is refused', ca.checkFeed(badOwner, NOW).ok, false);
const noAdminOnly = clone(FEED); delete noAdminOnly.counts.adminOnly;
check('1i  v2 feed without counts.adminOnly is refused', ca.checkFeed(noAdminOnly, NOW).ok, false);
const noMembers = clone(FEED); noMembers.items[0].members = [];
check('1j  fleet item without members is refused', ca.checkFeed(noMembers, NOW).ok, false);
const fleetV1 = clone(FEED_V1); fleetV1.items[0].clientId = 'fleet'; fleetV1.items[0].members = [{ clientId: 'x', fingerprint: 'y' }];
check('1k  fleet item in a v1 feed is refused', ca.checkFeed(fleetV1, NOW_V1).ok, false);

// ── 2. The v2 report: fleet first, one card per client, owner tags, drift line.
const report = ca.formatReport(FEED, NOW, { mention: MENTION });
const lines = report.split('\n');
console.log('\n----- v2 report preview -----\n' + report + '\n-----------------------------\n');
check('2a  report passes its own criteria', ca.validateReport(report, FEED, { mention: MENTION }), { ok: true, problems: [] });
check('2b  header names the CR date and mentions @fulfillment once', lines[0], `*Client attention · Thu, Oct 8* ${MENTION}`);
check('2c  mention appears exactly once', report.split(MENTION).length - 1, 1);
check('2d  fleet section comes before the clients', lines.indexOf('*Fleet*') < lines.indexOf('*Clients*'), true);
check('2e  fleet line names the count and the owner', lines[lines.indexOf('*Fleet*') + 1].startsWith('🔴 [ours] Fleet (4 clients): 4 clients stopped sending on 2026-10-06'), true);
const cards = lines.filter((l) => /^(🔴|🟠) \*.+\*$/.test(l));
check('2f  one card per client (4 clients, 5 client items)', cards.length, 4);
check('2g  urgent cards before watch cards, feed order kept', cards, ['🔴 *US Legal - Guido Soto*', '🔴 *Palantier AI - Carlos Jimenez*', '🔴 *Strategia - Andrés Morera*', '🟠 *Flor Lizano Bolaños*']);
const usLegal = lines.indexOf('🔴 *US Legal - Guido Soto*');
check('2h  both US Legal items sit under its card, each with its owner', [lines[usLegal + 1].startsWith('• [client] Prosp says'), lines[usLegal + 2].startsWith('• [ours] No LinkedIn account linked')], [true, true]);
check('2i  every item line carries an owner tag', lines.filter((l) => l.startsWith('• ')).every((l) => /^• \[(ours|client)\] /.test(l)), true);
check('2j  drift line equals counts.adminOnly', lines.includes('🟡 12 clients off benchmark, review on the admin page.'), true);
check('2k  in-band line equals the feed', lines.includes('✅ 10 clients in band.'), true);
check('2l  no "more clients" line when every client fits', lines.some((l) => l.startsWith('…and')), false);
check('2m  no mention when none is configured', ca.formatReport(FEED, NOW).includes('subteam'), false);
check('2n  report without a mention passes when none is configured', ca.validateReport(ca.formatReport(FEED, NOW), FEED).ok, true);

// ── 3. The v1 feed still renders and validates (no owner tags, no drift line).
const reportV1 = ca.formatReport(FEED_V1, NOW_V1);
check('3a  v1 report passes its own criteria', ca.validateReport(reportV1, FEED_V1), { ok: true, problems: [] });
check('3b  v1 item lines have no owner tag', reportV1.split('\n').some((l) => /^• \[/.test(l)), false);
check('3c  v1 report has no drift line', reportV1.includes('off benchmark'), false);
check('3d  v1 report caps the cards and counts the rest', /…and \d+ more clients \(\d+ items\) on the admin page\./.test(reportV1), true);

// ── 4. Card cap: 13 clients, 10 cards, the rest counted with their items.
const big = clone(FEED);
big.items = big.items.filter((i) => i.clientId !== 'fleet');
for (let k = 0; k < 10; k++) {
  big.items.push({ ...clone(FEED.items[5]), clientId: `c-extra-${k}`, clientName: `Extra ${k}`, fingerprint: `ff00000000${k}` });
  big.items.push({ ...clone(FEED.items[5]), clientId: `c-extra-${k}`, clientName: `Extra ${k}`, code: 'acceptance_drop', level: 'urgent', fingerprint: `fe00000000${k}` });
}
const bigReport = ca.formatReport(big, NOW);
const bigLines = bigReport.split('\n');
check('4a  at most MAX_CLIENT_CARDS cards', bigLines.filter((l) => /^(🔴|🟠) \*.+\*$/.test(l)).length, ca.MAX_CLIENT_CARDS);
check('4b  the rest are counted as clients and items', bigLines.find((l) => l.startsWith('…and')), '…and 4 more clients (7 items) on the admin page.');
check('4c  capped report still passes its criteria', ca.validateReport(bigReport, big), { ok: true, problems: [] });

// ── 5. The validator catches a broken report (made to fail on purpose).
check('5a  a dropped item line fails', ca.validateReport(report.replace(/\n• \[ours\] No LinkedIn account linked[^\n]*/, ''), FEED, { mention: MENTION }).ok, false);
check('5b  a dropped card fails', ca.validateReport(report.replace('🟠 *Flor Lizano Bolaños*\n', ''), FEED, { mention: MENTION }).ok, false);
check('5c  a wrong drift number fails', ca.validateReport(report.replace('🟡 12 clients', '🟡 11 clients'), FEED, { mention: MENTION }).ok, false);
check('5d  a wrong in-band number fails', ca.validateReport(report.replace('✅ 10 clients', '✅ 9 clients'), FEED, { mention: MENTION }).ok, false);
check('5e  a second mention fails', ca.validateReport(`${report}\n${MENTION}`, FEED, { mention: MENTION }).ok, false);
check('5f  a missing mention fails when one is configured', ca.validateReport(ca.formatReport(FEED, NOW), FEED, { mention: MENTION }).ok, false);
check('5g  an injected undefined fails', ca.validateReport(report.replace('Reconnect', 'undefined'), FEED, { mention: MENTION }).ok, false);
check('5h  a missing admin link fails', ca.validateReport(report.replace(FEED.adminUrl, 'https://example.com'), FEED, { mention: MENTION }).ok, false);
const untagged = report.replace('• [client] Prosp says', '• Prosp says');
check('5i  a v2 item line without an owner tag fails', ca.validateReport(untagged, FEED, { mention: MENTION }).problems, ['1 item line(s) without an owner tag']);

// ── 6. Alerts: owner tag, fleet shape, dedupe keys, metadata, records.
const alert = ca.formatAlert(FEED.items[1], FEED.adminUrl);
check('6a  alert line carries the owner and the ✅ hint', alert, `🔴 *Client attention* · [client] US Legal - Guido Soto: ${FEED.items[1].reason} → ${FEED.items[1].firstMove} <${FEED.adminUrl}|Open> · ✅ marks it handled`);
check('6b  alert passes its criteria', ca.validateAlert(alert), { ok: true, problems: [] });
const fleetAlert = ca.formatAlert(FEED.items[0], FEED.adminUrl);
check('6c  fleet alert names the count', fleetAlert.startsWith('🔴 *Client attention* · [ours] Fleet (4 clients): 4 clients stopped'), true);
check('6d  fleet alert passes its criteria', ca.validateAlert(fleetAlert).ok, true);
check('6e  v1 alert (no owner) still passes', ca.validateAlert(ca.formatAlert(FEED_V1.items[1], FEED_V1.adminUrl)).ok, true);
check('6f  a malformed alert fails', ca.validateAlert('🔴 Client attention: something').ok, false);
check('6g  alert key is client, code, fingerprint', ca.alertKey(FEED.items[1]), 'attention:c-uslegal:linkedin_disconnected:bb0000000001');
check('6h  fleet member keys use the fleet code and each member fingerprint', ca.memberKeys(FEED.items[0]), [
  'attention:c-action:sending_stopped:aa0000000001',
  'attention:c-factory:sending_stopped:aa0000000002',
  'attention:c-licita:sending_stopped:aa0000000003',
  'attention:c-umc:sending_stopped:aa0000000004',
]);
check('6i  a per-client item has no member keys', ca.memberKeys(FEED.items[1]), []);

const urgentKeys = FEED.items.filter((i) => i.level === 'urgent').map(ca.alertKey);
check('6j  nothing alerted yet: every urgent item is fresh', ca.planAlerts(FEED, new Set()).send.map(ca.alertKey), urgentKeys);
check('6k  already alerted items are skipped', ca.planAlerts(FEED, new Set([urgentKeys[1]])).send.map(ca.alertKey), urgentKeys.filter((k) => k !== urgentKeys[1]));
const allMembers = new Set(ca.memberKeys(FEED.items[0]));
check('6l  a fleet whose members all alerted on their own is not new', ca.planAlerts(FEED, allMembers).send.some((i) => i.clientId === 'fleet'), false);
const someMembers = new Set(ca.memberKeys(FEED.items[0]).slice(0, 2));
check('6m  a fleet with one member never alerted is new', ca.planAlerts(FEED, someMembers).send.some((i) => i.clientId === 'fleet'), true);
const many = clone(FEED);
for (let k = 0; k < 6; k++) many.items.push({ ...clone(FEED.items[4]), clientId: `c-m-${k}`, clientName: `M ${k}`, fingerprint: `ab0000000${k}00` });
const plan = ca.planAlerts(many, new Set());
check('6n  alerts per run are capped with an overflow count', [plan.send.length, plan.overflow], [ca.MAX_ALERTS_PER_RUN, many.items.filter((i) => i.level === 'urgent').length - ca.MAX_ALERTS_PER_RUN]);
check('6o  overflow line', ca.formatAlertOverflow(3, FEED.adminUrl), `🔴 *Client attention* · 3 more urgent items this run: <${FEED.adminUrl}|see the admin page>`);

const meta = ca.alertMetadata(FEED.items[0]);
check('6p  metadata carries the item and, for a fleet, its members as strings', [meta.event_type, meta.event_payload.client_id, meta.event_payload.code, typeof meta.event_payload.members, JSON.parse(meta.event_payload.members).length], ['client_attention_alert', 'fleet', 'sending_stopped', 'string', 4]);
check('6q  metadata values are all strings', Object.values(ca.alertMetadata(FEED.items[1]).event_payload).every((v) => typeof v === 'string'), true);
check('6r  per-client metadata has no members key', 'members' in ca.alertMetadata(FEED.items[1]).event_payload, false);
const bigFleet = clone(FEED.items[0]);
bigFleet.members = Array.from({ length: 40 }, (_, k) => ({ clientId: `c${k}`, fingerprint: `f${k}` }));
check('6s  metadata members are capped', JSON.parse(ca.alertMetadata(bigFleet).event_payload.members).length, ca.MAX_METADATA_MEMBERS);

const record = ca.alertRecord(FEED.items[1], { channel: 'C123', ts: '1700000000.000100' }, NOW);
const parsed = ca.parseAlertRecord(record);
check('6t  alert record round-trips with channel, ts and post time', [parsed.clientName, parsed.code, parsed.fingerprint, parsed.channel, parsed.ts, parsed.postedAt], ['US Legal - Guido Soto', 'linkedin_disconnected', 'bb0000000001', 'C123', '1700000000.000100', NOW.toISOString()]);
check('6u  legacy record "name · code" is tolerated', ca.parseAlertRecord('US Legal - Guido Soto · sending_stopped'), { clientName: 'US Legal - Guido Soto', code: 'sending_stopped', fingerprint: null, channel: null, ts: null, postedAt: null });
check('6v  record without a post keeps nulls', JSON.parse(ca.alertRecord(FEED.items[1], null, NOW)).ts, null);
check('6w  no em dashes anywhere in the output', /\u2014/.test(report + reportV1 + alert + fleetAlert), false);

// ── 7. ✅ on an alert: targets from the message metadata, the thread reply,
// and the route's place in index.js (before Route 0, which swallows DM ✅).
const fs = require('fs');
const path = require('path');
const metaItem = ca.alertMetadata(FEED.items[1]).event_payload;
check('7a  per-client target from metadata', ca.handledTargets(metaItem), [{ customerId: 'c-uslegal', signalCode: 'linkedin_disconnected', fingerprint: 'bb0000000001' }]);
const metaFleet = ca.alertMetadata(FEED.items[0]).event_payload;
check('7b  fleet targets are every member with the fleet code', ca.handledTargets(metaFleet).map((t) => `${t.customerId}:${t.signalCode}:${t.fingerprint}`), [
  'c-action:sending_stopped:aa0000000001', 'c-factory:sending_stopped:aa0000000002', 'c-licita:sending_stopped:aa0000000003', 'c-umc:sending_stopped:aa0000000004',
]);
check('7c  broken metadata gives no targets', [ca.handledTargets(null), ca.handledTargets({ code: 'x' }), ca.handledTargets({ client_id: 'fleet', code: 'x', members: 'not json' })], [[], [], []]);
check('7d  thread reply for one client', ca.handledReply('<@U1>', 'handled', 1), 'Marked handled by <@U1>. It comes back if the evidence changes.');
check('7e  thread reply for a fleet', ca.handledReply('<@U1>', 'handled', 4), 'Marked handled by <@U1> for 4 clients. It comes back if the evidence changes.');
check('7f  thread reply for a snooze', ca.handledReply('<@U1>', 'snoozed', 1), 'Snoozed 3 days by <@U1>. It comes back after that.');
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const handler = SRC.slice(SRC.indexOf("slack.event('reaction_added'"));
const caRoute = handler.indexOf("event_type === 'client_attention_alert'");
const route0 = handler.indexOf('Route 0:');
check('7g  the client attention route runs before Route 0 in the reaction handler', caRoute > -1 && route0 > -1 && caRoute < route0, true);
check('7h  the handler never marks anything on its own (only on a human reaction)', /handleClientAttentionReaction\(event, baseEmoji, msg/.test(handler), true);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall checks passed');
