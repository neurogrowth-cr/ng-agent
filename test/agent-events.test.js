// Rules test for the agent event bus (Max side).  Run:  node test/agent-events.test.js
// lib/agentEvents.js is pure. Events below are synthetic (public repo).
const fs = require('fs');
const path = require('path');
const ev = require('../lib/agentEvents');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const NOW = new Date('2026-11-09T16:00:00Z'); // Mon 10:00 CR
const mk = (id, over = {}) => ({ id, created_at: new Date(NOW.getTime() - (100 - id) * 1000).toISOString(),
  source: 'axon', kind: 'error', severity: 'warn', subject_key: 'run:abc', summary: 'Factory run failed', payload: {}, ...over });

// ── 1. buildEvent: the contract the emitters share
const good = ev.buildEvent('axon', { kind: 'needs_human', severity: 'warn', summary: '  Launch QA failed\n on Acme  ', idempotencyKey: 'launch_qa:1:2026-11-09', subjectKey: 'campaign:1' });
check('1a  valid event builds', !!good.row, true);
check('1b  summary whitespace folded', good.row.summary, 'Launch QA failed on Acme');
check('1c  defaults audience ron', good.row.audience, 'ron');
check('1d  unknown source', ev.buildEvent('hubspot', { kind: 'error', summary: 'x', idempotencyKey: 'k' }).error, 'unknown source hubspot');
check('1e  unknown kind', ev.buildEvent('axon', { kind: 'heartbeat', summary: 'x', idempotencyKey: 'k' }).error, 'unknown kind heartbeat');
check('1f  missing idempotency key', ev.buildEvent('axon', { kind: 'error', summary: 'x' }).error, 'idempotencyKey is required (1-200 chars)');
check('1g  empty summary', ev.buildEvent('axon', { kind: 'error', summary: '   ', idempotencyKey: 'k' }).error, 'summary is required');
check('1h  payload over 4 KB refused', ev.buildEvent('axon', { kind: 'error', summary: 'x', idempotencyKey: 'k', payload: { t: 'a'.repeat(5000) } }).error, 'payload over 4096 bytes');
check('1i  long summary clipped to 300', ev.buildEvent('axon', { kind: 'error', summary: 'b'.repeat(400), idempotencyKey: 'k' }).row.summary.length, 300);
check('1j  array payload becomes {}', ev.buildEvent('axon', { kind: 'error', summary: 'x', idempotencyKey: 'k', payload: [1] }).row.payload, {});

// ── 2. routeEvent
const r = (o, ctx) => ev.routeEvent(mk(1, o), { now: NOW, ...ctx }).route;
check('2a  warn error posts', r({}), 'post_error');
check('2b  red error DMs Ron', r({ severity: 'red' }), 'dm_ron');
check('2c  needs_human warn goes to the brief', r({ kind: 'needs_human' }), 'clarity');
check('2d  needs_human red DMs Ron', r({ kind: 'needs_human', severity: 'red' }), 'dm_ron');
check('2e  job_done is stored, never posted', r({ kind: 'job_done' }), 'store');
check('2f  handoff off the allowlist is dropped', r({ kind: 'handoff', payload: { handler: 'stop_campaign' } }), 'drop');
check('2g  malformed source dropped', r({ source: 'evil' }), 'drop');
check('2h  same error within 6 h collapses', r({}, { recentErrorPosts: new Map([['axon|run:abc', NOW.getTime() - 3600 * 1000]]) }), 'collapsed');
check('2i  same error after 6 h posts again', r({}, { recentErrorPosts: new Map([['axon|run:abc', NOW.getTime() - 7 * 3600 * 1000]]) }), 'post_error');
check('2j  over the daily budget', r({}, { usedToday: new Map([['axon', 20]]) }), 'over_budget');
check('2k  job_done ignores the budget', r({ kind: 'job_done' }, { usedToday: new Map([['axon', 99]]) }), 'store');

// ── 3. planBatch: collapse and budget inside one poll
const batch = ev.planBatch([mk(3), mk(2), mk(4, { subject_key: 'run:other' })], { now: NOW });
check('3a  oldest first', batch.map(b => b.id), [2, 3, 4]);
check('3b  second error on same key collapses', batch.map(b => b.route), ['post_error', 'collapsed', 'post_error']);
const many = Array.from({ length: 22 }, (_, i) => mk(i + 1, { kind: 'needs_human', subject_key: `c:${i}` }));
const planned = ev.planBatch(many, { now: NOW });
check('3c  budget caps at 20 per source', planned.filter(p => p.route === 'clarity').length, 20);
check('3d  the rest are over budget', planned.filter(p => p.route === 'over_budget').length, 2);

// ── 4. digest and helpers
check('4a  quiet day, no digest', ev.formatShadowDigest([]), null);
const digest = ev.formatShadowDigest([
  { route: 'post_error', source: 'axon', kind: 'error', summary: 'Factory run failed for Acme' },
  { route: 'clarity', source: 'axon', kind: 'needs_human', summary: 'Launch QA failed for Beta' },
], { day: '2026-11-09' });
check('4b  digest names both routes', /would post to #ng-pm-agent/.test(digest) && /morning brief/.test(digest), true);
check('4c  digest has no em dash', digest.includes('\u2014'), false);
check('4d  stale backlog over 15 min', ev.staleBacklog(new Date(NOW.getTime() - 20 * 60000), NOW), 20);
check('4e  fresh backlog is fine', ev.staleBacklog(new Date(NOW.getTime() - 5 * 60000), NOW), null);
check('4f  mode defaults off', ev.eventsMode({}), 'off');
check('4g  unknown mode is off', ev.eventsMode({ AGENT_EVENTS_MODE: 'yes' }), 'off');
check('4h  shadow mode', ev.eventsMode({ AGENT_EVENTS_MODE: 'Shadow' }), 'shadow');
check('4i  CR day boundary', ev.crDay('2026-11-09T05:59:00Z'), '2026-11-08');
check('4j  live text', ev.formatLive(mk(1)), '⚠️ *AXON error*: Factory run failed\n_run:abc_');

// ── 5. the migration and the lib agree on the enums
const sql = fs.readFileSync(path.join(__dirname, '..', 'migrations', '026_agent_events.sql'), 'utf8');
const enumIn = (name) => (sql.match(new RegExp(`${name}\\s+IN \\(([^)]*)\\)`)) || [])[1].split(',').map(s => s.trim().replace(/'/g, ''));
check('5a  sources match', enumIn('source'), ev.SOURCES);
check('5b  kinds match', enumIn('kind'), ev.KINDS);
check('5c  severities match', enumIn('severity'), ev.SEVERITIES);
check('5d  audiences match', enumIn('audience'), ev.AUDIENCES);
check('5e  payload cap matches', sql.includes(`<= ${ev.PAYLOAD_MAX_BYTES}`), true);

// ── 6. Clarity Brief items (phase 2)
const ev6 = (id, over = {}) => ({ id, created_at: new Date(NOW.getTime() - id * 3600e3).toISOString(), source: 'axon', kind: 'needs_human',
  subject_key: `campaign:${id}`, summary: `Launch QA failed for Client ${id}`, acked_at: null, ...over });
const url = (s) => `https://slack.com/app_redirect?channel=${s}`;
const one = ev.briefCandidates([ev6(5)], { channelUrl: url, now: NOW });
check('6a  one event, one item', one.length, 1);
check('6b  item shape', [one[0].source, one[0].actor, one[0].tier, one[0].dedupeKey, one[0].url], ['agents', 'axon', 2, 'agents:axon:5', 'https://slack.com/app_redirect?channel=axon']);
check('6c  single title names it', one[0].title, 'AXON: Launch QA failed for Client 5');
check('6d  never sent to the classifier', one[0].needsClassification, undefined);
const grouped = ev.briefCandidates([ev6(1), ev6(2), ev6(3), ev6(4), ev6(9, { subject_key: 'campaign:1' })], { channelUrl: url, now: NOW });
check('6e  many events, still one item per agent', grouped.length, 1);
check('6f  repeat subject counted once', grouped[0].title, 'AXON: 4 things need a human');
check('6g  newest id in the key', grouped[0].dedupeKey, 'agents:axon:9');
check('6h  snippet caps at 3 and counts the rest', /and 1 more$/.test(grouped[0].snippet), true);
const mixed = ev.briefCandidates([ev6(1), ev6(2, { source: 'revi' })], { channelUrl: url, now: NOW });
check('6i  one item per agent', mixed.map(m => m.actor).sort(), ['axon', 'revi']);
check('6j  acked, errors and old events skipped', ev.briefCandidates([ev6(1, { acked_at: NOW.toISOString() }), ev6(2, { kind: 'error' }), ev6(200)], { now: NOW }).length, 0);
check('6k  parse key', ev.parseBriefKey('agents:axon:42'), { source: 'axon', maxId: 42 });
check('6l  foreign key ignored', [ev.parseBriefKey('max:dm:1'), ev.parseBriefKey('agents:hubspot:1')], [null, null]);
check('6m  ✅, done and ❌ acknowledge, ⏭ does not', ['accepted', 'done', 'dismissed', 'snoozed'].map(ev.acksEvents), [true, true, true, false]);
const cb = require('../lib/clarityBrief');
check('6n  brief lists agents as a source', cb.SOURCES.includes('agents'), true);
const reply = cb.formatItemReply({ ...grouped[0], whyRon: grouped[0].whyRon, action: grouped[0].action }, 1);
check('6o  brief reply passes the contract', cb.validateClarityBrief({ parent: cb.formatParent({ date: NOW, items: [grouped[0]], scanned: cb.scannedLine({}) }), replies: [reply] }).ok, true);

// ── 7. Red events DM Ron once per subject a week (Ron, 2026-11-25)
const red = (id, over = {}) => mk(id, { kind: 'needs_human', severity: 'red', subject_key: 'campaign:9', ...over });
const day = 24 * 3600 * 1000;
check('7a  first red DMs Ron', r({ kind: 'needs_human', severity: 'red', subject_key: 'campaign:9' }), 'dm_ron');
check('7b  repeat red within a week goes to the brief', ev.routeEvent(red(1), { now: NOW, recentRedDms: new Map([['axon|campaign:9', NOW.getTime() - 2 * day]]) }).route, 'clarity');
check('7c  after a week it DMs again', ev.routeEvent(red(1), { now: NOW, recentRedDms: new Map([['axon|campaign:9', NOW.getTime() - 8 * day]]) }).route, 'dm_ron');
check('7d  another campaign still DMs', ev.routeEvent(red(1, { subject_key: 'campaign:10' }), { now: NOW, recentRedDms: new Map([['axon|campaign:9', NOW.getTime()]]) }).route, 'dm_ron');
check('7e  repeat red error collapses', ev.routeEvent(red(1, { kind: 'error' }), { now: NOW, recentRedDms: new Map([['axon|campaign:9', NOW.getTime()]]) }).route, 'collapsed');
const day3 = ev.planBatch([red(1), red(2), red(3, { subject_key: 'campaign:10' }), red(4, { subject_key: 'campaign:10' })], { now: NOW });
check('7f  same batch: one DM per campaign, rest to the brief', day3.map(d => d.route), ['dm_ron', 'clarity', 'dm_ron', 'clarity']);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nagent-events: all checks passed');
