// Rules test for portal request alerts (dash Ayuda and Campañas → Slack).
// Run:  node test/portal-request.test.js
const pr = require('../lib/portalRequest');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MENTION = '<!subteam^S0BH988QW1K|@fulfillment>';
const HELP = {
  id: 'help:42',
  kind: 'help',
  clientName: 'Mind Lift',
  label: 'Access or Technical Problem',
  priority: 'High',
  excerpt: 'I cannot log in since this morning.',
  adminUrl: 'https://dash.neurogrowth.io/admin?tab=feedback',
  mode: 'live',
};
const CAMPAIGN = {
  id: 'campaign:r1',
  kind: 'campaign',
  clientName: 'Acme <Corp> & Co',
  label: 'New campaign',
  priority: null,
  excerpt: 'Goal: Book calls. Target: CFOs. Offer: Audit.',
  adminUrl: 'https://dash.neurogrowth.io/admin?tab=campaigns&section=requests',
  mode: 'dry_run',
};

// ── 1. Payload checks (fail closed)
check('1a  help request is accepted', pr.checkRequest(HELP).ok, true);
check('1b  campaign request is accepted', pr.checkRequest(CAMPAIGN).ok, true);
check('1c  unknown kind is refused', pr.checkRequest({ ...HELP, kind: 'other' }).ok, false);
check('1d  missing client is refused', pr.checkRequest({ ...HELP, clientName: '  ' }).ok, false);
check('1e  empty excerpt is refused', pr.checkRequest({ ...HELP, excerpt: '' }).ok, false);
check('1f  a link outside the dash admin is refused', pr.checkRequest({ ...HELP, adminUrl: 'https://evil.example/admin' }).ok, false);
check('1g  a link with Slack control characters is refused', pr.checkRequest({ ...HELP, adminUrl: 'https://dash.neurogrowth.io/admin|click' }).ok, false);
check('1h  missing mode is a dry run', pr.checkRequest({ ...HELP, mode: undefined }).request.mode, 'dry_run');
check('1i  unknown mode is a dry run', pr.checkRequest({ ...HELP, mode: 'LIVE!' }).request.mode, 'dry_run');
check('1j  "-" priority is dropped', pr.checkRequest({ ...HELP, priority: '-' }).request.priority, null);
check('1k  long excerpt is clipped', pr.checkRequest({ ...HELP, excerpt: 'x'.repeat(900) }).request.excerpt.length, pr.MAX_EXCERPT);
check('1l  not an object is refused', pr.checkRequest(null).ok, false);

// ── 2. Text
check('2a  live help request text', pr.formatRequest(pr.checkRequest(HELP).request, { mention: MENTION }), [
  `${MENTION} 🆘 New help request from *Mind Lift*`,
  'Category: Access or Technical Problem · Priority: High',
  '> I cannot log in since this morning.',
  '<https://dash.neurogrowth.io/admin?tab=feedback|Open in admin> · Answer in the portal so the client sees it there.',
].join('\n'));
check('2b  campaign request escapes the client name and has no priority', pr.formatRequest(pr.checkRequest(CAMPAIGN).request), [
  '📣 New campaign request from *Acme &lt;Corp&gt; &amp; Co*',
  'Type: New campaign',
  '> Goal: Book calls. Target: CFOs. Offer: Audit.',
  '<https://dash.neurogrowth.io/admin?tab=campaigns&section=requests|Open in admin> · Answer in the portal so the client sees it there.',
].join('\n'));
check('2c  a multi-line excerpt stays one quoted line',
  pr.formatRequest(pr.checkRequest({ ...HELP, excerpt: 'one\n\ntwo' }).request).split('\n')[2], '> one two');
check('2d  no em dash in the text', /—/.test(pr.formatRequest(pr.checkRequest(HELP).request, { mention: MENTION })), false);

// ── 3. Once per request
let t = 0;
const seen = pr.createSeen(1000, () => t);
check('3a  new id is not seen', seen.has('help:1'), false);
seen.add('help:1');
check('3b  repeated id is seen', seen.has('help:1'), true);
t = 1001;
check('3c  id is forgotten after the ttl', seen.has('help:1'), false);
seen.add('help:2'); seen.delete('help:2');
check('3d  a failed post can be retried', seen.has('help:2'), false);

// ── 4. Updates: taken or closed (dash "Take it" and "Close Feedback")
const LIVE_CHANNEL = 'C0B48H7NT41';
const CLAIM = {
  id: 'help:42',
  event: 'claimed',
  by: 'Valeria',
  clientName: 'Mind Lift',
  channel: LIVE_CHANNEL,
  ts: '1764170400.123456',
  mode: 'live',
};
const CLOSE = { ...CLAIM, event: 'closed', by: 'Gerald', answer: 'We changed the second message.\nIt is live.' };

check('4a  claim is accepted', pr.checkUpdate(CLAIM).ok, true);
check('4b  close is accepted', pr.checkUpdate(CLOSE).ok, true);
check('4c  unknown event is refused', pr.checkUpdate({ ...CLAIM, event: 'reopened' }).ok, false);
check('4d  missing by is refused', pr.checkUpdate({ ...CLAIM, by: ' ' }).ok, false);
check('4e  missing client is refused', pr.checkUpdate({ ...CLAIM, clientName: '' }).ok, false);
check('4f  not an object is refused', pr.checkUpdate('x').ok, false);
check('4g  missing mode is a dry run', pr.checkUpdate({ ...CLAIM, mode: undefined }).update.mode, 'dry_run');
check('4h  a claim never carries an answer', pr.checkUpdate({ ...CLAIM, answer: 'hi' }).update.answer, '');
check('4i  a long answer is clipped', pr.checkUpdate({ ...CLOSE, answer: 'y'.repeat(900) }).update.answer.length, pr.MAX_ANSWER);
check('4j  a malformed ts drops the thread', pr.checkUpdate({ ...CLAIM, ts: 'abc' }).update.ts, null);
check('4k  a malformed channel drops the thread', pr.checkUpdate({ ...CLAIM, channel: '#general' }).update.channel, null);

const claim = pr.checkUpdate(CLAIM).update;
check('4l  live update threads under the alert in the request channel',
  pr.threadTarget(claim, { liveChannel: LIVE_CHANNEL }), { channel: LIVE_CHANNEL, ts: CLAIM.ts });
check('4m  live update never threads in another channel',
  pr.threadTarget({ ...claim, channel: 'C0OTHER999' }, { liveChannel: LIVE_CHANNEL }), null);
check('4n  dry run threads only in a DM',
  pr.threadTarget({ ...claim, mode: 'dry_run', channel: 'D0RONDM123' }, { liveChannel: LIVE_CHANNEL }), { channel: 'D0RONDM123', ts: CLAIM.ts });
check('4o  dry run never threads in the request channel',
  pr.threadTarget({ ...claim, mode: 'dry_run' }, { liveChannel: LIVE_CHANNEL }), null);
check('4p  no stored alert means no thread',
  pr.threadTarget(pr.checkUpdate({ ...CLAIM, channel: undefined, ts: undefined }).update, { liveChannel: LIVE_CHANNEL }), null);

const close = pr.checkUpdate(CLOSE).update;
check('4q  claim text in the thread', pr.formatUpdate(claim, { threaded: true }), '👀 *Valeria* took it.');
check('4r  claim text standalone names the client', pr.formatUpdate(claim, { threaded: false }), '👀 *Valeria* took the help request from *Mind Lift*.');
check('4s  close text in the thread quotes the answer on one line', pr.formatUpdate(close, { threaded: true }),
  '✅ Closed by *Gerald*. Answer sent to the client:\n> We changed the second message. It is live.');
check('4t  close with no answer says so', pr.formatUpdate({ ...close, answer: '' }, { threaded: true }),
  '✅ Closed by *Gerald*. Closed with no answer: the client only sees Resolved.');
check('4u  close text standalone escapes the client name',
  pr.formatUpdate({ ...close, clientName: 'Acme <Corp>' }, { threaded: false }).split('.')[0], '✅ *Gerald* closed the help request from *Acme &lt;Corp&gt;*');
check('4v  no em dash in update texts',
  [pr.formatUpdate(claim, { threaded: false }), pr.formatUpdate(close, { threaded: false })].some((s) => /—/.test(s)), false);
check('4w  reactions: eyes while taken', pr.updateReactions('claimed'), { add: ['eyes'], remove: [] });
check('4x  reactions: check mark once closed, eyes and clock gone', pr.updateReactions('closed'), { add: ['white_check_mark'], remove: ['eyes', 'alarm_clock'] });

// ── 5. Send update and overdue (Ron, 2026-11-27: 24, 48, 72 h until the first update or the close)
const UPDATE = { ...CLAIM, event: 'updated', by: 'Valeria', answer: 'We are adjusting message 2.\nNew version tomorrow.' };
const OVERDUE = { id: 'help:42', event: 'overdue', hours: 24, claimedBy: 'Valeria', clientName: 'Mind Lift', channel: LIVE_CHANNEL, ts: CLAIM.ts, mode: 'live' };

check('5a  update is accepted', pr.checkUpdate(UPDATE).ok, true);
check('5b  update without text is refused', pr.checkUpdate({ ...UPDATE, answer: '  ' }).ok, false);
check('5c  overdue needs no "by"', pr.checkUpdate(OVERDUE).ok, true);
check('5d  overdue needs a response time of 24, 48 or 72', pr.checkUpdate({ ...OVERDUE, hours: 12 }).ok, false);
check('5e  overdue never carries an answer', pr.checkUpdate({ ...OVERDUE, answer: 'x' }).update.answer, '');
check('5f  a claim still needs "by"', pr.checkUpdate({ ...CLAIM, by: '' }).ok, false);

const upd = pr.checkUpdate(UPDATE).update;
const late = pr.checkUpdate(OVERDUE).update;
check('5g  update text in the thread', pr.formatUpdate(upd, { threaded: true }),
  '💬 *Valeria* sent the client an update:\n> We are adjusting message 2. New version tomorrow.');
check('5h  update text standalone names the client', pr.formatUpdate(upd, { threaded: false }).split('\n')[0],
  '💬 *Valeria* sent *Mind Lift* an update on their help request:');
check('5i  overdue in the thread tags the team and names who has it', pr.formatUpdate(late, { threaded: true, mention: MENTION }),
  `${MENTION} ⏰ Past its 24 h response time with no update to the client. Taken by *Valeria*.`);
check('5j  overdue nobody took, dry run, standalone', pr.formatUpdate({ ...late, claimedBy: '' }, { threaded: false }),
  '⏰ The help request from *Mind Lift* is past its 24 h response time with no update to the client. Nobody has taken it.');
check('5k  reactions: update keeps eyes and clears the clock', pr.updateReactions('updated'), { add: ['eyes'], remove: ['alarm_clock'] });
check('5l  reactions: overdue adds the clock', pr.updateReactions('overdue'), { add: ['alarm_clock'], remove: [] });
check('5m  no em dash in the new texts',
  [pr.formatUpdate(upd, { threaded: false }), pr.formatUpdate(late, { threaded: false })].some((x) => /\u2014/.test(x)), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall portal request checks passed');
