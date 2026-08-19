// Tests the pure thread-awareness contract: who owns a thread, how a thread is
// rendered for Max to read, which channel messages he answers at all, and the
// one-reply-per-event guard.
//
// Regression under test (2026-08-19): Ron replied inside the outcome-escalation
// thread in Max's DM and got an answer about an unrelated conversation, posted
// at channel level instead of in the thread.
//
// Run:  node test/thread-context.test.js
//
// Same extraction trick as the other tests: the block is sliced out of
// index.js and compiled with new Function, so this can never drift from
// shipped behaviour.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const THREAD_ROOT_LIMIT'),
  SRC.indexOf('// ─── end pure thread helpers'),
);
const g = new Function(`${block}; return { isMaxMessage, isMaxThread, formatThreadTranscript, shouldAnswerChannelMessage, claimEvent, THREAD_ROOT_LIMIT, THREAD_REPLY_LIMIT };`)();

const BOT = 'U0BOTMAX00';
let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}
function checkMatch(name, actual, re) {
  if (re.test(actual)) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      ${re} did not match\n      got   ${actual}`); }
}

// Fixed ts values so the rendered timestamps are deterministic.
const TS_ROOT  = '1755000000.000100';
const TS_REPLY = '1755003600.000200';

const NAMES = { U05HXGX18H3: 'Ron', U0AMTEKDCPN: 'Jose', U0APYAE0999: 'Jonathan' };
const nameFor = (id) => (id === BOT ? 'Max' : (NAMES[id] || 'Team Member'));

console.log('isMaxMessage / isMaxThread — who owns the thread');
check('bot_id post is Max', g.isMaxMessage({ bot_id: 'B01' }, BOT), true);
check('post from the bot user is Max', g.isMaxMessage({ user: BOT }, BOT), true);
check('human post is not Max', g.isMaxMessage({ user: 'U05HXGX18H3' }, BOT), false);
check('missing message is not Max', g.isMaxMessage(null, BOT), false);
check('no botUserId configured — user match cannot fire', g.isMaxMessage({ user: BOT }, undefined), false);

check('root posted by Max → Max thread',
  g.isMaxThread([{ bot_id: 'B01' }, { user: 'U05HXGX18H3' }], BOT), true);
check('Max replied later → still his thread',
  g.isMaxThread([{ user: 'U05HXGX18H3' }, { user: BOT }], BOT), true);
check('humans only → not his thread',
  g.isMaxThread([{ user: 'U05HXGX18H3' }, { user: 'U0AMTEKDCPN' }], BOT), false);
check('empty thread → not his thread', g.isMaxThread([], BOT), false);
check('undefined thread → not his thread', g.isMaxThread(undefined, BOT), false);

console.log('\nformatThreadTranscript — what Max actually reads');
const thread = [
  { ts: TS_ROOT,  bot_id: 'B01', text: 'Outcome-logging escalation:\n' + 'x'.repeat(2000) },
  { ts: TS_REPLY, user: 'U05HXGX18H3', text: 'is this still true? ' + 'y'.repeat(500) },
];
const rendered = g.formatThreadTranscript(thread, { botUserId: BOT, nameFor });
checkMatch('Max is labelled Max', rendered, /\] Max: Outcome-logging escalation:/);
checkMatch('roster name is used, not user:Uxxxx', rendered, /\] Ron: is this still true\?/);
check('no raw user: ids leak through', /user:U/.test(rendered), false);
// The root is the report being asked about — the old inline version cut it at
// 300 chars, which is part of why Max could not answer "are these still pending".
// Counted across the whole render: a real report root is multi-line, so the
// root is not simply the first line of the transcript.
const xs = (rendered.match(/x/g) || []).length;
const ys = (rendered.match(/y/g) || []).length;
check('root gets the 1200-char budget, not the 300 the replies get',
  xs > g.THREAD_REPLY_LIMIT && xs > g.THREAD_ROOT_LIMIT - 60, true);
check('root is still truncated at the budget', xs < 2000, true);
check('reply is cut at the reply budget', ys <= g.THREAD_REPLY_LIMIT, true);
checkMatch('truncation is marked with an ellipsis', rendered, /…/);

const mentionThread = [{ ts: TS_ROOT, user: 'U0AMTEKDCPN', text: 'ping <@U05HXGX18H3> and <@U0BOTMAX00> on this' }];
checkMatch('mention markup resolves to names',
  g.formatThreadTranscript(mentionThread, { botUserId: BOT, nameFor }), /@Ron and @Max/);
check('unknown sender falls back to the roster default',
  g.formatThreadTranscript([{ ts: TS_ROOT, user: 'UNOBODY', text: 'hi' }], { botUserId: BOT, nameFor })
    .includes('Team Member: hi'), true);
check('no nameFor → raw id label (helper is standalone)',
  g.formatThreadTranscript([{ ts: TS_ROOT, user: 'UNOBODY', text: 'hi' }], { botUserId: BOT })
    .includes('user:UNOBODY: hi'), true);
check('empty thread renders empty', g.formatThreadTranscript([], { botUserId: BOT, nameFor }), '');
check('missing text does not throw',
  g.formatThreadTranscript([{ ts: TS_ROOT, user: 'U05HXGX18H3' }], { botUserId: BOT, nameFor })
    .endsWith('Ron: '), true);

console.log('\nshouldAnswerChannelMessage — where Max is allowed to speak');
check('#ng-pm-agent top-level → answer (unchanged behaviour)',
  g.shouldAnswerChannelMessage({ channelName: 'ng-pm-agent', isThreadReply: false, maxIsInThread: false }), true);
check('#ng-pm-agent thread → answer',
  g.shouldAnswerChannelMessage({ channelName: 'ng-pm-agent', isThreadReply: true, maxIsInThread: false }), true);
check('other channel, top-level → stay out (no new firehose)',
  g.shouldAnswerChannelMessage({ channelName: 'ng-sales-goats', isThreadReply: false, maxIsInThread: true }), false);
check('other channel, thread rooted on a Max post → answer',
  g.shouldAnswerChannelMessage({ channelName: 'ng-sales-goats', isThreadReply: true, maxIsInThread: true }), true);
check('other channel, thread between two humans → stay out',
  g.shouldAnswerChannelMessage({ channelName: 'ng-sales-goats', isThreadReply: true, maxIsInThread: false }), false);
check('private channel is not special-cased — same rule',
  g.shouldAnswerChannelMessage({ channelName: 'ng-ops-management', isThreadReply: true, maxIsInThread: true }), true);
check('unnamed channel (group DM) thread with Max → answer',
  g.shouldAnswerChannelMessage({ channelName: '', isThreadReply: true, maxIsInThread: true }), true);
check('unnamed channel top-level → stay out',
  g.shouldAnswerChannelMessage({ channelName: undefined, isThreadReply: false, maxIsInThread: true }), false);

console.log('\nclaimEvent — one reply per Slack event (app_mention vs slack.message)');
const T0 = 1_755_000_000_000;
check('first claim wins', g.claimEvent('C123', '111.0001', T0), true);
check('second claim on the same event is a no-op', g.claimEvent('C123', '111.0001', T0 + 50), false);
check('a different message in the same channel is unaffected', g.claimEvent('C123', '222.0002', T0 + 50), true);
check('the same ts in a different channel is unaffected', g.claimEvent('C999', '111.0001', T0 + 50), true);
check('claim expires after the TTL so the map cannot grow forever',
  g.claimEvent('C123', '111.0001', T0 + 61_000), true);

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All thread-context tests passed.');
