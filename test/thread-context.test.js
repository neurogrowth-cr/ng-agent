// Tests the pure thread-awareness contract: who owns a thread, how a thread is
// rendered for Max to read, which channel messages he answers at all, and the
// one-reply-per-event guard.
//
// Regression under test (2026-08-19): Ron replied inside the outcome-escalation
// thread in Max's DM and got an answer about an unrelated conversation, posted
// at channel level instead of in the thread.
//
// Regression under test (2026-08-25): isMaxMessage counted ANY bot post as
// Max's own, so Make's booking cards in #ng-sales-goats registered as his
// threads and he answered every human reply under them — one of those replies
// leaked an internal escalation draft into the team channel. He now speaks
// outside #ng-pm-agent only when tagged, or when he rooted the thread AND spoke
// last (someone is answering him).
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
const g = new Function(`${block}; return { isMaxMessage, maxRootsThread, maxSpokeLast, formatThreadTranscript, shouldAnswerChannelMessage, isNoReply, claimEvent, THREAD_ROOT_LIMIT, THREAD_REPLY_LIMIT };`)();

const BOT = 'U0BOTMAX00';
// Real ids from the workspace — this pair is the whole 2026-08-25 bug.
const MAX_BOT  = 'B0AMNSM25QW';
const MAKE_BOT = 'B08BC34U486';
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

console.log('isMaxMessage — whose post is it, really');
check("Max's own bot_id is Max", g.isMaxMessage({ bot_id: MAX_BOT }, BOT, MAX_BOT), true);
check("Make's bot_id is NOT Max (the 2026-08-25 bug)",
  g.isMaxMessage({ bot_id: MAKE_BOT }, BOT, MAX_BOT), false);
check('some other app is not Max', g.isMaxMessage({ bot_id: 'B0OTHER' }, BOT, MAX_BOT), false);
check('post from the bot user is Max', g.isMaxMessage({ user: BOT }, BOT, MAX_BOT), true);
check('human post is not Max', g.isMaxMessage({ user: 'U05HXGX18H3' }, BOT, MAX_BOT), false);
check('missing message is not Max', g.isMaxMessage(null, BOT, MAX_BOT), false);
check('no botUserId configured — user match cannot fire', g.isMaxMessage({ user: BOT }, undefined, MAX_BOT), false);
check('unresolved maxBotId — no bot post counts as Max (fails toward silence)',
  g.isMaxMessage({ bot_id: MAX_BOT }, BOT, null), false);

console.log('\nmaxRootsThread / maxSpokeLast — the two signals the gate runs on');
const MAX_POST  = { bot_id: MAX_BOT };
const MAKE_POST = { bot_id: MAKE_BOT };
const RON       = { user: 'U05HXGX18H3' };
const OSCAR     = { user: 'U0B1S1UMH9P' };

check('Max posted the root → his thread',
  g.maxRootsThread([MAX_POST, RON], BOT, MAX_BOT), true);
check('Make posted the root → NOT his thread, even if he spoke later',
  g.maxRootsThread([MAKE_POST, OSCAR, MAX_POST], BOT, MAX_BOT), false);
check('humans only → not his thread', g.maxRootsThread([RON, OSCAR], BOT, MAX_BOT), false);
check('empty thread → not his thread', g.maxRootsThread([], BOT, MAX_BOT), false);
check('undefined thread → not his thread', g.maxRootsThread(undefined, BOT, MAX_BOT), false);

check('first reply under a Max report — the root IS the last message',
  g.maxSpokeLast([MAX_POST], BOT, MAX_BOT), true);
check('Max answered and is being answered back',
  g.maxSpokeLast([MAX_POST, RON, MAX_POST], BOT, MAX_BOT), true);
check('two humans talking to each other → he is not the last speaker',
  g.maxSpokeLast([MAX_POST, RON, OSCAR], BOT, MAX_BOT), false);
check('Make spoke last → not him', g.maxSpokeLast([MAKE_POST], BOT, MAX_BOT), false);
check('empty thread → nobody spoke last', g.maxSpokeLast([], BOT, MAX_BOT), false);

console.log('\nformatThreadTranscript — what Max actually reads');
const thread = [
  { ts: TS_ROOT,  bot_id: MAX_BOT, text: 'Outcome-logging escalation:\n' + 'x'.repeat(2000) },
  { ts: TS_REPLY, user: 'U05HXGX18H3', text: 'is this still true? ' + 'y'.repeat(500) },
];
const rendered = g.formatThreadTranscript(thread, { botUserId: BOT, maxBotId: MAX_BOT, nameFor });
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

// Another app's post must be named, not silently rendered as a teammate. Before
// the fix a Make booking card came through to Max as "Team Member: :card_index:…"
const makeCard = [{ ts: TS_ROOT, bot_id: MAKE_BOT, bot_profile: { name: 'Make' }, text: 'Diego Palacios — Strategy call booked' }];
checkMatch('another app is named by its app name, not labelled Max',
  g.formatThreadTranscript(makeCard, { botUserId: BOT, maxBotId: MAX_BOT, nameFor }), /\] Make: Diego Palacios/);
check('an app post is never labelled Team Member',
  g.formatThreadTranscript(makeCard, { botUserId: BOT, maxBotId: MAX_BOT, nameFor }).includes('Team Member'), false);
checkMatch('app with no bot_profile falls back to username',
  g.formatThreadTranscript([{ ts: TS_ROOT, bot_id: 'B0OTHER', username: 'Zapier', text: 'hi' }], { botUserId: BOT, maxBotId: MAX_BOT, nameFor }), /\] Zapier: hi/);
checkMatch('nameless app falls back to a generic App label',
  g.formatThreadTranscript([{ ts: TS_ROOT, bot_id: 'B0OTHER', text: 'hi' }], { botUserId: BOT, maxBotId: MAX_BOT, nameFor }), /\] App: hi/);

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
// Shorthand: the gate takes the two thread signals plus whether he was tagged.
const gate = (o) => g.shouldAnswerChannelMessage({ channelName: 'ng-sales-goats', isThreadReply: true, tagged: false, maxRootedThread: false, maxIsLastSpeaker: false, ...o });

check('#ng-pm-agent top-level → answer (unchanged behaviour)',
  gate({ channelName: 'ng-pm-agent', isThreadReply: false }), true);
check('#ng-pm-agent thread → answer',
  gate({ channelName: 'ng-pm-agent' }), true);

// The 2026-08-25 regression, end to end: Make card root, Oscar, then Ron. Max
// neither rooted the thread nor spoke last, and nobody tagged him.
check('THE BUG — Make-rooted thread, humans talking → stay out',
  gate({ maxRootedThread: false, maxIsLastSpeaker: false }), false);
check('Make-rooted thread where Max spoke last → still out, it is not his thread',
  gate({ maxRootedThread: false, maxIsLastSpeaker: true }), false);
check('his own report, someone replying to him → answer',
  gate({ maxRootedThread: true, maxIsLastSpeaker: true }), true);
check('his own report, two humans now talking to each other → drop out',
  gate({ maxRootedThread: true, maxIsLastSpeaker: false }), false);

check('a tag always wins, even in another app\'s thread',
  gate({ tagged: true, maxRootedThread: false, maxIsLastSpeaker: false }), true);
check('a tag wins at top level too',
  gate({ tagged: true, isThreadReply: false }), true);

check('other channel, top-level, untagged → stay out (no new firehose)',
  gate({ isThreadReply: false, maxRootedThread: true, maxIsLastSpeaker: true }), false);
check('private channel is not special-cased — same rule',
  gate({ channelName: 'ng-ops-management', maxRootedThread: true, maxIsLastSpeaker: true }), true);
check('unnamed channel (group DM) follow-up to Max → answer',
  gate({ channelName: '', maxRootedThread: true, maxIsLastSpeaker: true }), true);
check('unnamed channel top-level → stay out',
  gate({ channelName: undefined, isThreadReply: false, maxRootedThread: true, maxIsLastSpeaker: true }), false);

console.log('\nisNoReply — the value gate sentinel');
check('bare sentinel', g.isNoReply('NO_REPLY'), true);
check('padded sentinel', g.isNoReply('  NO_REPLY\n'), true);
check('backticked sentinel', g.isNoReply('`NO_REPLY`'), true);
check('sentinel with a trailing period', g.isNoReply('NO_REPLY.'), true);
check('lowercase sentinel', g.isNoReply('no_reply'), true);
check('a real answer that mentions it is NOT a skip',
  g.isNoReply('NO_REPLY is what I would normally send, but here is the number: 47'), false);
check('an ordinary answer is not a skip', g.isNoReply('Diego has no email in GHL yet.'), false);
check('empty reply is not a skip (handled by the empty-reply retry)', g.isNoReply(''), false);
check('null is not a skip', g.isNoReply(null), false);

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
