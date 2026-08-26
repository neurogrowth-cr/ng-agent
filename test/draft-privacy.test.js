// A draft awaiting approval must never reach a channel. Run:
//   node test/draft-privacy.test.js
//
// Regression under test (2026-08-25): Oscar raised a GHL problem in
// #ng-sales-goats, Max drafted an escalation for #ng-pm-agent and emitted the
// APPROVAL_NEEDED sentinel exactly as designed — and handleDraftReply echoed
// the whole draft back through `say`, which in a channel handler is a
// broadcast. The unapproved escalation was published to the entire sales team.
//
// The invariant these tests pin: in a channel, NOTHING that goes through `say`
// contains any part of the draft body. The body goes to the recipient's DM.
//
// Same extraction trick as the other tests: the block is sliced out of index.js
// and compiled with new Function, with index.js's module-level dependencies
// injected as parameters, so this cannot drift from shipped behaviour.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// A draft awaiting approval belongs to the person'),
  SRC.indexOf('// ─── end draft delivery'),
);

const RON = 'U05HXGX18H3';
const OSCAR = 'U0B1S1UMH9P';

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}

// Builds a fresh sandbox per scenario so state never bleeds between cases.
function harness({ dmFails = false, pendingDrafts = {} } = {}) {
  const said = [];   // everything that went through `say` — the channel, in a channel handler
  const dmed = [];   // everything sent by direct chat.postMessage
  const pendingApprovals = {};
  const slack = { client: { chat: { postMessage: ({ channel, text }) => {
    dmed.push({ channel, text });
    return dmFails ? Promise.reject(new Error('not_in_channel')) : Promise.resolve();
  } } } };
  const say = (text) => { said.push(typeof text === 'string' ? text : text.text); return Promise.resolve(); };
  const getMemberContext = (id) => ({ displayName: id === OSCAR ? 'Oscar' : 'Someone' });
  const g = new Function(
    'slack', 'logActivity', 'getMemberContext', 'pendingApprovals', 'pendingDrafts', 'RON_SLACK_ID', 'EMAIL_PROXY_LIVE', 'supabase',
    `${block}; return { handleDraftReply, handleSetterReview, deliverDraft };`,
  )(slack, () => {}, getMemberContext, pendingApprovals, pendingDrafts, RON, false, null);
  return { g, said, dmed, pendingApprovals, say };
}

// The actual draft from the incident, trimmed. If any fragment of this shows up
// in a channel post, the leak is back.
const DRAFT = 'Ron — Oscar esta tratando de agregar el email al contacto de Diego Palacios en GHL y GHL no le esta dejando hacer el cambio.';
const ESCALATION = `APPROVAL_NEEDED|ng-pm-agent|1|${OSCAR}|Bloqueo operativo en GHL|${DRAFT}`;
const SELF_APPROVE = `APPROVAL_NEEDED|ng-sales-goats|0|${OSCAR}||${DRAFT}`;

// Substrings distinctive enough that a partial leak still trips the check.
const FRAGMENTS = ['Diego Palacios', 'no le esta dejando', DRAFT];
function leaks(texts) {
  return texts.some(t => FRAGMENTS.some(f => String(t).includes(f)));
}

console.log('escalation raised from a CHANNEL — the 2026-08-25 regression');
{
  const { g, said, dmed, say, pendingApprovals } = harness();
  check('handled', g.handleDraftReply(ESCALATION, OSCAR, say, 'cid-1', true), true);
  check('THE BUG — no fragment of the draft reaches the channel', leaks(said), false);
  check('the channel gets exactly one pointer', said.length, 1);
  check('the pointer says where the draft went',
    /sent you the draft in a DM/.test(said[0]), true);
  check('the pointer does not leak the escalation reason either',
    said[0].includes('Bloqueo operativo'), false);
  // Two DMs: the originator's copy and Ron's approval request.
  check('originator gets the draft by DM', dmed.some(d => d.channel === OSCAR && d.text.includes(DRAFT)), true);
  check('Ron still gets the attributed draft', dmed.some(d => d.channel === RON && d.text.includes(DRAFT)), true);
  check("Ron's copy names who escalated", dmed.find(d => d.channel === RON).text.includes('Oscar'), true);
  check('approval is staged for Ron, not the originator',
    [Object.keys(pendingApprovals), pendingApprovals[RON]?.message], [[RON], DRAFT]);
}

console.log('\nescalation raised from a DM — unchanged, say is already private');
{
  const { g, said, dmed, say } = harness();
  g.handleDraftReply(ESCALATION, OSCAR, say, 'cid-2', false);
  check('the originator still sees the full draft inline', said.some(t => t.includes(DRAFT)), true);
  check('no redundant DM to the originator', dmed.some(d => d.channel === OSCAR), false);
  check('Ron still gets his copy', dmed.some(d => d.channel === RON), true);
}

console.log('\nself-approval draft raised from a CHANNEL');
{
  const { g, said, dmed, say, pendingApprovals } = harness();
  check('handled', g.handleDraftReply(SELF_APPROVE, OSCAR, say, 'cid-3', true), true);
  check('no fragment of the draft reaches the channel', leaks(said), false);
  check('the draft goes to the approver by DM',
    dmed.some(d => d.channel === OSCAR && d.text.includes(DRAFT)), true);
  check('the pointer sends them to their DMs to approve, not the channel',
    /DMs[\s\S]*yes/.test(said[0]), true);
  check('approver is the originator', Object.keys(pendingApprovals), [OSCAR]);
}

console.log('\nself-approval draft raised from a DM — unchanged');
{
  const { g, said, dmed, say } = harness();
  g.handleDraftReply(SELF_APPROVE, OSCAR, say, 'cid-4', false);
  check('full draft inline', said.some(t => t.includes(DRAFT)), true);
  check('no extra DM', dmed.length, 0);
}

console.log('\nsetter email review — the prospect body is just as private');
{
  const drafts = { [OSCAR]: { kind: 'email_outbound', to: 'prospect@acme.com', cc: '', subject: 'Seguimiento', body: DRAFT } };
  const { g, said, dmed, say } = harness({ pendingDrafts: drafts });
  check('handled', g.handleDraftReply(`SETTER_REVIEW_NEEDED|outbound|${OSCAR}`, OSCAR, say, 'cid-5', true), true);
  check('no fragment of the email reaches the channel', leaks(said), false);
  check("the prospect's address does not reach the channel",
    said.some(t => t.includes('prospect@acme.com')), false);
  check('the setter gets the preview by DM',
    dmed.some(d => d.channel === OSCAR && d.text.includes(DRAFT)), true);
}
{
  const drafts = { [OSCAR]: { kind: 'email_outbound', to: 'p@acme.com', cc: '', subject: 'S', body: DRAFT } };
  const { g, said, dmed, say } = harness({ pendingDrafts: drafts });
  g.handleDraftReply(`SETTER_REVIEW_NEEDED|outbound|${OSCAR}`, OSCAR, say, 'cid-6', false);
  check('in a DM the preview is inline as before', said.some(t => t.includes(DRAFT)), true);
  check('and not double-sent', dmed.length, 0);
}
{
  const { g, said, say } = harness();  // no staged draft
  check('an expired draft still answers', g.handleDraftReply(`SETTER_REVIEW_NEEDED|outbound|${OSCAR}`, OSCAR, say, 'cid-7', true), true);
  check('and says so without inventing a body', said, ['Draft expired. Please start over.']);
}

console.log('\ndefaults and non-drafts');
{
  const { g, said, dmed, say } = harness();
  check('a plain reply is not a draft', g.handleDraftReply('Diego has no email in GHL yet.', OSCAR, say, 'cid-8', true), false);
  check('and nothing is posted', [said.length, dmed.length], [0, 0]);
}
{
  // A call site that forgets the flag must err private, never broadcast.
  const { g, said, dmed, say } = harness();
  g.handleDraftReply(ESCALATION, OSCAR, say, 'cid-9');
  check('inChannel defaults to false — a missed call site keeps the draft off the channel',
    dmed.some(d => d.channel === RON), true);
  check('and treats say as the private surface it is in a DM', said.some(t => t.includes(DRAFT)), true);
}

console.log('\nwhen the DM cannot be delivered');
{
  const { g, said, say } = harness({ dmFails: true });
  g.handleDraftReply(SELF_APPROVE, OSCAR, say, 'cid-10', true);
  // The rejection is handled asynchronously — let the queue drain before asserting.
  setTimeout(() => {
    check('the failure is surfaced in the channel', said.length, 2);
    check('but still without the draft', leaks(said), false);
    check('and tells them what to do', /open a DM with me/i.test(said[1]), true);

    console.log('');
    if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
    console.log('All draft-privacy tests passed.');
  }, 0);
}
