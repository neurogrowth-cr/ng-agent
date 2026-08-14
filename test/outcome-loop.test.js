// Tests the outcome-loop pure contract: outcome→prospect-status promotion
// (mirrored from dash status-map.ts + outcome-map.ts), the REVI proposal
// gate (won is never proposable), and the 36h recording matcher.
// Run:  node test/outcome-loop.test.js
//
// Same extraction trick as the other tests: the block is sliced out of
// index.js and compiled with new Function, so this can never drift from
// shipped behaviour.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const OUTCOME_STATUS_RANK'),
  SRC.indexOf('// Writes one outcome row + the matching prospect-status promotion'),
);
const g = new Function(`${block}; return { nextProspectStatusForOutcome, proposalFromReviRead, matchRecordingToCall, VALID_LOGGABLE_OUTCOMES, OUTCOME_MATCH_PAD_MS };`)();

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}

console.log('nextProspectStatusForOutcome — mirrors dash mergeProspectStatus + prospectStatusFromOutcome');
// Advances
check('follow_up promotes appointment_booked → nurture (30→35)', g.nextProspectStatusForOutcome('follow_up', 'appointment_booked'), 'nurture');
check('no_show promotes appointment_booked → appointment_held (dash contract: the call event happened)', g.nextProspectStatusForOutcome('no_show', 'appointment_booked'), 'appointment_held');
check('won is terminal from anywhere', g.nextProspectStatusForOutcome('won', 'prospect'), 'won');
check('lost is terminal from nurture', g.nextProspectStatusForOutcome('lost', 'nurture'), 'lost');
check('disqualified is terminal', g.nextProspectStatusForOutcome('disqualified', 'qualified'), 'disqualified');
// No-ops and guards
check('same status is a no-op', g.nextProspectStatusForOutcome('follow_up', 'nurture'), null);
check('never regress: follow_up (→nurture, 35) does not downgrade appointment_held (40)', g.nextProspectStatusForOutcome('follow_up', 'appointment_held'), null);
check('terminal is sticky: follow_up cannot leave won', g.nextProspectStatusForOutcome('follow_up', 'won'), null);
check('terminal is sticky: no_show cannot leave lost', g.nextProspectStatusForOutcome('no_show', 'lost'), null);
check('terminal→terminal is allowed (won over lost is an explicit terminal write)', g.nextProspectStatusForOutcome('won', 'lost'), 'won');
check('unknown outcome maps to no change', g.nextProspectStatusForOutcome('garbage', 'prospect'), null);

console.log('proposalFromReviRead — won is NEVER proposable, pending is no read');
check('stall proposes follow_up', g.proposalFromReviRead('stall'), { outcome: 'follow_up', wonHint: false });
check('lost proposes lost', g.proposalFromReviRead('lost'), { outcome: 'lost', wonHint: false });
check('won proposes nothing but sets wonHint', g.proposalFromReviRead('won'), { outcome: null, wonHint: true });
check('pending proposes nothing', g.proposalFromReviRead('pending'), { outcome: null, wonHint: false });
check('null proposes nothing', g.proposalFromReviRead(null), { outcome: null, wonHint: false });
check('case-insensitive', g.proposalFromReviRead('STALL'), { outcome: 'follow_up', wonHint: false });

console.log('VALID_LOGGABLE_OUTCOMES — what a human may log');
check('the five loggable outcomes', [...g.VALID_LOGGABLE_OUTCOMES].sort(), ['disqualified', 'follow_up', 'lost', 'no_show', 'won']);

console.log('matchRecordingToCall — 36h window, nearest wins, one recording vouches once');
const H = 3600 * 1000;
const T0 = Date.parse('2026-08-01T18:00:00Z');
const mkRecs = () => ([
  { email: 'a@x.com', at: T0 + 2 * H,  matched: false, tag: 'a-near' },
  { email: 'a@x.com', at: T0 + 30 * H, matched: false, tag: 'a-far' },
  { email: 'b@x.com', at: T0,          matched: false, tag: 'b' },
]);
let recs = mkRecs();
check('matches same email inside window, nearest of two', g.matchRecordingToCall(recs, 'a@x.com', T0)?.tag, 'a-near');
check('a matched recording is consumed — second call gets the other one', g.matchRecordingToCall(recs, 'a@x.com', T0)?.tag, 'a-far');
check('third call for same email finds nothing left', g.matchRecordingToCall(recs, 'a@x.com', T0), null);
recs = mkRecs();
check('wrong email never matches', g.matchRecordingToCall(recs, 'c@x.com', T0), null);
check('email compare is case-insensitive', g.matchRecordingToCall(recs, 'B@X.com', T0)?.tag, 'b');
recs = [{ email: 'a@x.com', at: T0 + g.OUTCOME_MATCH_PAD_MS, matched: false, tag: 'edge' }];
check('exactly 36h away is inside the window', g.matchRecordingToCall(recs, 'a@x.com', T0)?.tag, 'edge');
recs = [{ email: 'a@x.com', at: T0 + g.OUTCOME_MATCH_PAD_MS + 1, matched: false, tag: 'out' }];
check('36h + 1ms is outside', g.matchRecordingToCall(recs, 'a@x.com', T0), null);
check('missing email on the call never matches', g.matchRecordingToCall(mkRecs(), '', T0), null);
check('bad appointment time never matches', g.matchRecordingToCall(mkRecs(), 'a@x.com', NaN), null);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll outcome-loop tests passed.');
