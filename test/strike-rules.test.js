// Rules test for the auto strike mover.  Run:  node test/strike-rules.test.js
//
// Extracts the real evaluateStrikeMove + its constants straight out of index.js
// rather than copying them, so the test can never drift from shipped behaviour.
// index.js boots the Slack app on require, hence the extract-and-eval approach.
//
// Every fixture below is shaped from message payloads captured live from GHL on
// 2026-07-28 — including the two rules that read as obviously correct and failed
// on contact with real data (see cases 1 and 2).
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const STRIKE_PIPELINE_ID'),
  SRC.indexOf('// Sweep — see cron registration below.'),
);
const pure = block
  .replace(/async function ghlFetchJson[\s\S]*?\n}\n/, '')
  .replace(/async function ghlSearchOppsByStage[\s\S]*?\n}\n/, '')
  .replace(/async function ghlFindConversationId[\s\S]*?\n}\n/, '')
  .replace(/async function ghlMoveOpportunityStage[\s\S]*?\n}\n/, '');
const evaluateStrikeMove = new Function(`${pure}; return evaluateStrikeMove;`)();

const PIPE = 'KH1IQuaN8aNB1lfRpvP4';
const S = {
  NL: '93de6a09-78a4-4253-bea4-c1528ed6f2b3',
  IC: '4b936528-794e-40ab-812d-144b9d5e8128',
  S1: '92245916-0622-4f46-aabc-6091b8af5fc0',
  S2: '6274c958-8252-4698-acc6-c6818d43a99f',
  S3: 'e639662d-6b1b-42b5-a89d-7ebd70ca97e3',
};
const opp = (stage, lastStageChangeAt) => ({
  id: 'opp', status: 'open', pipelineId: PIPE, pipelineStageId: stage, lastStageChangeAt,
});
const wa  = (dir, ts, source = 'app') => ({ messageType: 'TYPE_WHATSAPP', direction: dir, source, dateAdded: ts });
const act = (ts) => ({ messageType: 'TYPE_ACTIVITY_OPPORTUNITY', direction: 'outbound', source: 'app', dateAdded: ts, body: 'Opportunity updated' });

const NOW = Date.parse('2026-07-28T21:30:00.000Z');
const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// 1. REAL: test card in Strike 1 (moved 20:39:17); its newest WhatsApp is 20:06:44,
//    older than the stage change. This is the double-count that wrongly pushed a
//    live lead (Oscar Orozco) to Strike 1 after his setter had dragged the card.
check('watermark blocks re-counting the same message',
  evaluateStrikeMove(opp(S.S1, '2026-07-28T20:39:17.025Z'), [
    wa('outbound', '2026-07-28T19:48:05.811Z'),
    wa('outbound', '2026-07-28T20:06:33.000Z'),
    wa('outbound', '2026-07-28T20:06:44.000Z'),
    act('2026-07-28T20:39:17.350Z'),
  ], NOW).skip, 'already_counted');

// 2. REAL: Sandra Gonzalez — setter double-texted at 20:04/20:05 after her 19:47
//    reply. A "previous message was outbound ⇒ chase" rule marches her to Strike 3.
check('engaged lead is protected from double-text chase',
  evaluateStrikeMove(opp(S.IC, '2026-07-26T10:00:00.000Z'), [
    wa('outbound', '2026-07-28T19:37:32.000Z'),
    wa('outbound', '2026-07-28T19:38:13.000Z'),
    wa('inbound',  '2026-07-28T19:47:46.480Z'),
    wa('outbound', '2026-07-28T20:04:40.000Z'),
    wa('outbound', '2026-07-28T20:05:14.000Z'),
  ], NOW).skip, 'lead_engaged');

// 3. REAL: José Luis — lead replied 08:06, setter answered twice at 13:18.
check('same protection on the José Luis thread',
  evaluateStrikeMove(opp(S.IC, '2026-07-26T09:00:00.000Z'), [
    wa('outbound', '2026-07-28T07:59:46.812Z', 'workflow'),
    wa('inbound',  '2026-07-28T08:06:50.579Z'),
    wa('outbound', '2026-07-28T13:18:07.635Z'),
    wa('outbound', '2026-07-28T13:18:29.339Z'),
  ], NOW).skip, 'lead_engaged');

check('genuine chase advances IC → Strike 1',
  evaluateStrikeMove(opp(S.IC, '2026-07-25T12:00:00.000Z'), [
    wa('inbound',  '2026-07-24T10:00:00.000Z'),
    wa('outbound', '2026-07-28T21:00:00.000Z'),
  ], NOW), { move: S.S1, reason: 'chase — lead silent 24h+' });

check('never-replied lead advances S2 → Strike 3',
  evaluateStrikeMove(opp(S.S2, '2026-07-25T12:00:00.000Z'), [
    wa('outbound', '2026-07-26T09:00:00.000Z', 'workflow'),
    wa('outbound', '2026-07-28T21:00:00.000Z'),
  ], NOW), { move: S.S3, reason: 'chase — lead never replied' });

// REAL: the automated First Message workflow send carries source 'workflow'.
check('automated workflow send is ignored',
  evaluateStrikeMove(opp(S.IC, '2026-07-20T12:00:00.000Z'), [
    wa('outbound', '2026-07-28T19:25:07.335Z', 'workflow'),
  ], NOW).skip, 'automated_send');

check('New Lead → Initial Contact on first human touch',
  evaluateStrikeMove(opp(S.NL, '2026-07-28T19:23:05.393Z'), [
    wa('outbound', '2026-07-28T19:25:07.335Z', 'workflow'),
    wa('outbound', '2026-07-28T20:13:34.000Z'),
  ], NOW), { move: S.IC, reason: 'first human touch' });

// Without the messageType filter the mover reads its own stage-change activity
// as the newest "message" in the thread.
check('stage-change activity is not mistaken for a message',
  evaluateStrikeMove(opp(S.IC, '2026-07-25T12:00:00.000Z'), [
    wa('outbound', '2026-07-28T21:00:00.000Z'),
    act('2026-07-28T21:10:00.000Z'),
  ], NOW), { move: S.S1, reason: 'chase — lead never replied' });

check('debounce blocks a second advance within 20h',
  evaluateStrikeMove(opp(S.IC, '2026-07-28T18:30:00.000Z'), [
    wa('outbound', '2026-07-28T21:00:00.000Z'),
  ], NOW).skip, 'debounced');

check('Strike 3 is the ceiling',
  evaluateStrikeMove(opp(S.S3, '2026-07-25T12:00:00.000Z'), [
    wa('outbound', '2026-07-28T21:00:00.000Z'),
  ], NOW).skip, 'stage_out_of_scope');

check('lead spoke last is left alone',
  evaluateStrikeMove(opp(S.IC, '2026-07-25T12:00:00.000Z'), [
    wa('outbound', '2026-07-28T20:00:00.000Z'),
    wa('inbound',  '2026-07-28T20:30:00.000Z'),
  ], NOW).skip, 'lead_spoke_last');

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
