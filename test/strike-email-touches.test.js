// Emails sent through Max count as strike-mover touches.  Run:  node test/strike-email-touches.test.js
//
// Same extract-and-eval approach as strike-rules.test.js: mergeEmailTouches and
// evaluateStrikeMove are pulled straight out of index.js so the test can never
// drift from shipped behaviour.
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
const { evaluateStrikeMove, mergeEmailTouches } =
  new Function(`${pure}; return { evaluateStrikeMove, mergeEmailTouches };`)();

const PIPE = 'KH1IQuaN8aNB1lfRpvP4';
const S = {
  IC: '4b936528-794e-40ab-812d-144b9d5e8128',
  S1: '92245916-0622-4f46-aabc-6091b8af5fc0',
};
const opp = (stage, lastStageChangeAt) => ({
  id: 'opp', status: 'open', pipelineId: PIPE, pipelineStageId: stage, lastStageChangeAt,
});
const wa  = (dir, ts, source = 'app') => ({ messageType: 'TYPE_WHATSAPP', direction: dir, source, dateAdded: ts });
const act = (ts) => ({ messageType: 'TYPE_ACTIVITY_OPPORTUNITY', direction: 'outbound', source: 'app', dateAdded: ts, body: 'Opportunity updated' });

const NOW = Date.parse('2026-09-22T22:00:00.000Z');
const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// ── mergeEmailTouches ────────────────────────────────────────────────────────
const ghl = [wa('outbound', '2026-09-18T10:00:00.000Z'), act('2026-09-18T10:05:00.000Z')];

check('no thread returns the GHL messages untouched',
  mergeEmailTouches(ghl, null) === ghl, true);

check('thread with neither timestamp returns the GHL messages untouched',
  mergeEmailTouches(ghl, { last_outbound_at: null, last_inbound_at: null }) === ghl, true);

check('synthetic email entries are merged oldest-first among GHL messages',
  mergeEmailTouches(ghl, { last_outbound_at: '2026-09-17T09:00:00.000Z', last_inbound_at: '2026-09-19T12:00:00.000Z' })
    .map(m => `${m.messageType}:${m.direction}:${m.dateAdded}`),
  [
    'TYPE_EMAIL:outbound:2026-09-17T09:00:00.000Z',
    'TYPE_WHATSAPP:outbound:2026-09-18T10:00:00.000Z',
    'TYPE_ACTIVITY_OPPORTUNITY:outbound:2026-09-18T10:05:00.000Z',
    'TYPE_EMAIL:inbound:2026-09-19T12:00:00.000Z',
  ]);

check('synthetic entries carry source app and the max_email marker',
  mergeEmailTouches([], { last_outbound_at: '2026-09-20T09:00:00.000Z', last_inbound_at: null })[0],
  { messageType: 'TYPE_EMAIL', direction: 'outbound', source: 'app', dateAdded: '2026-09-20T09:00:00.000Z', via: 'max_email' });

// ── end-to-end through evaluateStrikeMove ────────────────────────────────────
check('Max email is the only touch: IC → Strike 1 (never replied)',
  evaluateStrikeMove(opp(S.IC, '2026-09-18T12:00:00.000Z'), mergeEmailTouches(
    [act('2026-09-18T12:00:00.000Z')],
    { last_outbound_at: '2026-09-20T15:00:00.000Z', last_inbound_at: null },
  ), NOW), { move: S.S1, reason: 'chase — lead never replied' });

check('lead replied to the Max email after it was sent: left alone',
  evaluateStrikeMove(opp(S.IC, '2026-09-18T12:00:00.000Z'), mergeEmailTouches(
    [act('2026-09-18T12:00:00.000Z')],
    { last_outbound_at: '2026-09-20T15:00:00.000Z', last_inbound_at: '2026-09-21T09:00:00.000Z' },
  ), NOW).skip, 'lead_spoke_last');

check('email reply an hour ago protects a WhatsApp follow-up',
  evaluateStrikeMove(opp(S.IC, '2026-09-18T12:00:00.000Z'), mergeEmailTouches(
    [wa('outbound', '2026-09-22T21:30:00.000Z')],
    { last_outbound_at: '2026-09-20T15:00:00.000Z', last_inbound_at: '2026-09-22T21:00:00.000Z' },
  ), NOW).skip, 'lead_engaged');

check('Max email older than the last stage change is not re-counted',
  evaluateStrikeMove(opp(S.IC, '2026-09-21T12:00:00.000Z'), mergeEmailTouches(
    [],
    { last_outbound_at: '2026-09-20T15:00:00.000Z', last_inbound_at: null },
  ), NOW).skip, 'already_counted');

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
