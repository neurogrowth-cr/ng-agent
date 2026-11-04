// Rules test for the ops-lead handover (Josué leaves 2026-12-01; Gerald is ops
// lead designate since 2026-11-03, Ron's decision).
//   Run:  node test/ops-lead-routing.test.js
//
// Max routes several outputs by role, not by name: the keep-or-close deciders,
// the tech_ops persona and the proactive flags go to whoever holds `tech_ops`
// (the 09:00 ops brief and the Monday WBR reminder post to OPS_BRIEF_CHANNEL
// since #417, guarded by test/ops-brief-channel.test.js). During the handover
// BOTH Josué and Gerald must hold the role (Gerald through the new `roles`
// list, so his primary `campaigns` role, persona and permissions do not change).
// On 2026-12-01: move Josué to DEPARTED_MEMBERS, make Gerald's primary role
// `tech_ops` with `roles: ['campaigns']`, and update section 2 below.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const rosterBlock = SRC.slice(SRC.indexOf('const TEAM_MEMBERS'), SRC.indexOf('const ROLE_PERMISSIONS'));
const helperBlock = SRC.slice(SRC.indexOf('function slackIdsByRole'), SRC.indexOf('function buildRoleSystemPrompt'));
if (rosterBlock.length < 100 || helperBlock.length < 50) { console.error('FAIL: could not extract the roster or helper block'); process.exit(1); }

const { TEAM_MEMBERS, slackIdsByRole, opsLeadLabel } =
  new Function(`${rosterBlock}; ${helperBlock}; return { TEAM_MEMBERS, slackIdsByRole, opsLeadLabel };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const JOSUE = 'U08ABBFNGUW', GERALD = 'U0BAAC0KS82', VALERIA = 'U09Q3BXJ18B', TANIA = 'U07SMMDMSLQ';

// ── 1. Secondary roles reach the role lists without changing the primary ────
check('1a tech_ops outputs reach Josue and Gerald during the handover', slackIdsByRole('tech_ops').sort(), [JOSUE, GERALD].sort());
check('1b Gerald keeps campaigns as primary role', TEAM_MEMBERS[GERALD].role, 'campaigns');
check('1c campaigns brief still reaches Gerald', slackIdsByRole('campaigns'), [GERALD]);
check('1d a role nobody holds resolves to an empty list, not a throw', slackIdsByRole('client_success'), []);
check('1e Valeria alone holds fulfillment (Kai escalations owner)', slackIdsByRole('fulfillment'), [VALERIA]);

// ── 2. Copy that named Josue follows the seat ───────────────────────────────
check('2a opsLeadLabel names both holders', opsLeadLabel(), 'Josue or Gerald');
check('2b no hardcoded "Let Josue know" left in the standup copy', /Let Josue know/.test(SRC), false);
check('2c no hardcoded "so Josue can sequence" left', /so Josue can sequence/.test(SRC), false);
check('2d WBR reminder is not pinned to one Slack id', /const WBR_REMINDER_TO = OPS_BRIEF_CHANNEL;/.test(SRC) && !/const WBR_REMINDER_TO = 'U/.test(SRC), true);

// ── 3. Deactivated accounts are not DM targets ───────────────────────────────
check('3a Tania (deactivated Slack user) no longer in TEAM_MEMBERS', new RegExp(`${TANIA}':\\s*\\{`).test(rosterBlock), false);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
