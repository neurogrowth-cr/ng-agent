// Rules test for the ops-lead seat (Josué left 2026-11-12; Gerald is ops lead
// designate since 2026-11-03, Ron's decision).
//   Run:  node test/ops-lead-routing.test.js
//
// Max routes several outputs by role, not by name: the tech_ops persona and the
// proactive flags go to whoever holds `tech_ops` (the 09:00 ops brief and the
// Monday WBR reminder post to OPS_BRIEF_CHANNEL since #417, guarded by
// test/ops-brief-channel.test.js). Gerald holds the role through the `roles`
// list, so his primary `campaigns` role, persona and permissions do not change.
// Making `tech_ops` his primary role (with `roles: ['campaigns']`) is a separate
// decision of Ron's: it widens his permissions. Section 4 must keep passing
// unchanged either way: Phase 1 and the closes stay with Valeria.
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
check('1a tech_ops outputs reach Gerald alone since Josue left', slackIdsByRole('tech_ops'), [GERALD]);
check('1b Gerald keeps campaigns as primary role', TEAM_MEMBERS[GERALD].role, 'campaigns');
check('1c campaigns brief still reaches Gerald', slackIdsByRole('campaigns'), [GERALD]);
check('1d a role nobody holds resolves to an empty list, not a throw', slackIdsByRole('client_success'), []);
check('1e Valeria alone holds fulfillment (Kai escalations owner)', slackIdsByRole('fulfillment'), [VALERIA]);

// ── 2. Copy that named Josue follows the seat ───────────────────────────────
check('2a opsLeadLabel names the one holder', opsLeadLabel(), 'Gerald');
check('2b no hardcoded "Let Josue know" left in the standup copy', /Let Josue know/.test(SRC), false);
check('2c no hardcoded "so Josue can sequence" left', /so Josue can sequence/.test(SRC), false);
check('2d WBR reminder is not pinned to one Slack id', /const WBR_REMINDER_TO = OPS_BRIEF_CHANNEL;/.test(SRC) && !/const WBR_REMINDER_TO = 'U/.test(SRC), true);

// ── 3. Deactivated accounts are not DM targets ───────────────────────────────
check('3a Tania (deactivated Slack user) no longer in TEAM_MEMBERS', new RegExp(`${TANIA}':\\s*\\{`).test(rosterBlock), false);

// ── 3b. Josue left 2026-11-12: off the roster, on the departed list, no DM fallback
const departedBlock = SRC.slice(SRC.indexOf('const DEPARTED_MEMBERS'), SRC.indexOf('const DEPARTED_BY_ID'));
check('3b Josue no longer in TEAM_MEMBERS', new RegExp(`'${JOSUE}':\\s*\\{`).test(rosterBlock), false);
check('3c Josue is a departed member, by email, Slack id and GHL user id',
  /'josue\.duran@neurogrowth\.io': \{[\s\S]*?since: '2026-11-12'[\s\S]*?aliases: \['U08ABBFNGUW', 't28sdyo0eaunhjhl4jyu'\]/.test(departedBlock), true);
check('3d no code path DMs his Slack id as a fallback', /\[\s*'U08ABBFNGUW'\s*\]/.test(SRC.replace(departedBlock, '')), false);
check('3e the roster prompt tells Max never to tag him', /never tag, DM or assign anything to Josue/.test(SRC), true);
check('3f the standup no longer tags him', /Tag Josue, Valeria, Gerald/.test(SRC), false);

// ── 4. Phase ownership does not follow the ops-lead seat (Ron, 2026-11-03) ──
// "phase one owner is Valeria. phase 2 owner is Gerald. QA is going to be done
// by RON." Friday delta owner tags and the keep-or-close deciders route by the
// phase owner's role, so the 12-01 flip of tech_ops to Gerald does not move
// Phase 1 or the closes onto him.
const fridayBlock = SRC.slice(SRC.indexOf('const FRIDAY_OWNER_ROLE'), SRC.indexOf('function fridayUpcoming'));
if (fridayBlock.length < 50) { console.error('FAIL: could not extract the Friday owner block'); process.exit(1); }
const { FRIDAY_OWNER_ROLE, fridayOwnersFrom, fridayOwnerTag } =
  new Function(`const FRIDAY_WINDOW_DAYS = 14, FRIDAY_AT_RISK_DAYS = 7, FRIDAY_LAUNCH_MEMORY_DAYS = 28; ${fridayBlock}; return { FRIDAY_OWNER_ROLE, fridayOwnersFrom, fridayOwnerTag };`)();
const owners = fridayOwnersFrom(TEAM_MEMBERS);
check('4a Friday Phase 1 owner role is fulfillment', FRIDAY_OWNER_ROLE.phase_1, 'fulfillment');
check('4b Friday Phase 2 owner role is campaigns', FRIDAY_OWNER_ROLE.phase_2, 'campaigns');
check('4c Friday Phase 1 lines tag Valeria', fridayOwnerTag('phase_1', owners), ` Owner: <@${VALERIA}>.`);
check('4d Friday Phase 2 lines tag Gerald', fridayOwnerTag('phase_2', owners), ` Owner: <@${GERALD}>.`);
check('4e Friday blocked lines tag Valeria (client-side holds sit with Phase 0/1)', fridayOwnerTag('blocked', owners), ` Owner: <@${VALERIA}>.`);
check('4f keep-or-close deciders are Ron plus the fulfillment role', /deciders: \[RON_SLACK_ID, \.\.\.slackIdsByRole\('fulfillment'\)\]/.test(SRC), true);
check('4g no hardcoded "Flag Josue" left in the proactive copy', /Flag Josue/.test(SRC), false);
check('4h the roster tells Max the pre-launch QA is Ron\'s', /pre-launch campaign QA \(the go\/no-go\)/.test(SRC), true);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
