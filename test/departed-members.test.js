// Rules test for the departed-member registry.
//   Run:  node test/departed-members.test.js
//
// Jonathan Madriz stopped closing on 2026-07-19; Ron flagged it on 2026-08-24,
// the evening before the open-deal sweep's first LIVE run would have DMed him
// three cards. The rules that fell out of that are worth asserting, because
// every one of them is a silent failure if it regresses:
//
//   • Max must never message someone who left (a DM to a departed employee is
//     both useless and a disclosure).
//   • Their name must STILL resolve, or reports covering their tenure render
//     raw GHL ids instead of a person.
//   • Their live follow-up must land on a real, active colleague — coverage
//     pointing at another leaver would silently drop deals on the floor.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const DEPARTED_MEMBERS'),
  SRC.indexOf('// (fetchIClosedIntakeForProspect deleted'),
);
if (!block || block.length < 100) { console.error('FAIL: could not extract the departed-member block'); process.exit(1); }

const { DEPARTED_MEMBERS, DEPARTED_BY_ID, departedMember, CLOSER_SLACK } =
  new Function(`${block}; return { DEPARTED_MEMBERS, DEPARTED_BY_ID, departedMember, CLOSER_SLACK };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── 1. Every shape a person arrives as resolves to the same record ──────────
// GHL hands us a roster email on one row, a raw user id on another, and Slack
// gives a member id — miss any one and a leaver gets a DM through that path.
check('1a roster email', departedMember('jonathan.madriz.neurogrowth@gmail.com')?.name, 'Jonathan Madriz');
check('1b raw GHL user id, lowercase', departedMember('gqymykpddltdxvbkfl2c')?.name, 'Jonathan Madriz');
check('1c raw GHL user id, mixed case', departedMember('gqYMYkpDDlTdxvBkfl2C')?.name, 'Jonathan Madriz');
check('1d Slack id', departedMember('U0APYAE0999')?.name, 'Jonathan Madriz');
check('1e active closers are never flagged',
  [departedMember('jose.neurogrowth@gmail.com'), departedMember('ronny.duarte@neurogrowth.io')], [null, null]);
check('1f unknown ids and junk resolve to null, not a throw',
  [departedMember(''), departedMember(null), departedMember('nobody@example.com')], [null, null, null]);

// ── 2. THE safety property: no DM path can reach a departed member ──────────
// Asserted over the registry rather than one hardcoded id, so adding the next
// leaver to DEPARTED_MEMBERS without pulling them from CLOSER_SLACK fails here.
const closerKeys = new Set(Object.keys(CLOSER_SLACK).map(k => k.toLowerCase()));
const reachable = Object.keys(DEPARTED_BY_ID).filter(id => closerKeys.has(id));
check('2a no departed id appears in CLOSER_SLACK', reachable, []);
const departedSlackIds = new Set(Object.values(DEPARTED_BY_ID).flatMap(r => (r.aliases || []).map(a => String(a).toUpperCase())));
const leakedSlackIds = Object.values(CLOSER_SLACK).filter(id => departedSlackIds.has(String(id).toUpperCase()));
check('2b no departed Slack id is a DM target under any key', leakedSlackIds, []);

// ── 3. Coverage must be a real, active, messageable colleague ───────────────
// A departed SETTER (role 'setter') never appears as a closer_id, so there are
// no open deals to inherit: coverage must be explicitly null, never a guess.
for (const [email, rec] of Object.entries(DEPARTED_MEMBERS)) {
  check(`3e ${rec.name} records a departure date`, /^\d{4}-\d{2}-\d{2}$/.test(rec.since || ''), true);
  if (rec.role === 'setter') { check(`3f ${rec.name} (setter) has coverage null`, rec.coverage, null); continue; }
  check(`3a ${rec.name} names a coverage closer`, typeof rec.coverage === 'string' && rec.coverage.length > 0, true);
  check(`3b ${rec.name}'s coverage is reachable in CLOSER_SLACK`, Boolean(CLOSER_SLACK[rec.coverage]), true);
  check(`3c ${rec.name}'s coverage is not itself departed`, departedMember(rec.coverage), null);
  check(`3d ${rec.name} does not cover themselves`, rec.coverage === email, false);
}

// ── 4. History is preserved, not rewritten ─────────────────────────────────
check('4a the name still resolves for reports covering his tenure',
  /'jonathan\.madriz\.neurogrowth@gmail\.com': 'Jonathan Madriz'/.test(SRC), true);
check('4b the raw GHL id fallback still names him too',
  /'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz'/.test(SRC), true);

// ── 5. Off the roster — a live Slack account cannot query company data ──────
const rosterBlock = SRC.slice(SRC.indexOf('const TEAM_MEMBERS'), SRC.indexOf('const ROLE_PERMISSIONS'));
check('5a no departed Slack id remains in TEAM_MEMBERS',
  /U0APYAE0999':\s*\{/.test(rosterBlock), false);
check('5b and the removal is explained where the next reader will look',
  /departed 2026-07-19/.test(rosterBlock), true);

// ── 6. The prompt must not present him as current ──────────────────────────
const promptBlock = SRC.slice(0, SRC.indexOf('HOW YOU OPERATE'));
check('6a the roster line no longer calls him a closer',
  /Jonathan Madriz \(U0APYAE0999\) — High-Ticket Closer/.test(promptBlock), false);
check('6b it states he is former and must not be assigned work',
  /FORMER closer, departed 2026-07-19/.test(promptBlock), true);

// ── 7. William B, setter, departed 2026-10-07 ──────────────────────────────
// Ron, 2026-10-07: the active setters are Sebastian and Oscar. Same rules as
// above, plus the setter-specific ACTION maps (claims, nudges, standup DMs).
check('7a roster email resolves', departedMember('william.neurogrowth@gmail.com')?.name, 'William B');
check('7b raw GHL id resolves (mixed case)', departedMember('N8mvtuHbbbY7QppqNMr7')?.name, 'William B');
check('7c Slack id resolves', departedMember('U0B16P6DQ2F')?.name, 'William B');
check('7d no longer in TEAM_MEMBERS', /U0B16P6DQ2F':\s*\{/.test(rosterBlock), false);
check('7e prompt no longer calls him a current setter',
  /William B \(U0B16P6DQ2F\) — Appointment Setter/.test(promptBlock), false);
check('7f prompt states he is former', /FORMER appointment setter, departed 2026-10-07/.test(promptBlock), true);
check('7g name maps still name him for history',
  /'N8mvtuHbbbY7QppqNMr7': 'William B'/.test(SRC) && /'william\.neurogrowth@gmail\.com':\s+'William B'/.test(SRC), true);
const sliceConst = (name) => {
  const start = SRC.indexOf(`const ${name} = {`);
  if (start < 0) return null;
  return SRC.slice(start, SRC.indexOf('};', start));
};
for (const m of ['GHL_TO_SLACK', 'SLACK_TO_GHL_USER', 'EMAIL_TO_GHL_USER_ID', 'ghlUserNames', 'HOT_REPLY_SETTERS']) {
  const body = sliceConst(m);
  check(`7h ${m} exists`, Boolean(body), true);
  check(`7i ${m} has no William id, Slack id or email`,
    /n8mvtuhbbby7qppqnmr7|U0B16P6DQ2F|william/i.test((body || '').replace(/\/\/.*$/gm, '')), false);
}
const setterDm = SRC.slice(SRC.indexOf('// ── DM each setter'), SRC.indexOf('];', SRC.indexOf('// ── DM each setter')));
check('7j standup setter DM list skips him', /U0B16P6DQ2F/.test(setterDm), false);

console.log(failures ? `\n${failures} failure(s).` : '\nAll departed-member checks passed.');
process.exit(failures ? 1 : 0);
