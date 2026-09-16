// Rules test for the closer → Slack id mapping.
//   Run:  node test/closer-slack-mapping.test.js
//
// Jose's GHL identity moved from jose.neurogrowth@gmail.com to
// jose.carranza@neurogrowth.io in late August 2026. Nothing failed: every DM
// loop resolves a Slack id or `continue`s, so from the week of 2026-08-31 the
// majority of his calls were counted by the nightly outcome cron and nudged to
// nobody. Appointments aa350776 and 55cd1223 reached an `outcome-reminder`
// count of 10 with no card ever sent, and outcome coverage for calls held after
// 2026-09-05 fell to 43%.
//
// The properties below are each a silent outage if they regress:
//   * Every closer_id the portal actually emits must resolve to a Slack id.
//   * A roster rename must be fixable without a deploy (CLOSER_SLACK_EXTRA).
//   * That escape hatch must not be able to resurrect a departed member.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const DEPARTED_MEMBERS'),
  SRC.indexOf('// (fetchIClosedIntakeForProspect deleted'),
);
if (!block || block.length < 100) { console.error('FAIL: could not extract the closer-mapping block'); process.exit(1); }

// The block ends with reportUnmappedClosers, which closes over `slack` and
// `RON_SLACK_ID`. It is only declared here, never called, so stubs keep the
// parse honest without pulling in the Slack client.
function build(extraEnv) {
  const factory = new Function('process', 'console', 'slack', 'RON_SLACK_ID',
    `${block}; return { CLOSER_SLACK, departedMember, reportUnmappedClosers };`);
  const warnings = [];
  return factory(
    { env: { CLOSER_SLACK_EXTRA: extraEnv } },
    { ...console, warn: (m) => warnings.push(String(m)) },
    { client: { chat: { postMessage: async () => {} } } },
    'U05HXGX18H3',
  );
}

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const base = build(undefined);
const JOSE = 'U0AMTEKDCPN';

// --- 1. THE regression: both of Jose's identities reach the same person ---
check('1a company-domain id resolves (the 2026-08-31 outage)', base.CLOSER_SLACK['jose.carranza@neurogrowth.io'], JOSE);
check('1b legacy gmail id still resolves (older rows)',        base.CLOSER_SLACK['jose.neurogrowth@gmail.com'], JOSE);
check('1c both identities are the same human',
  base.CLOSER_SLACK['jose.carranza@neurogrowth.io'] === base.CLOSER_SLACK['jose.neurogrowth@gmail.com'], true);

// --- 2. Every closer_id the portal emits must be mappable ---
// Observed in revops_appointments over the 70 days to 2026-09-15. A value here
// that stops resolving means that closer silently stops being asked.
const OBSERVED_CLOSER_IDS = [
  'jose.carranza@neurogrowth.io',
  'jose.neurogrowth@gmail.com',
  'ronny.duarte@neurogrowth.io',
];
const resolve = (m, id) => m[id] || m[String(id || '').toLowerCase()];
const unmapped = OBSERVED_CLOSER_IDS.filter(id => !resolve(base.CLOSER_SLACK, id));
check('2a no observed closer_id is unmapped', unmapped, []);
// Jonathan is observed in rows before 2026-07-19 and must stay unreachable.
check('2b a departed closer stays unmapped', Boolean(resolve(base.CLOSER_SLACK, 'jonathan.madriz.neurogrowth@gmail.com')), false);

// --- 3. A rename is a config change, not a deploy ---
const withExtra = build('jose.newname@neurogrowth.io:U0AMTEKDCPN, someone.else@neurogrowth.io:U0NEWPERSON');
check('3a env mapping is applied',            resolve(withExtra.CLOSER_SLACK, 'jose.newname@neurogrowth.io'), JOSE);
check('3b a second pair is applied',          resolve(withExtra.CLOSER_SLACK, 'someone.else@neurogrowth.io'), 'U0NEWPERSON');
check('3c mixed case resolves either way',    resolve(withExtra.CLOSER_SLACK, 'Jose.NewName@neurogrowth.io'), JOSE);
check('3d hardcoded entries survive',         resolve(withExtra.CLOSER_SLACK, 'jose.carranza@neurogrowth.io'), JOSE);

// --- 4. The escape hatch cannot undo a departure ---
// Rule 2 of DEPARTED_MEMBERS: Max never messages them again. An env var is not
// allowed to override a safety property.
const withLeaver = build('jonathan.madriz.neurogrowth@gmail.com:U0APYAE0999');
check('4a a departed roster email is refused', Boolean(resolve(withLeaver.CLOSER_SLACK, 'jonathan.madriz.neurogrowth@gmail.com')), false);
const withLeaverAlias = build('gqymykpddltdxvbkfl2c:U0APYAE0999');
check('4b a departed raw GHL id is refused too', Boolean(resolve(withLeaverAlias.CLOSER_SLACK, 'gqymykpddltdxvbkfl2c')), false);

// --- 5. Junk in the env never throws and never half-applies ---
const junk = build('  , :U0BAD, noslackid:, , malformed, ok@ng.io:U0FINE ,');
check('5a a valid pair among junk still applies', resolve(junk.CLOSER_SLACK, 'ok@ng.io'), 'U0FINE');
check('5b an id with no Slack id is skipped',     Boolean(resolve(junk.CLOSER_SLACK, 'noslackid')), false);
check('5c a bare token is skipped',               Boolean(resolve(junk.CLOSER_SLACK, 'malformed')), false);
check('5d empty env leaves the map untouched',
  Object.keys(build('').CLOSER_SLACK).length, Object.keys(base.CLOSER_SLACK).length);

// --- 5b. Anyone Max DMs as a closer must also have a report name ---
// CLOSER_SLACK and SALES_TEAM_MAP are separate maps. On 2026-09-16 the
// company-domain address was added to the first but not the second, so Jose's
// cards were delivered while every leaderboard split him into "Jose Carranza"
// and a raw email row.
{
  const tStart = SRC.indexOf('const SALES_TEAM_MAP');
  const tEnd = SRC.indexOf('};', tStart);
  const SALES_TEAM_MAP = new Function(`${SRC.slice(tStart, tEnd + 2)}; return SALES_TEAM_MAP;`)();
  const nameless = Object.keys(base.CLOSER_SLACK)
    .filter(k => k.includes('@'))
    .filter(k => !(SALES_TEAM_MAP[k] || SALES_TEAM_MAP[k.toLowerCase()]));
  check('5e every closer email Max DMs resolves to a report name', nameless, []);
  check('5f both of Jose\'s identities report as one person',
    SALES_TEAM_MAP['jose.carranza@neurogrowth.io'], SALES_TEAM_MAP['jose.neurogrowth@gmail.com']);
}

// --- 6. The unmapped-closer alert only fires on real misses ---
// A departed member is an EXPECTED miss. Reporting it would train Ron to
// ignore the alert, which is how the gap stayed invisible in the first place.
(async () => {
  const sent = [];
  const factory = new Function('process', 'console', 'slack', 'RON_SLACK_ID',
    `${block}; return { reportUnmappedClosers };`);
  const { reportUnmappedClosers } = factory(
    { env: {} }, console,
    { client: { chat: { postMessage: async (a) => { sent.push(a); } } } },
    'U05HXGX18H3',
  );

  await reportUnmappedClosers('test', {});
  check('6a no misses sends nothing', sent.length, 0);

  await reportUnmappedClosers('test', { 'jonathan.madriz.neurogrowth@gmail.com': 3 });
  check('6b a departed-only miss sends nothing', sent.length, 0);

  await reportUnmappedClosers('test', { 'newguy@neurogrowth.io': 4, 'jonathan.madriz.neurogrowth@gmail.com': 3 });
  check('6c a real miss alerts Ron', sent.length, 1);
  check('6d the alert names the unmapped id', sent[0].text.includes('newguy@neurogrowth.io'), true);
  check('6e the alert counts what went unnudged', sent[0].text.includes('4 item(s)'), true);
  check('6f the alert excludes the departed member', sent[0].text.includes('jonathan'), false);
  check('6g the alert names the no-deploy fix', sent[0].text.includes('CLOSER_SLACK_EXTRA'), true);

  if (failures) { console.error(`\n${failures} check(s) failed.`); process.exit(1); }
  console.log('\nAll closer-slack-mapping checks passed.');
})();
