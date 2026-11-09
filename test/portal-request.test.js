// Rules test for portal request alerts (dash Ayuda and Campañas → Slack).
// Run:  node test/portal-request.test.js
const pr = require('../lib/portalRequest');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MENTION = '<!subteam^S0BH988QW1K|@fulfillment>';
const HELP = {
  id: 'help:42',
  kind: 'help',
  clientName: 'Mind Lift',
  label: 'Access or Technical Problem',
  priority: 'High',
  excerpt: 'I cannot log in since this morning.',
  adminUrl: 'https://dash.neurogrowth.io/admin?tab=feedback',
  mode: 'live',
};
const CAMPAIGN = {
  id: 'campaign:r1',
  kind: 'campaign',
  clientName: 'Acme <Corp> & Co',
  label: 'New campaign',
  priority: null,
  excerpt: 'Goal: Book calls. Target: CFOs. Offer: Audit.',
  adminUrl: 'https://dash.neurogrowth.io/admin?tab=campaigns&section=requests',
  mode: 'dry_run',
};

// ── 1. Payload checks (fail closed)
check('1a  help request is accepted', pr.checkRequest(HELP).ok, true);
check('1b  campaign request is accepted', pr.checkRequest(CAMPAIGN).ok, true);
check('1c  unknown kind is refused', pr.checkRequest({ ...HELP, kind: 'other' }).ok, false);
check('1d  missing client is refused', pr.checkRequest({ ...HELP, clientName: '  ' }).ok, false);
check('1e  empty excerpt is refused', pr.checkRequest({ ...HELP, excerpt: '' }).ok, false);
check('1f  a link outside the dash admin is refused', pr.checkRequest({ ...HELP, adminUrl: 'https://evil.example/admin' }).ok, false);
check('1g  a link with Slack control characters is refused', pr.checkRequest({ ...HELP, adminUrl: 'https://dash.neurogrowth.io/admin|click' }).ok, false);
check('1h  missing mode is a dry run', pr.checkRequest({ ...HELP, mode: undefined }).request.mode, 'dry_run');
check('1i  unknown mode is a dry run', pr.checkRequest({ ...HELP, mode: 'LIVE!' }).request.mode, 'dry_run');
check('1j  "-" priority is dropped', pr.checkRequest({ ...HELP, priority: '-' }).request.priority, null);
check('1k  long excerpt is clipped', pr.checkRequest({ ...HELP, excerpt: 'x'.repeat(900) }).request.excerpt.length, pr.MAX_EXCERPT);
check('1l  not an object is refused', pr.checkRequest(null).ok, false);

// ── 2. Text
check('2a  live help request text', pr.formatRequest(pr.checkRequest(HELP).request, { mention: MENTION }), [
  `${MENTION} 🆘 New help request from *Mind Lift*`,
  'Category: Access or Technical Problem · Priority: High',
  '> I cannot log in since this morning.',
  '<https://dash.neurogrowth.io/admin?tab=feedback|Open in admin> · Answer in the portal so the client sees it there.',
].join('\n'));
check('2b  campaign request escapes the client name and has no priority', pr.formatRequest(pr.checkRequest(CAMPAIGN).request), [
  '📣 New campaign request from *Acme &lt;Corp&gt; &amp; Co*',
  'Type: New campaign',
  '> Goal: Book calls. Target: CFOs. Offer: Audit.',
  '<https://dash.neurogrowth.io/admin?tab=campaigns&section=requests|Open in admin> · Answer in the portal so the client sees it there.',
].join('\n'));
check('2c  a multi-line excerpt stays one quoted line',
  pr.formatRequest(pr.checkRequest({ ...HELP, excerpt: 'one\n\ntwo' }).request).split('\n')[2], '> one two');
check('2d  no em dash in the text', /—/.test(pr.formatRequest(pr.checkRequest(HELP).request, { mention: MENTION })), false);

// ── 3. Once per request
let t = 0;
const seen = pr.createSeen(1000, () => t);
check('3a  new id is not seen', seen.has('help:1'), false);
seen.add('help:1');
check('3b  repeated id is seen', seen.has('help:1'), true);
t = 1001;
check('3c  id is forgotten after the ttl', seen.has('help:1'), false);
seen.add('help:2'); seen.delete('help:2');
check('3d  a failed post can be retried', seen.has('help:2'), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall portal request checks passed');
