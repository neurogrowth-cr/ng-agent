// Rules test for the weekly provisioning-lag nag (runProvisioningLagCheck).
//   Run:  node test/provisioning-lag.test.js
//
// Extracts the real pure block (classifyProvisioningLag + formatter + consts)
// straight out of index.js so the test cannot drift from shipped behaviour —
// same approach as stale-lead-sweep.test.js (index.js boots Slack on require).
//
// The fixture names are the real motivating cases: Josstinne Montaño's $3.5k
// win sat unprovisioned for weeks with nothing watching (found 2026-07-27),
// and Aura Bonilla was provisioned under a different email than she booked
// with (found 2026-08-20).
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const PROVISIONING_BRIDGE_LIVE_AT'),
  SRC.indexOf('// ─── end provisioning-lag pure block'),
);
if (!block) { console.error('FAIL: could not extract the provisioning-lag block'); process.exit(1); }

const { classifyProvisioningLag, formatProvisioningLagMessage, PROVISIONING_LAG_DAYS, PROVISIONING_LAG_SANITY_MAX } =
  new Function(`${block}; return { classifyProvisioningLag, formatProvisioningLagMessage, PROVISIONING_LAG_DAYS, PROVISIONING_LAG_SANITY_MAX };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const NOW = Date.parse('2026-08-24T14:30:00Z'); // a Monday 8:30 CR
const daysAgo = (d) => new Date(NOW - d * 86400000).toISOString();

// ── 1. Bucketing rules ───────────────────────────────────────────────────────
const rows = [
  // Overdue, no dashboard at all → lagging.
  { prospect_id: 'p1', full_name: 'Josstinne Montaño & Socios', email: 'josstinne.montano@gmail.com', closer: 'Ron Duarte', won_at: daysAgo(12), linked: false, dashboard_exists: false },
  // Overdue, dashboard exists under her email but never linked → bridge miss.
  { prospect_id: 'p2', full_name: 'Aura Bonilla Bravo', email: 'auris144@gmail.com', closer: 'Ron Duarte', won_at: daysAgo(9), linked: false, dashboard_exists: true },
  // Won 3 days ago → inside the grace window, not nagged.
  { prospect_id: 'p3', full_name: 'Fresh Win', email: 'fresh@x.com', closer: 'Jose Carranza', won_at: daysAgo(3), linked: false, dashboard_exists: false },
  // Linked → healthy regardless of age.
  { prospect_id: 'p4', full_name: 'Johan Solis', email: 'johan.sb0211@gmail.com', closer: 'Jose Carranza', won_at: daysAgo(20), linked: true, dashboard_exists: true },
  // Name missing → email is the display; closer missing → 'unassigned'.
  { prospect_id: 'p5', full_name: '  ', email: 'noname@x.com', closer: '', won_at: daysAgo(8), linked: false, dashboard_exists: false },
  // Garbage date → dropped, not NaN'd into the report.
  { prospect_id: 'p6', full_name: 'Bad Date', email: 'bad@x.com', closer: 'Ron', won_at: 'not-a-date', linked: false, dashboard_exists: false },
];
const b = classifyProvisioningLag(rows, NOW);
check('1a lagging bucket', b.lagging.map(e => e.display), ['Josstinne Montaño & Socios', 'noname@x.com']);
check('1b lagging sorted oldest-first', b.lagging.map(e => e.days), [12, 8]);
check('1c bridge-miss bucket', b.unlinked.map(e => e.display), ['Aura Bonilla Bravo']);
check('1d fresh count', b.fresh, 1);
check('1e closer fallback', b.lagging[1].closer, 'unassigned');
check('1f grace window is 7d', PROVISIONING_LAG_DAYS, 7);

// ── 2. Message structural contract (criteria 4a of the recipe) ───────────────
const msg = formatProvisioningLagMessage(b);
check('2a header literal', msg.includes('*PROVISIONING LAG — won deals with no client dashboard*'), true);
check('2b bridge-miss header literal', msg.includes('*BRIDGE MISS — dashboard exists but was never linked to the sale*'), true);
const bullets = msg.split('\n').filter(l => l.startsWith('• '));
check('2c every bullet matches the contract',
  bullets.every(l => /^• .+ — won \d{4}-\d{2}-\d{2} \(\d+d ago\) — closer .+$/.test(l)), true);
check('2d bullet count', bullets.length, 3);
check('2e no undefined/null in output', /undefined|(?<![a-z])null(?![a-z])/.test(msg), false);
check('2f action footer present', msg.includes('Revenue Handoff'), true);

// ── 3. Silent when healthy ───────────────────────────────────────────────────
check('3a all linked → null', formatProvisioningLagMessage(classifyProvisioningLag(
  [{ prospect_id: 'p', full_name: 'X', email: 'x@x.com', closer: 'Ron', won_at: daysAgo(30), linked: true, dashboard_exists: true }], NOW)), null);
check('3b all fresh → null', formatProvisioningLagMessage(classifyProvisioningLag(
  [{ prospect_id: 'p', full_name: 'X', email: 'x@x.com', closer: 'Ron', won_at: daysAgo(2), linked: false, dashboard_exists: false }], NOW)), null);
check('3c empty input → null', formatProvisioningLagMessage(classifyProvisioningLag([], NOW)), null);

// ── 4. Sanity bound fails closed (criteria 4b) ───────────────────────────────
const flood = Array.from({ length: PROVISIONING_LAG_SANITY_MAX + 1 }, (_, i) => ({
  prospect_id: `f${i}`, full_name: `Flood ${i}`, email: `f${i}@x.com`, closer: 'Ron',
  won_at: daysAgo(10), linked: false, dashboard_exists: false,
}));
const floodMsg = formatProvisioningLagMessage(classifyProvisioningLag(flood, NOW));
check('4a sanity message fires', floodMsg.includes('SANITY BOUND EXCEEDED'), true);
check('4b sanity message names no prospects', floodMsg.includes('Flood'), false);
check('4c at the bound it still lists', formatProvisioningLagMessage(classifyProvisioningLag(flood.slice(0, PROVISIONING_LAG_SANITY_MAX), NOW)).split('\n').filter(l => l.startsWith('• ')).length, PROVISIONING_LAG_SANITY_MAX);

process.exit(failures ? 1 : 0);
