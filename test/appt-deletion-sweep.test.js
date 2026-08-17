// Rules test for the GHL appointment deletion sweep.  Run:  node test/appt-deletion-sweep.test.js
//
// Extracts the real classifyGhlApptProbe function straight out of index.js rather
// than copying it, so the test can never drift from shipped behaviour (same
// approach as make-watchdog.test.js — index.js boots the Slack app on require).
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('function classifyGhlApptProbe'),
  SRC.indexOf('async function runApptDeletionSweep'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const classifyGhlApptProbe = new Function(`${block}; return classifyGhlApptProbe;`)();

// ── Deletion signals ────────────────────────────────────────────────────────
check('404 is a deletion', classifyGhlApptProbe(404, null), 'deleted');
check('200 with deleted:true is a deletion',
  classifyGhlApptProbe(200, { appointment: { deleted: true, appoinmentStatus: 'confirmed' } }), 'deleted');

// ── Cancelled belongs to the webhook path, not the sweep ────────────────────
check('cancelled status is NOT a deletion',
  classifyGhlApptProbe(200, { appointment: { deleted: false, appoinmentStatus: 'cancelled' } }), 'cancelled');
check('US spelling canceled also recognized',
  classifyGhlApptProbe(200, { appointment: { deleted: false, appointmentStatus: 'canceled' } }), 'cancelled');

// ── Live appointments pass through untouched ────────────────────────────────
check('confirmed is active',
  classifyGhlApptProbe(200, { appointment: { deleted: false, appoinmentStatus: 'confirmed' } }), 'active');
check('showed is active (outcome flow owns it)',
  classifyGhlApptProbe(200, { appointment: { deleted: false, appoinmentStatus: 'showed' } }), 'active');
check('noshow is active (outcome flow owns it)',
  classifyGhlApptProbe(200, { appointment: { deleted: false, appoinmentStatus: 'noshow' } }), 'active');

// ── Failures never classify as deleted (a 500 must not fire a cancel alert) ──
check('500 is an error, not a deletion', classifyGhlApptProbe(500, null), 'error');
check('401 is an error (bad token must not mass-cancel)', classifyGhlApptProbe(401, null), 'error');
check('200 with no appointment body is an error', classifyGhlApptProbe(200, {}), 'error');
check('200 with null body is an error', classifyGhlApptProbe(200, null), 'error');
check('missing status on live row is active (deleted flag is the deletion signal)',
  classifyGhlApptProbe(200, { appointment: { deleted: false } }), 'active');

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll appt-deletion-sweep checks passed.');
