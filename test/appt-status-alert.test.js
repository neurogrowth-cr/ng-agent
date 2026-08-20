// Rules test for the appointment-status sweep's write-failure alert.
// Run:  node test/appt-status-alert.test.js
//
// Same extraction trick as the other suites: buildApptWriteFailureAlert is
// sliced out of index.js and compiled with new Function, so the test can never
// drift from shipped behaviour (index.js boots the Slack app on require).
//
// Guards the 2026-08-20 incident: 4 of 4 writes 401'd on a missing
// calendars/events.write scope, and the alert told Ron to go look for a
// transient auth blink in the Railway logs. The run already knew better —
// its reads had succeeded.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const GHL_SCOPE_401'),
  SRC.indexOf('async function runAppointmentStatusSync'),
);

let failures = 0;
function check(label, cond) {
  if (cond) { console.log(`ok   ${label}`); }
  else { failures += 1; console.error(`FAIL ${label}`); }
}

const buildApptWriteFailureAlert = new Function(`${block}; return buildApptWriteFailureAlert;`)();

const SCOPE = 'appt PUT abc → 401: {"statusCode":401,"message":"The token is not authorized for this scope."}';
const BLINK = 'appt PUT abc → 401: {"statusCode":401,"message":"Invalid JWT"}';
const FIVE  = 'appt PUT abc → 502: bad gateway';

// ── The live 2026-08-20 shape: reads fine, every write refused on scope ──────
const scopeAlert = buildApptWriteFailureAlert({
  considered: 40, showed: 0, failed: 4, failures: [SCOPE, SCOPE, SCOPE, SCOPE], readsOk: true,
});
check('names the missing scope', scopeAlert.includes('calendars/events.write'));
check('names where to fix it', scopeAlert.includes('Private Integrations'));
check('rules the blink out in words', scopeAlert.includes('NOT the transient 401 blink'));
check('does not send Ron to the Railway logs', !scopeAlert.includes('Railway logs'));
check('says a redeploy is not needed', scopeAlert.includes('no redeploy is needed'));
check('still carries the tally', scopeAlert.includes('4 write(s) failed, 0 succeeded (of 40 considered)'));
check('still says attendance was not marked', scopeAlert.includes('Attendance was NOT marked'));

// ── A real blink, or anything mixed, must keep the old guidance ──────────────
const blinkAlert = buildApptWriteFailureAlert({
  considered: 10, showed: 0, failed: 3, failures: [BLINK, BLINK, BLINK], readsOk: true,
});
check('non-scope 401s keep the blink guidance', blinkAlert.includes('Railway logs'));
check('non-scope 401s do not name the scope', !blinkAlert.includes('calendars/events.write'));

const mixedAlert = buildApptWriteFailureAlert({
  considered: 10, showed: 1, failed: 3, failures: [SCOPE, SCOPE, FIVE], readsOk: true,
});
check('one non-scope failure is enough to stay generic', mixedAlert.includes('Railway logs'));

// ── Reads down is a bigger problem than a write scope — never mislabel it ────
const readsDown = buildApptWriteFailureAlert({
  considered: 10, showed: 0, failed: 4, failures: [SCOPE, SCOPE, SCOPE, SCOPE], readsOk: false,
});
check('scope claim requires the reads to have worked', !readsDown.includes('calendars/events.write'));

// ── Degenerate inputs ───────────────────────────────────────────────────────
check('no failure list → generic, never a scope accusation',
  buildApptWriteFailureAlert({ considered: 5, showed: 0, failed: 2, failures: [], readsOk: true }).includes('Railway logs'));
check('no arguments at all still returns a string',
  typeof buildApptWriteFailureAlert() === 'string');
check('matching is case-insensitive',
  buildApptWriteFailureAlert({
    failed: 1, failures: ['401: The Token Is Not Authorized For This Scope.'], readsOk: true,
  }).includes('calendars/events.write'));

console.log(failures ? `\n${failures} check(s) failed` : '\nAll checks passed');
process.exit(failures ? 1 : 0);
