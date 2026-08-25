// Rules test for the client/fulfillment-call filter on the GHL lead-intake
// webhook.  Run:  node test/client-call-leads.test.js
//
// Extracts the real CLIENT_CALL_CALENDAR_IDS / CLIENT_CALL_SOURCE_RE /
// clientCallLeadReason block straight out of index.js rather than copying it,
// so the test cannot drift from shipped behaviour (same approach as
// stale-lead-sweep.test.js — index.js boots the Slack app on require).
//
// Fixtures are the two real leaks: CARLOS TRUJILLO (activation call, posted to
// #ng-sales-goats 2026-08-25) and Jose Tencio (2026-08-13, source naming a
// calendar GHL has since renamed). Sales fixtures are the live source strings
// from lead_posts, which is ~99% Facebook / Paid Social / Social media.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const CLIENT_CALL_CALENDAR_IDS'),
  SRC.indexOf('const LEAD_CLAIM_EMOJIS'),
);
if (!block.includes('function clientCallLeadReason')) {
  console.error('FAIL could not extract the client-call block from index.js');
  process.exit(1);
}

let failures = 0;
function check(label, cond) {
  if (cond) console.log(`ok   ${label}`);
  else { failures++; console.error(`FAIL ${label}`); }
}

function build(env = {}) {
  return new Function('process', `${block}; return clientCallLeadReason;`)({ env });
}
const reason = build();

// ── The two real leaks ───────────────────────────────────────────────────────
// Carlos carries all three signals; each must independently be enough, because
// a webhook payload can arrive with the GHL contact lookup having failed.
check('activation call — createdBy.sourceId alone',
  reason({ calendarIds: ['VYv8EyyFsNiYpKpyqsus'] }) === 'calendar VYv8EyyFsNiYpKpyqsus');
check('activation call — attribution mediumId alone',
  reason({ calendarIds: [undefined, 'VYv8EyyFsNiYpKpyqsus'] }) === 'calendar VYv8EyyFsNiYpKpyqsus');
check('activation call — source string alone (no contact lookup)',
  reason({ sourceRaw: 'LinkedIn Flywheel Activation Call' }) === 'source "LinkedIn Flywheel Activation Call"');
check('activation call — source only on the fetched contact',
  reason({ contactSource: 'LinkedIn Flywheel Activation Call' }) !== null);

// Jose Tencio, 2026-08-13. The calendar has since been renamed "Client 1:1 with
// Ron", which is exactly why the ID match exists — but the stored name must
// still be caught, since that is all a fallback payload carries.
check('renamed 1:1 calendar — old name still matches',
  reason({ sourceRaw: 'LinkedIn Flywheel Internal 1:1 with Ron' }) !== null);
check('renamed 1:1 calendar — current name matches',
  reason({ sourceRaw: 'LinkedIn Flywheel - Client 1:1 with Ron' }) !== null);
check('1:1 with Josue by calendar id',
  reason({ calendarIds: ['H9q2eNZMsr0APPCrinWC'] }) === 'calendar H9q2eNZMsr0APPCrinWC');
check('quick sync by calendar id',
  reason({ calendarIds: ['YkSsJAMlGhOGK93Lix7Y'] }) === 'calendar YkSsJAMlGhOGK93Lix7Y');
check('quick sync by name',      reason({ sourceRaw: 'LinkedIn Flywheel Quick Sync' }) !== null);
check('quick sync, no space',    reason({ sourceRaw: 'LinkedIn Flywheel QuickSync' }) !== null);
check('official delivery by id', reason({ calendarIds: ['VTdxE8vEJ26U509wazNL'] }) !== null);
// The live calendar name has a leading space — trimming is not assumed anywhere.
check('official delivery by name (leading space, as stored in GHL)',
  reason({ sourceRaw: ' LinkedIn Flywheel Official Delivery' }) !== null);

// ── Real sales traffic must pass untouched ───────────────────────────────────
for (const s of ['Facebook', 'Paid Social', 'Social media', 'WhatsApp', 'CRM UI',
                 'iClosed - LinkedIn Flywheel (fallback)', 'Organic', 'Referral']) {
  check(`sales lead passes: ${s}`, reason({ sourceRaw: s }) === null);
}
// Sales calendars share the "LinkedIn Flywheel" prefix with the client ones —
// matching on that prefix would silently kill the entire paid funnel.
for (const [name, id] of [['LinkedIn Flywheel - Intro', 'KRTGx8XteIJSCcKAShHS'],
                          ['LinkedIn Flywheel - Appointment', 'fYQJCzbk4hvV0brpJqoE'],
                          ['LinkedIn Flywheel - Appointment - ONLY RON', '0qwExROqOMRBXVmY93i5'],
                          ['LinkedIn Flywheel - Self Serving', 'HXLeEjxpa0gdiTPNiAzc']]) {
  check(`sales calendar passes: ${name}`,
    reason({ sourceRaw: name, contactSource: name, calendarIds: [id] }) === null);
}

// ── Degrade-open behaviour ───────────────────────────────────────────────────
// A payload with nothing usable is a lead, not a drop: failing closed here would
// silently swallow real leads whenever the GHL contact lookup errors.
check('empty payload is not a drop',        reason({}) === null);
check('no argument at all is not a drop',   reason() === null);
check('nulls in calendarIds are ignored',   reason({ calendarIds: [null, undefined, ''] }) === null);
check('unknown calendar id is not a drop',  reason({ calendarIds: ['someOtherCalendar'] }) === null);

// ── Env override ─────────────────────────────────────────────────────────────
const withEnv = build({ CLIENT_CALL_CALENDAR_IDS: 'newCal123 , newCal456' });
check('env-added calendar drops',        withEnv({ calendarIds: ['newCal456'] }) === 'calendar newCal456');
check('env override keeps the defaults', withEnv({ calendarIds: ['VYv8EyyFsNiYpKpyqsus'] }) !== null);
check('env override does not drop sales', withEnv({ sourceRaw: 'Facebook' }) === null);

console.log(failures ? `\n${failures} check(s) failed` : '\nall checks passed');
process.exit(failures ? 1 : 0);
