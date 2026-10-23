// Rules test for the webinar-registrant filter on the GHL lead-intake webhook.
// Run:  node test/webinar-leads.test.js
//
// Extracts the real WEBINAR_SOURCE_RE / webinarLeadReason block straight out
// of index.js rather than copying it, so the test cannot drift from shipped
// behaviour (same approach as client-call-leads.test.js, which slices the
// same region; index.js boots the Slack app on require).
//
// Fixtures are the real leak of 2026-10-22/23: /registro form contacts with
// source "Webinar Optin" and tag "webinar 27 de octubre" enrolled by GHL's
// "New Lead Intake and Assignment" workflow, producing 30 claimable cards and
// 30 duplicate Appointment Setting opportunities. Sales fixtures are the live
// source strings from lead_posts.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const WEBINAR_SOURCE_RE'),
  SRC.indexOf('const LEAD_CLAIM_EMOJIS'),
);
if (!block.includes('function webinarLeadReason')) {
  console.error('FAIL could not extract the webinar block from index.js');
  process.exit(1);
}

let failures = 0;
function check(label, cond) {
  if (cond) console.log(`ok   ${label}`);
  else { failures++; console.error(`FAIL ${label}`); }
}

const reason = new Function('process', `${block}; return webinarLeadReason;`)({ env: {} });

// ── The real leak, each signal independently sufficient ─────────────────────
// A webhook payload can arrive with the GHL contact lookup having failed, so
// the payload source alone must be enough; and a contact whose source was
// overwritten by a later session must still be caught by its tag.
check('webhook source alone',
  reason({ sourceRaw: 'Webinar Optin' }) === 'source "Webinar Optin"');
check('fetched contact source alone',
  reason({ contactSource: 'Webinar Optin' }) === 'source "Webinar Optin"');
check('tag alone',
  reason({ tags: ['webinar 27 de octubre'] }) === 'tag "webinar 27 de octubre"');
check('all three signals, source wins the reason string',
  reason({ sourceRaw: 'Webinar Optin', contactSource: 'Webinar Optin', tags: ['webinar 27 de octubre'] }) === 'source "Webinar Optin"');

// Future classes: source and tag naming will drift, "webinar" is the anchor.
check('future webinar source', reason({ sourceRaw: 'Webinar LinkedIn Noviembre' }) !== null);
check('future webinar tag',    reason({ tags: ['intake-done', 'WEBINAR 12 de noviembre'] }) !== null);

// An existing FB lead-form contact who also registers for the class is dropped
// by its tag. Intended: that contact already had its New Lead card the first
// time around, and Max's repeat-intake guard (REPEAT_INTAKE_WINDOW_MS) handles the
// re-fire for pre-existing contacts anyway, so losing this intake is correct.
check('FB lead form contact tagged for the webinar drops by tag',
  reason({ sourceRaw: 'Facebook', contactSource: 'Facebook', tags: ['webinar 27 de octubre'] }) === 'tag "webinar 27 de octubre"');

// ── Real sales traffic must pass untouched ───────────────────────────────────
check('intake-done tag with Facebook source passes',
  reason({ sourceRaw: 'Facebook', contactSource: 'Facebook', tags: ['intake-done'] }) === null);
for (const s of ['Paid Social', 'Social media', 'WhatsApp', 'Self-serve (VSL)',
                 'LinkedIn Flywheel - Appointment', 'Facebook', 'CRM UI', 'Organic', 'Referral']) {
  check(`sales lead passes: ${s}`, reason({ sourceRaw: s, contactSource: s }) === null);
}

// ── Degrade-open behaviour ───────────────────────────────────────────────────
// A payload with nothing usable is a lead, not a drop: failing closed here
// would silently swallow real leads whenever the GHL contact lookup errors.
check('empty payload is not a drop',       reason({}) === null);
check('no argument at all is not a drop',  reason() === null);
check('null tags are not a drop',          reason({ tags: null }) === null);
check('nulls inside tags are ignored',     reason({ tags: [null, undefined, ''] }) === null);

console.log(failures ? `\n${failures} check(s) failed` : '\nall checks passed');
process.exit(failures ? 1 : 0);
