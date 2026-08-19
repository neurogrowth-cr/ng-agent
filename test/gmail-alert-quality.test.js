// Rules test for the customer lifecycle alert quality check.
// Run:  node test/gmail-alert-quality.test.js
//
// Extracts the real block straight out of index.js rather than copying it, so the
// test can never drift from shipped behaviour (same approach as make-watchdog and
// strike-rules — index.js boots the Slack app on require).
//
// Card shapes are taken verbatim from the live blueprints checked in at
// ~/automations/ops/make-blueprints/ (4356754 modules 3/13/15/17/18, and 5975679
// module 2), captured 2026-08-18.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const GMAIL_ALERT_CHANNELS'),
  SRC.indexOf('// Hourly. The fan-out polls every 20 min'),
);
if (!block) { console.error('FAIL: could not extract the gmail alert block from index.js'); process.exit(1); }

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// Build the block with stubbed I/O. Returns the checker, the pure evaluator, and
// the alert log.
function build({ history = {}, clients = [], clientErr = null, historyErr = {} } = {}) {
  const posted = [];
  const logged = [];
  const slackStub = {
    client: {
      conversations: {
        history: async ({ channel }) => {
          if (historyErr[channel]) { const e = new Error(historyErr[channel]); e.data = { error: historyErr[channel] }; throw e; }
          return { messages: (history[channel] || []).map((text, i) => ({ text, ts: `${i}.0` })) };
        },
      },
    },
  };
  const portalStub = {
    from: () => ({
      select: () => ({
        gte: () => ({
          lte: () => ({
            limit: async () => ({ data: clients, error: clientErr }),
          }),
        }),
      }),
    }),
  };
  const factory = new Function(
    'slack', 'portalSupabase', 'postMakeHealthAlert', 'logActivity', 'RON_SLACK_ID', 'console',
    `${block}; return { checkGmailAlertQuality, evaluateAlertCard };`
  );
  const api = factory(
    slackStub, portalStub,
    async (text) => { posted.push(text); },
    (e) => { logged.push(e); },
    'U05HXGX18H3',
    { log: () => {}, warn: () => {}, error: () => {} },
  );
  return { ...api, posted, logged };
}

// ── Card fixtures, verbatim shapes from the blueprints ──
const GOOD_CLOSER = `⏱️ CLOSER REQUIRED ⏱️
*Customer/Company Name:* Tropikasa
Closer Required - Tropikasa - Flywheel AI Account
2026-08-18`;

const GOOD_ACTIVITY = `⏱️ ACTIVITY TRACKING ⏱️
*Customer/Company Name:* Acme Corp
*Customer Email:* ops@acme.com
Activity completed for Acme Corp
2026-08-18`;

const GOOD_NEW_CLIENT = `🔔🔔 NEW FLYWHEEL AI CUSTOMER 🔔🔔
Customer/Company Name: Tropikasa
Customer Email: kty@tropikasa.com
Start Date: 2026-08-18
Customer-ID: tropikasa`;

// The Aug 13–17 outage shape: split() found no anchor, name renders blank.
const BLANK_NAME = `⏱️ CLOSER REQUIRED ⏱️
*Customer/Company Name:*
Closer Required - Tropikasa - Flywheel AI Account
2026-08-18`;

// The gap the 2026-08-17 rewrite left open: subject prefix renamed, replace() is a
// no-op, so the whole raw subject lands in the name slot.
const RAW_SUBJECT_LEAK = `⏱️ SETTER REQUIRED ⏱️
*Customer/Company Name:* Setter Needed - Tropikasa
Setter Needed - Tropikasa
2026-08-18`;

// Old prefix survives because only part of the subject changed.
const RESIDUE_LEAK = `⏱️ CLOSER REQUIRED ⏱️
*Customer/Company Name:* Closer Required - Tropikasa
Closer Required - Tropikasa — Flywheel AI Account
2026-08-18`;

const OBJECT_DUMP = `⏱️ ONBOARDING COMPLETED ⏱️
*Customer/Company Name:* [object Object]
Flywheel onboarding form completed – Tropikasa
2026-08-18`;

const BAD_EMAIL = `⏱️ ACTIVITY TRACKING ⏱️
*Customer/Company Name:* Acme Corp
*Customer Email:*
Activity completed
2026-08-18`;

// Module 3's fallback is legitimate on ACTIVITY TRACKING only.
const FALLBACK_OK = `⏱️ ACTIVITY TRACKING ⏱️
*Customer/Company Name:* — not included in this email —
*Customer Email:* ops@acme.com
Activity completed
2026-08-18`;
const FALLBACK_MISPLACED = `⏱️ SETTER REQUIRED ⏱️
*Customer/Company Name:* — not included in this email —
Setter Required - Tropikasa - Flywheel AI Account
2026-08-18`;

// Route 13 carries no name field at all — must not be flagged.
const PROSP_ISSUE = `:rotating_light: *Issues with Prosp Campaign*

*Subject:* Campaign paused
*Date:* Mon, 18 Aug 2026 09:00:00 -0600`;

const { evaluateAlertCard } = build();

// ── 1. The pure contract ──
check('1a good closer card passes',        evaluateAlertCard(GOOD_CLOSER).ok, true);
check('1b good activity card passes',      evaluateAlertCard(GOOD_ACTIVITY).ok, true);
check('1c good new-client card passes',    evaluateAlertCard(GOOD_NEW_CLIENT).ok, true);
check('1d prosp issue card passes',        evaluateAlertCard(PROSP_ISSUE).ok, true);
check('1e module-3 fallback is allowed on activity tracking', evaluateAlertCard(FALLBACK_OK).ok, true);

check('2a blank name is caught',           evaluateAlertCard(BLANK_NAME).ok, false);
check('2b blank name names the problem',   evaluateAlertCard(BLANK_NAME).problems.includes('empty customer name'), true);
check('3a raw-subject leak is caught',     evaluateAlertCard(RAW_SUBJECT_LEAK).ok, false);
check('3b raw-subject leak is identified', evaluateAlertCard(RAW_SUBJECT_LEAK).problems.some(p => p.includes('identical to another line')), true);
check('4a residue leak is caught',         evaluateAlertCard(RESIDUE_LEAK).ok, false);
check('4b residue leak names the fragment', evaluateAlertCard(RESIDUE_LEAK).problems.some(p => p.includes('residue')), true);
check('5a object dump is caught',          evaluateAlertCard(OBJECT_DUMP).ok, false);
check('6a empty email is caught',          evaluateAlertCard(BAD_EMAIL).ok, false);
check('7a misplaced fallback is caught',   evaluateAlertCard(FALLBACK_MISPLACED).ok, false);

// ── 8. End to end: a clean window posts nothing ──
(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A9NH9PZ7C: [GOOD_CLOSER], C0A7X9G6S78: [GOOD_NEW_CLIENT] },
    clients: [{ client_name: 'Tropikasa', email: 'kty@tropikasa.com', created_at: '2026-08-18T10:00:00Z' }],
  });
  await checkGmailAlertQuality('c');
  check('8  clean window posts nothing', posted.length, 0);
})()

// ── 9. A bad card alerts once, and not again ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({ history: { C0A9NH9PZ7C: [BLANK_NAME] } });
  await checkGmailAlertQuality('c');
  check('9a bad card alerts',            posted.length, 1);
  check('9b alert tags Ron',             posted[0].includes('<@U05HXGX18H3>'), true);
  await checkGmailAlertQuality('c');
  check('9c no duplicate alert on rerun', posted.length, 1);
})

// ── 10. The divergence case: the customer exists, no card was ever posted ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A7X9G6S78: [] },
    clients: [{ client_name: 'Kty Araya', email: 'kty@tropikasa.com', created_at: '2026-08-06T10:00:00Z' }],
  });
  await checkGmailAlertQuality('c');
  check('10a missing alert is caught',       posted.length, 1);
  check('10b alert names the customer',      posted[0].includes('Kty Araya'), true);
  check('10c alert warns against re-posting', posted[0].includes('duplicate card is worse'), true);
})

// ── 11. A closer card must NOT count as new-customer coverage ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A9NH9PZ7C: [GOOD_CLOSER], C0A7X9G6S78: [] },
    clients: [{ client_name: 'Tropikasa', email: 'kty@tropikasa.com', created_at: '2026-08-18T10:00:00Z' }],
  });
  await checkGmailAlertQuality('c');
  check('11  a different event type does not mask a missing new-customer card', posted.length, 1);
})

// ── 12. Fail closed: an unreadable channel degrades the verdict, never greens it ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    historyErr: { C0A7X9G6S78: 'channel_not_found' },
    clients: [],
  });
  await checkGmailAlertQuality('c');
  check('12a unreadable channel still posts',   posted.length, 1);
  check('12b it is reported as skipped, not green', posted[0].includes('Skipped'), true);
  check('12c divergence is suppressed when the channel is unreadable',
        posted[0].includes('no alert card'), false);
})

// ── 13. Fail closed: an unreadable client table never invents missing customers ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A7X9G6S78: [GOOD_NEW_CLIENT] },
    clientErr: { message: 'permission denied' },
  });
  await checkGmailAlertQuality('c');
  check('13a table error is reported', posted[0].includes('Skipped'), true);
  check('13b no phantom missing customers', posted[0].includes('no alert card'), false);
})

.then(() => {
  console.log(failures ? `\n${failures} check(s) failed.` : '\nAll checks passed.');
  process.exit(failures ? 1 : 0);
});
