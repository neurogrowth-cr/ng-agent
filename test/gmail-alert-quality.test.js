// Rules test for the customer lifecycle alert quality check.
// Run:  node test/gmail-alert-quality.test.js
//
// Extracts the real block straight out of index.js rather than copying it, so the
// test can never drift from shipped behaviour (same approach as make-watchdog and
// strike-rules — index.js boots the Slack app on require).
//
// Card shapes are taken verbatim from conversations.history — what this code READS —
// NOT from the Make blueprint, which is what Make SENDS. Those differ: Make emits a
// literal "⏱️" and Slack stores ":stopwatch:". Building fixtures from the blueprint
// is what let the emoji-shortcode bug ship green on 2026-08-19; every card silently
// failed the header match, so the contract examined nothing.
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
    `${block}; return { checkGmailAlertQuality, evaluateAlertCard, cardExistsFor, shouldReportSkipOnly };`
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
const GOOD_CLOSER = `:stopwatch: CLOSER REQUIRED :stopwatch:
*Customer/Company Name:* Tropikasa
Closer Required - Tropikasa - Flywheel AI Account
2026-08-18`;

const GOOD_ACTIVITY = `:stopwatch: ACTIVITY TRACKING :stopwatch:
*Customer/Company Name:* Acme Corp
*Customer Email:* ops@acme.com
Activity completed for Acme Corp
2026-08-18`;

const GOOD_NEW_CLIENT = `:bell::bell: NEW FLYWHEEL AI CUSTOMER :bell::bell:
Customer/Company Name: Tropikasa
Customer Email: <mailto:kty@tropikasa.com|kty@tropikasa.com>
Start Date: 2026-08-18
Customer-ID: tropikasa`;

// The Aug 13–17 outage shape: split() found no anchor, name renders blank.
const BLANK_NAME = `:stopwatch: CLOSER REQUIRED :stopwatch:
*Customer/Company Name:*
Closer Required - Tropikasa - Flywheel AI Account
2026-08-18`;

// The gap the 2026-08-17 rewrite left open: subject prefix renamed, replace() is a
// no-op, so the whole raw subject lands in the name slot.
const RAW_SUBJECT_LEAK = `:stopwatch: SETTER REQUIRED :stopwatch:
*Customer/Company Name:* Setter Needed - Tropikasa
Setter Needed - Tropikasa
2026-08-18`;

// Old prefix survives because only part of the subject changed.
const RESIDUE_LEAK = `:stopwatch: CLOSER REQUIRED :stopwatch:
*Customer/Company Name:* Closer Required - Tropikasa
Closer Required - Tropikasa — Flywheel AI Account
2026-08-18`;

const OBJECT_DUMP = `:stopwatch: ONBOARDING COMPLETED :stopwatch:
*Customer/Company Name:* [object Object]
Flywheel onboarding form completed – Tropikasa
2026-08-18`;

const BAD_EMAIL = `:stopwatch: ACTIVITY TRACKING :stopwatch:
*Customer/Company Name:* Acme Corp
*Customer Email:*
Activity completed
2026-08-18`;

// Module 3's fallback is legitimate on ACTIVITY TRACKING only.
const FALLBACK_OK = `:stopwatch: ACTIVITY TRACKING :stopwatch:
*Customer/Company Name:* — not included in this email —
*Customer Email:* ops@acme.com
Activity completed
2026-08-18`;
const FALLBACK_MISPLACED = `:stopwatch: SETTER REQUIRED :stopwatch:
*Customer/Company Name:* — not included in this email —
Setter Required - Tropikasa - Flywheel AI Account
2026-08-18`;

// Route 13 carries no name field at all — must not be flagged.
const PROSP_ISSUE = `:rotating_light: *Issues with Prosp Campaign*

*Subject:* Campaign paused
*Date:* Mon, 18 Aug 2026 09:00:00 -0600`;

const { evaluateAlertCard, cardExistsFor, shouldReportSkipOnly } = build();

// 09:00 and 10:00 CR — the daily skip-only slot and an ordinary hour.
const AT_9AM_CR  = Date.parse('2026-08-19T15:00:00Z');
const AT_10AM_CR = Date.parse('2026-08-19T16:00:00Z');

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

// ── 7b. Card matching — the regression from the first live run ──
// 2026-08-19 15:00 CR: the check flagged a customer whose card had been posted in
// the same minute, because the two systems order the name components differently.
const AURA_CARD = `:bell::bell: NEW FLYWHEEL AI CUSTOMER :bell::bell:
Customer/Company Name: Cacao Legal - Aura Bonilla
Customer Email: <mailto:abonillabravo@gmail.com|abonillabravo@gmail.com>
Start Date: 2026-08-18
Customer-ID: cacao-legal-aura-bonilla`.toLowerCase();

const AURA_ROW = { client_name: 'Aura Bonilla - Cacao Legal ', email: 'abonillabravo@gmail.com' };

check('7b1 reversed name components still match (the live false positive)',
      cardExistsFor(AURA_ROW, [AURA_CARD]), true);
check('7b2 match survives with no email on the row (token fallback)',
      cardExistsFor({ client_name: 'Aura Bonilla - Cacao Legal', email: '' }, [AURA_CARD]), true);
check('7b3 match survives when the card carries no email line',
      cardExistsFor(AURA_ROW, [`:bell::bell: new flywheel ai customer :bell::bell:
customer/company name: cacao legal - aura bonilla
start date: 2026-08-18`]), true);
check('7b4 a genuinely absent customer is still reported',
      cardExistsFor({ client_name: 'Kty Araya - Tropikasa', email: 'kty@tropikasa.com' }, [AURA_CARD]), false);
// A one-token dashboard name against a longer card name is genuinely ambiguous, and
// the tie is broken deliberately in favour of "covered". Treating it as missing
// reintroduces exactly the false-alarm class this fix exists to remove, and a
// divergence check nobody trusts catches nothing at all. Documented in the spec.
check('7b5 a shorter dashboard name is treated as covered by a longer card name',
      cardExistsFor({ client_name: 'Acme', email: '' }, ['customer/company name: acme holdings international']), true);
// The reverse is NOT true — a card that omits part of the dashboard name is a miss.
check('7b5b a card missing part of the dashboard name is still reported',
      cardExistsFor({ client_name: 'Acme Holdings International', email: '' }, ['customer/company name: acme']), false);
check('7b6 email match wins even when the name is written differently',
      cardExistsFor({ client_name: 'Totally Different Ltd', email: 'abonillabravo@gmail.com' }, [AURA_CARD]), true);
check('7b7 a row with neither email nor usable name is not silently cleared',
      cardExistsFor({ client_name: '', email: '' }, [AURA_CARD]), false);

// ── 7c. Emoji representation — the 2026-08-19 production regression ──
// Make sends a literal "🔔🔔"; conversations.history returns ":bell::bell:". Matching
// headers on the literal emoji made text.includes(header) false for every card, so
// the contract examined nothing, reported "0 bad cards", and left seenCards empty —
// which made the divergence check call every customer missing. Both forms must work:
// this code reads Slack today, but nothing guarantees Slack keeps normalising.
const SHORTCODE_CARD = `:bell::bell: NEW FLYWHEEL AI CUSTOMER :bell::bell:
Customer/Company Name: Cacao Legal - Aura Bonilla
Customer Email: <mailto:abonillabravo@gmail.com|abonillabravo@gmail.com>
Start Date: 2026-08-18
Customer-ID: cacao-legal-aura-bonilla`;

const LITERAL_EMOJI_CARD = SHORTCODE_CARD.replace(/:bell::bell:/g, '🔔🔔');

check('7c1 a shortcode card passes the contract',  evaluateAlertCard(SHORTCODE_CARD).ok, true);
check('7c2 a literal-emoji card passes too',       evaluateAlertCard(LITERAL_EMOJI_CARD).ok, true);
check('7c3 a mailto-wrapped email is not flagged', evaluateAlertCard(SHORTCODE_CARD).problems.length, 0);

// ── 7d. End to end: the exact production case that stayed broken ──
// A real customer, a real card posted in the same minute, in real Slack wire format.
// Before this fix the header never matched, so this reported her as missing.
(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A7X9G6S78: [SHORTCODE_CARD] },
    clients: [{ client_name: 'Aura Bonilla - Cacao Legal ', email: 'abonillabravo@gmail.com',
                created_at: '2026-08-18T20:47:37Z' }],
  });
  await checkGmailAlertQuality('c');
  check('7d  a shortcode card counts as coverage (the live false positive)', posted.length, 0);
})()

// ── 8. End to end: a clean window posts nothing ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A9NH9PZ7C: [GOOD_CLOSER], C0A7X9G6S78: [GOOD_NEW_CLIENT] },
    clients: [{ client_name: 'Tropikasa', email: 'kty@tropikasa.com', created_at: '2026-08-18T10:00:00Z' }],
  });
  await checkGmailAlertQuality('c');
  check('8  clean window posts nothing', posted.length, 0);
})

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
  await checkGmailAlertQuality('c', AT_9AM_CR);
  check('12a unreadable channel still posts',   posted.length, 1);
  check('12b it is reported as skipped, not green', posted[0].includes('Skipped'), true);
  check('12c divergence is suppressed when the channel is unreadable',
        posted[0].includes('no alert card'), false);
  // The first live skip named the route content but not the channel, so it said what
  // was unchecked without saying where to go. An alert you cannot act on is decoration.
  check('12d the skip names the actual channel',
        posted[0].includes('<#C0A7X9G6S78>'), true);
  check('12e channel_not_found carries the remediation',
        posted[0].includes('invite Max to that channel'), true);
})

// ── 12f. A non-membership error on a NON-new-client channel must not suppress
// divergence — only the new-client channel being unreadable does that.
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    historyErr: { C0A70J9638R: 'channel_not_found' },
    history: { C0A7X9G6S78: [] },
    clients: [{ client_name: 'Kty Araya', email: 'kty@tropikasa.com', created_at: '2026-08-19T10:00:00Z' }],
  });
  await checkGmailAlertQuality('c', AT_10AM_CR);
  check('12f a skip elsewhere does not suppress the divergence finding',
        posted[0].includes('Kty Araya'), true);
  check('12g and the unreadable channel is still named',
        posted[0].includes('<#C0A70J9638R>'), true);
})

// ── 13. Fail closed: an unreadable client table never invents missing customers ──
.then(async () => {
  const { checkGmailAlertQuality, posted } = build({
    history: { C0A7X9G6S78: [GOOD_NEW_CLIENT] },
    clientErr: { message: 'permission denied' },
  });
  await checkGmailAlertQuality('c', AT_9AM_CR);
  check('13a table error is reported', posted[0].includes('Skipped'), true);
  check('13b no phantom missing customers', posted[0].includes('no alert card'), false);
})

// ── 14. The hourly skip DM: a standing blind spot is a daily note, not an alarm ──
// Max is not in #ng-newclient-onboarding-tracking, so this skip repeats on every
// single run. Before this, that was 24 identical DMs a day with nothing to act on.
.then(async () => {
  check('14a the 09:00 CR run is the daily slot', shouldReportSkipOnly(AT_9AM_CR), true);
  check('14b 10:00 CR is not',                    shouldReportSkipOnly(AT_10AM_CR), false);
  // Midnight CR must read as hour 0, not 24 — an h12/h24 formatting quirk here would
  // silently move the daily note or delete it.
  check('14c midnight CR is hour 0, not 24',
        shouldReportSkipOnly(Date.parse('2026-08-19T06:10:00Z')), false);

  const off = build({ historyErr: { C0A70J9638R: 'channel_not_found' }, history: { C0A7X9G6S78: [] }, clients: [] });
  await off.checkGmailAlertQuality('c', AT_10AM_CR);
  check('14d a skip-only run outside the slot posts nothing', off.posted.length, 0);
  check('14e and logs nothing, so the row still means "a DM went out"', off.logged.length, 0);

  const on = build({ historyErr: { C0A70J9638R: 'channel_not_found' }, history: { C0A7X9G6S78: [] }, clients: [] });
  await on.checkGmailAlertQuality('c', AT_9AM_CR);
  check('14f the same run inside the slot posts',   on.posted.length, 1);
  check('14g the daily note says the rest was clean',
        on.posted[0].includes('No bad cards and no uncovered customers'), true);
  check('14h and still names the unreadable channel',
        on.posted[0].includes('<#C0A70J9638R>'), true);

  // The suppression must never reach a run that found something. A missing customer
  // at 03:00 is the case this whole check exists for.
  const real = build({
    historyErr: { C0A70J9638R: 'channel_not_found' },
    history: { C0A7X9G6S78: [] },
    clients: [{ client_name: 'Kty Araya', email: 'kty@tropikasa.com', created_at: '2026-08-19T10:00:00Z' }],
  });
  await real.checkGmailAlertQuality('c', Date.parse('2026-08-19T09:00:00Z'));
  check('14i a real finding still alerts at any hour', real.posted.length, 1);
  check('14j and carries the skip footer with it',     real.posted[0].includes('Skipped'), true);
  check('14k without the clean-run line',
        real.posted[0].includes('No bad cards and no uncovered customers'), false);
})

.then(() => {
  console.log(failures ? `\n${failures} check(s) failed.` : '\nAll checks passed.');
  process.exit(failures ? 1 : 0);
});
