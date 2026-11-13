// Lead card rows rebuilt from Slack.  Run:  node test/lead-post-heal.test.js
//
// lib/leadPostHeal.js is pure. Fixtures are synthetic, shaped like the
// 2026-11-12 database freeze: two cards posted, neither row written.
const heal = require('../lib/leadPostHeal');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const now = Date.parse('2026-11-13T01:00:00Z');
const ts = (iso) => (Date.parse(iso) / 1000).toFixed(6);
const card = (contactId, iso, extra = {}) => ({
  ts: ts(iso), text: '🆕 *New Lead*',
  metadata: { event_type: 'ghl_lead', event_payload: { contact_id: contactId, full_name: 'Ana Prueba', email: 'Ana@Example.com', ...extra } },
});
const opts = { channelId: 'C_LEADS', now };

const messages = [
  card('c-new', '2026-11-13T00:40:00Z', { source: 'Social media', channel: 'instagram', phone_last10: '5550001111', name_prefix3: 'ana' }),
  card('c-old', '2026-11-12T23:10:00Z'),
  card('c-has-row', '2026-11-12T20:00:00Z'),
  { ts: ts('2026-11-12T22:00:00Z'), text: 'a person talking' },
  { ts: ts('2026-11-12T22:05:00Z'), metadata: { event_type: 'guardian_card', event_payload: { contact_id: 'c-other' } } },
  { ts: ts('2026-11-12T22:06:00Z'), thread_ts: ts('2026-11-12T20:00:00Z'), metadata: { event_type: 'ghl_lead', event_payload: { contact_id: 'c-reply' } } },
  { ts: ts('2026-11-12T22:07:00Z'), metadata: { event_type: 'ghl_lead', event_payload: {} } },
];

check('only top-level lead cards with a contact id count', heal.leadCards(messages).map((m) => m.metadata.event_payload.contact_id), ['c-new', 'c-old', 'c-has-row']);

const rows = heal.missingRows(messages, new Set(['c-has-row']), opts);
check('a card with no row gets one; a card with a row is left alone', rows.map((r) => r.contact_id).sort(), ['c-new', 'c-old']);

const fresh = rows.find((r) => r.contact_id === 'c-new');
check('the row points at the card and takes its time from it', [fresh.slack_message_ts, fresh.slack_channel_id, fresh.posted_at], [ts('2026-11-13T00:40:00Z'), 'C_LEADS', '2026-11-13T00:40:00.000Z']);
check('the row carries what the card carries', [fresh.source, fresh.channel, fresh.phone_last10, fresh.email_lower, fresh.full_name, fresh.name_prefix3], ['Social media', 'instagram', '5550001111', 'ana@example.com', 'Ana Prueba', 'ana']);

const old = rows.find((r) => r.contact_id === 'c-old');
check('an older card without the extra fields leaves them null, never guessed', [old.source, old.channel, old.phone_last10, old.name_prefix3, old.email_lower], [null, null, null, null, 'ana@example.com']);

check('a card under 2 minutes old is left to its own write', heal.missingRows([card('c-young', '2026-11-13T00:59:00Z')], new Set(), opts), []);
check('a contact carded twice gets one row, on the first card', heal.missingRows([card('c-two', '2026-11-12T23:30:00Z'), card('c-two', '2026-11-12T23:00:00Z')], [], opts).map((r) => r.posted_at), ['2026-11-12T23:00:00.000Z']);
check('nothing to do on an empty page', [heal.missingRows([], new Set(), opts), heal.missingRows(null, null, opts)], [[], []]);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nAll lead post heal checks passed');
