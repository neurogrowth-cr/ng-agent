// Rules test for the same-contact repeat guard on the GHL lead-intake webhook.
// Run:  node test/repeat-intake.test.js
//
// Slices REPEAT_INTAKE_WINDOW_MS + repeatIntakeDecision straight out of
// index.js (same approach as client-call-leads.test.js) so the test cannot
// drift from shipped behaviour.
//
// Fixture is the real case: Andres Hernandez Chaves, card posted
// 2026-10-06T02:50:34Z, second webhook for the same contact 02:51:54Z.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const REPEAT_INTAKE_WINDOW_MS'),
  SRC.indexOf('async function handleGHLWebhook'),
);
if (!block.includes('function repeatIntakeDecision')) {
  console.error('FAIL could not extract the repeat-intake block from index.js');
  process.exit(1);
}
const decide = new Function(`${block}; return repeatIntakeDecision;`)();

let failures = 0;
function check(label, cond) {
  if (cond) console.log(`ok   ${label}`);
  else { failures++; console.error(`FAIL ${label}`); }
}

const CARD_AT = Date.parse('2026-10-06T02:50:34.756Z');
const row = { contact_id: 'PaWmwKmDhh7EZoceMT74', slack_message_ts: '1791255034.062229', slack_channel_id: 'C0AJANQBYUE', posted_at: '2026-10-06 02:50:34.756712+00', personal_excluded_at: null };

// The real repeat: 80 seconds after the card.
const r = decide({ existing: row, nowMs: Date.parse('2026-10-06T02:51:54.483Z') });
check('second webhook 80s later is a repeat', r !== null);
check('repeat points at the original card ts', r && r.ts === '1791255034.062229');
check('repeat keeps the original channel', r && r.channel === 'C0AJANQBYUE');
check('age is rounded to minutes', r && r.ageMinutes === 1);
check('repeat on a live card gets a thread note', r && r.thread === true);

// No prior card = new lead. Every shape the lookup can return.
check('no row → new lead', decide({ existing: null, nowMs: CARD_AT }) === null);
check('undefined row → new lead', decide({ nowMs: CARD_AT }) === null);
check('row without a slack ts → new lead', decide({ existing: { ...row, slack_message_ts: null }, nowMs: CARD_AT + 1000 }) === null);
check('row with unparseable posted_at → new lead', decide({ existing: { ...row, posted_at: 'nope' }, nowMs: CARD_AT + 1000 }) === null);

// Window edges: inside 30 days is a repeat, beyond it is a returning lead.
const DAY = 24 * 60 * 60 * 1000;
check('29 days later is still a repeat', decide({ existing: row, nowMs: CARD_AT + 29 * DAY }) !== null);
check('31 days later is a returning lead (fresh card)', decide({ existing: row, nowMs: CARD_AT + 31 * DAY }) === null);
check('custom window is honoured', decide({ existing: row, nowMs: CARD_AT + 2 * DAY, windowMs: DAY }) === null);
check('card posted in the future (clock skew) is not a repeat', decide({ existing: row, nowMs: CARD_AT - 5000 }) === null);

// A 🫂'd contact: the card is gone, so skip but say nothing in Slack.
const gone = decide({ existing: { ...row, personal_excluded_at: '2026-10-06T03:00:00Z' }, nowMs: CARD_AT + 60000 });
check('personal-excluded contact is still skipped', gone !== null);
check('personal-excluded contact gets no thread note', gone && gone.thread === false);

// Duplicate-contact rows (cross-contact dedup) point at the ORIGINAL card's ts,
// so a repeat on the junk contact threads under the real card too.
const dupRow = { ...row, contact_id: 'junkContact', slack_message_ts: '1791255034.062229' };
check('dup-contact row threads under the original card', decide({ existing: dupRow, nowMs: CARD_AT + 60000 })?.ts === '1791255034.062229');

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
