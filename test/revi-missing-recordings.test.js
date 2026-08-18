// Tests the missing-recordings divergence check.  Run:  node test/revi-missing-recordings.test.js
//
// The gap this closes, in both directions:
//   SALES  — 2026-08-17, closer Jose Carranza had 4 confirmed appointments and
//            Fathom recorded ZERO. Nothing fired; nobody noticed until Ron asked.
//   FULFIL — 2026-08-05, the "Jacobo Lau" client check-in was booked and never
//            recorded, so that client got no report and nobody found out.
// Nothing compared bookings against recordings, in either track.
//
// The two ways this check can be WORSE than useless:
//   1. Firing on cold start. revi.call_ledger is empty on the deploy that
//      introduces it, so without a warmup every appointment in the window looks
//      missing — an 18-row false alarm, verified against live data 2026-08-18.
//   2. Asserting a cause it cannot know. In this GHL location `confirmed` is the
//      BOOKING status (set by the widget at book time, never updated), so it
//      cannot separate "recorder was off" from "unmarked no-show".
// Fixtures use real GHL appointment ids and real REVI ledger row shapes.
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// Pure: decide which booked calls are genuinely missing'),
  SRC.indexOf('// END REVI CROSS-CHECK PURE HELPERS'),
);
const { classifyMissingRecordings } = new Function(`${block}; return { classifyMissingRecordings };`)();

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

const appt = (id, start, status = 'confirmed') => ({
  ghl_appointment_id: id, start_time: start, appointment_status: status,
  assigned_user_id: 'izLTA0jy5OrKyMvyItjV',
});
// Jose Carranza's real 2026-08-17 bookings — the incident.
const HELD = [
  appt('fHlPFPDo86eG4pE3mtOV', '2026-08-17T17:00:00+00'),
  appt('6vagU9lJxvlftOgjvqzP', '2026-08-17T18:00:00+00'),
  appt('7NFAvfMQqB2Os7ed0THW', '2026-08-17T20:00:00+00'),
  appt('3NKxYWY8yZ5Tno2NFBdF', '2026-08-17T22:00:00+00'),
];

// 1. COLD START. An empty ledger must produce NO verdict — not "everything is
//    missing". This is the check that keeps the first deploy quiet.
check('empty ledger warms up instead of alarming',
  (() => { const r = classifyMissingRecordings({ held: HELD, ledgerRows: [] });
    return [r.warmingUp, r.missing.length]; })(),
  [true, 0]);

// 2. Appointments from BEFORE the ledger's first row are unjudgeable — REVI was
//    not writing rows yet, so their absence proves nothing.
check('pre-ledger appointments are not judged',
  classifyMissingRecordings({
    held: HELD,
    ledgerRows: [{ call_date: '2026-08-17T21:00:00+00', ghl_appointment_id: 'someone-else' }],
  }).missing.map(a => a.ghl_appointment_id),
  ['3NKxYWY8yZ5Tno2NFBdF']);

// 3. The incident itself: ledger covers the window, none of the four joined.
const LEDGER = [{ call_date: '2026-08-17T00:00:00+00', ghl_appointment_id: 'unrelated-but-in-window' }];
check('all four of Jose\'s 08-17 appointments flag once the ledger covers them',
  classifyMissingRecordings({ held: HELD, ledgerRows: LEDGER }).missing.length,
  4);

// 4. A SKIPPED-but-ledgered call is NOT missing. REVI saw it and said why —
//    that is Check 1's job, and double-reporting it would train Ron to ignore both.
check('a ledgered skip is not a missing recording',
  classifyMissingRecordings({
    held: HELD,
    ledgerRows: [...LEDGER,
      { call_date: '2026-08-17T17:30:00+00', ghl_appointment_id: 'fHlPFPDo86eG4pE3mtOV', action: 'skipped', reason: 'under_min_minutes' }],
  }).missing.map(a => a.ghl_appointment_id),
  ['6vagU9lJxvlftOgjvqzP', '7NFAvfMQqB2Os7ed0THW', '3NKxYWY8yZ5Tno2NFBdF']);

// 5. HONESTY. Without corroboration every miss is ambiguous — the alert may not
//    claim the recorder failed when the call may simply never have happened.
check('bare `confirmed` yields ambiguous, never a claimed cause',
  (() => { const r = classifyMissingRecordings({ held: HELD, ledgerRows: LEDGER });
    return [r.happened.length, r.ambiguous.length]; })(),
  [0, 4]);

// 6. A logged sales outcome proves the call happened — that miss IS assertable.
check('a logged outcome upgrades ambiguous -> genuinely missing',
  (() => { const r = classifyMissingRecordings({
      held: HELD, ledgerRows: LEDGER, outcomeApptIds: ['7NFAvfMQqB2Os7ed0THW'] });
    return [r.happened.map(a => a.ghl_appointment_id), r.ambiguous.length]; })(),
  [['7NFAvfMQqB2Os7ed0THW'], 3]);

// 7. `showed` is unambiguous on its own — no outcome row needed.
check('`showed` with no recording is asserted, not hedged',
  classifyMissingRecordings({
    held: [appt('X', '2026-08-17T17:00:00+00', 'showed')], ledgerRows: LEDGER,
  }).happened.map(a => a.ghl_appointment_id),
  ['X']);

// 8. FULFILMENT is watched too. The Jacobo Lau case: a client check-in booked on
//    2026-08-05 that was never recorded — so that client got no report and
//    nobody found out. Sales-only scoping would still miss it today.
check('a fulfilment check-in with no recording is flagged, not ignored',
  classifyMissingRecordings({
    held: [{ ghl_appointment_id: 'lml3p5rsQZWKx9hXZify', start_time: '2026-08-05T20:30:00+00',
             appointment_status: 'confirmed', calendar_id: 'rRh3CZnC4DlF3Fo1HqXq',
             assigned_user_id: 'zoGW530iDnPOFqQNfssc' }],
    ledgerRows: [{ call_date: '2026-08-05T00:00:00+00', ghl_appointment_id: 'other' }],
  }).missing.map(a => a.ghl_appointment_id),
  ['lml3p5rsQZWKx9hXZify']);

// 9. ...and it stays AMBIGUOUS: revops_sales_outcomes only covers sales calls, so
//    a fulfilment miss can never be corroborated that way. The alert must not
//    imply it checked something it cannot check.
check('a fulfilment miss cannot be corroborated by a sales outcome',
  (() => { const r = classifyMissingRecordings({
      held: [{ ghl_appointment_id: 'lml3p5rsQZWKx9hXZify', start_time: '2026-08-05T20:30:00+00',
               appointment_status: 'confirmed', calendar_id: 'rRh3CZnC4DlF3Fo1HqXq' }],
      ledgerRows: [{ call_date: '2026-08-05T00:00:00+00', ghl_appointment_id: 'other' }],
      outcomeApptIds: [] });
    return [r.happened.length, r.ambiguous.length]; })(),
  [0, 1]);

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'ok  ' : 'FAIL'} ${c.name}`);
  if (!c.ok) { failed++; console.log(`     got:  ${JSON.stringify(c.got)}\n     want: ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
