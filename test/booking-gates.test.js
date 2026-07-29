// Tests the stalled-nudge booking gates.  Run:  node test/booking-gates.test.js
//
// These gates replaced lookups against `revops_iclosed_bookings`, a table that does
// not exist — so they had been silently returning "no booking" for every prospect,
// meaning anyone who had already booked or attended a call could still be chased as
// a stalled lead. Fixtures use real GHL /contacts/{id}/appointments payload shapes.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// GHL returns startTime as'),
  SRC.indexOf('// Returns true if a setter has DM\'d Max'),
);
const g = new Function(`${block}; return { parseGhlApptTime, findFutureBooking, findAttendedCall, findAppointmentPendingOutcome };`)();

const NOW = Date.parse('2026-07-29T23:00:00Z');
const appt = (startTime, appointmentStatus, extra = {}) => ({ startTime, appointmentStatus, deleted: false, ...extra });

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// GHL sends "YYYY-MM-DD HH:mm:ss" with no offset. Parsed as UTC, it must not drift
// with the host timezone — Railway runs UTC but a laptop may not.
check('appointment time parses as UTC, not host-local',
  g.parseGhlApptTime('2026-07-29 15:00:00'), Date.parse('2026-07-29T15:00:00Z'));
check('unparseable time yields 0 rather than NaN',
  g.parseGhlApptTime(''), 0);

// REAL: Maryia Proskouriakov — booked Jul 28, call Jul 29 15:00 UTC, still 'confirmed'
// because the closer hasn't logged the outcome. At 23:00 the call has passed.
const maryia = [appt('2026-07-29 15:00:00', 'confirmed')];
check('a passed call is not treated as an upcoming booking',
  g.findFutureBooking(maryia, NOW), null);
check('passed call still marked confirmed is held as pending outcome',
  g.findAppointmentPendingOutcome(maryia, NOW)?.event_at, '2026-07-29 15:00:00');

// Upcoming call ⇒ booked, never nudge.
check('upcoming confirmed call blocks the nudge',
  g.findFutureBooking([appt('2026-07-31 15:00:00', 'confirmed')], NOW)?.event_at, '2026-07-31 15:00:00');

// Cancelled / no-showed future slots must NOT count as booked — those prospects are
// exactly the ones worth chasing.
check('cancelled future call does not block the nudge',
  g.findFutureBooking([appt('2026-07-31 15:00:00', 'cancelled')], NOW), null);
check('no-showed future call does not block the nudge',
  g.findFutureBooking([appt('2026-07-31 15:00:00', 'noshow')], NOW), null);

// Attended call ⇒ different motion entirely.
check('attended call is detected',
  g.findAttendedCall([appt('2026-07-20 15:00:00', 'showed')])?.event_status, 'showed');
check('no-show is not an attended call',
  g.findAttendedCall([appt('2026-07-20 15:00:00', 'noshow')]), null);

// Earliest upcoming wins when several are booked.
check('earliest upcoming booking is returned',
  g.findFutureBooking([
    appt('2026-08-05 15:00:00', 'confirmed'),
    appt('2026-07-31 15:00:00', 'confirmed'),
  ], NOW)?.event_at, '2026-07-31 15:00:00');

// A stale unlogged appointment must not mute a prospect forever.
check('unlogged call older than the 7-day window stops holding the nudge',
  g.findAppointmentPendingOutcome([appt('2026-07-01 15:00:00', 'confirmed')], NOW), null);

// No history at all ⇒ every gate open, prospect is nudgeable.
check('no appointments leaves all gates open',
  [g.findFutureBooking([], NOW), g.findAttendedCall([]), g.findAppointmentPendingOutcome([], NOW)],
  [null, null, null]);

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
