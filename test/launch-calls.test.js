// Launch calls (Ron, 2026-11-09).  Run:  node test/launch-calls.test.js
// lib/launchCalls.js is pure. Calendar ids are the real GHL ids (not secret);
// client names are made up because this repo is public.
const fs = require('fs');
const path = require('path');
const lc = require('../lib/launchCalls');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const DAY = 86400000;
const LAUNCH = '2026-11-02T15:00:00.000Z';
const L = Date.parse(LAUNCH);
const at = (d) => L + d * DAY;
const DELIVERY = 'VTdxE8vEJ26U509wazNL', RON_1ON1 = 'rRh3CZnC4DlF3Fo1HqXq', ACTIVATION = 'VYv8EyyFsNiYpKpyqsus';

// 1. Window: days 0 to 30 only.
check('1a day 3 is checked', lc.launchCallsStatus({ startedAt: LAUNCH, now: at(3) }).day, 3);
check('1b before launch: not checked', lc.launchCallsStatus({ startedAt: LAUNCH, now: at(-1) }), null);
check('1c day 31: not checked (the overdue flag speaks)', lc.launchCallsStatus({ startedAt: LAUNCH, now: at(31) }), null);
check('1d no launch date: not checked', lc.launchCallsStatus({ startedAt: null, now: at(3) }), null);

// 2. Both booked.
let s = lc.launchCallsStatus({ startedAt: LAUNCH, now: at(1), appointments: [
  { calendarId: DELIVERY, startMs: at(2), status: 'confirmed' },
  { calendarId: RON_1ON1, startMs: at(30), status: 'confirmed' },
] });
check('2a both booked: nothing missing', s.missing, []);
check('2b flag is empty', lc.launchCallsFlag(s), '');

// 3. Missing, wrong window, wrong calendar, cancelled.
s = lc.launchCallsStatus({ startedAt: LAUNCH, now: at(1), appointments: [
  { calendarId: DELIVERY, startMs: at(2), status: 'cancelled' },
  { calendarId: RON_1ON1, startMs: at(20), status: 'confirmed' },
  { calendarId: ACTIVATION, startMs: at(30), status: 'confirmed' },
] });
check('3a cancelled delivery, 1:1 at day 20, activation calendar: both missing', s.missing, ['delivery', 'review']);
check('3b flag names both', lc.launchCallsFlag(s), ' 📞 not booked: launch call, 30-day call');
s = lc.launchCallsStatus({ startedAt: LAUNCH, now: at(1), appointments: [{ calendarId: DELIVERY, startMs: at(-3), status: 'showed' }] });
check('3c delivery held 3 days before launch counts; review missing', s.missing, ['review']);
s = lc.launchCallsStatus({ startedAt: LAUNCH, now: at(1), appointments: [{ calendarId: 'NEW-1ON1', startMs: at(29), status: 'confirmed' }], extraReview: ['NEW-1ON1'] });
check('3d extra review calendar id from env counts', s.missing, ['delivery']);
check('3e review window is days 25 to 40', [lc.REVIEW_WINDOW, lc.REVIEW_DAY], [[25, 40], 30]);
s = lc.launchCallsStatus({ startedAt: LAUNCH, now: at(1), appointments: [{ calendarId: 'WQPvlvNFh9Lp9Tb1OVez', startMs: at(31), status: 'confirmed' }, { calendarId: 'YkSsJAMlGhOGK93Lix7Y', startMs: at(2), status: 'confirmed' }] });
check('3f a 1:1 with Gerald counts as the review; a Quick Sync is not the delivery call', s.missing, ['delivery']);

// 4. Unread is never "booked".
check('4a GHL read failed', lc.launchCallsFlag(null, { readFailed: true }), ' ⚠️ launch calls: GHL read failed');
check('4b no contact', lc.launchCallsFlag(null, { noContact: true }), ' ⚠️ launch calls: no GHL contact for the client email');

// 5. The 1:1 reminder moved from day 20 to day 30.
check('5a day 20: no flag any more', lc.reviewFlag(20), '');
check('5b day 28: due soon', lc.reviewFlag(28), ' 📅 1:1 review due in ~2 days');
check('5c day 30: overdue', lc.reviewFlag(30), ' 🔴 1:1 review overdue, schedule now');

// 6. Source: wired into the standup, exact email only, no em dash in new copy.
const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const lib = fs.readFileSync(path.join(__dirname, '..', 'lib', 'launchCalls.js'), 'utf8');
check('6a standup calls the check', src.includes('await launchCallsStandupLines(phase3Clients, now)'), true);
check('6b the old day-20 flag is gone', src.includes("stabDay >= 20 ? ' 🔴 1:1 overdue"), false);
check('6c exact email search only', /async function ghlAppointmentsByEmail[\s\S]{0,600}contacts\/search\/duplicate/.test(src) && !/async function ghlAppointmentsByEmail[\s\S]{0,900}query=/.test(src), true);
check('6d kill switch', src.includes("LAUNCH_CALLS_CHECK || '').toLowerCase() === 'off'"), true);
check('6e no em dash in lib', /—/.test(lib), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nlaunch-calls: all assertions passed');
