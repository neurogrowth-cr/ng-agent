// Rules test for the lead-to-close block in the Friday delivery delta.
// Run:  node test/lead-to-close.test.js
//
// lib/leadToClose.js is pure, so it is required directly. All rows below are
// synthetic (public repo, never prod rows).
const ltc = require('../lib/leadToClose');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// Windows: Friday 2026-10-16 16:00 CR = 22:00Z.
const W = ltc.monthWindows(new Date('2026-10-16T22:00:00Z'));
check('mtd key', W.mtd.key, '2026-10');
check('mtd starts 00:00 CR on the 1st', W.mtd.startIso, '2026-10-01T06:00:00.000Z');
check('mtd ends 00:00 CR on Nov 1', W.mtd.endIso, '2026-11-01T06:00:00.000Z');
check('mtd day is the CR day', W.mtd.day, 16);
check('prev is September', [W.prev.key, W.prev.name, W.prev.startIso], ['2026-09', 'Sep', '2026-09-01T06:00:00.000Z']);
const J = ltc.monthWindows(new Date('2026-01-02T22:00:00Z'));
check('January wraps to last December', J.prev.key, '2025-12');
check('a UTC timestamp just after midnight UTC on the 1st is still last month in CR', ltc.crMonthKey('2026-10-01T03:00:00Z'), '2026-09');

// Lead channel buckets.
check('fb_form channel is the form', ltc.leadBucket({ source: 'Facebook', channel: 'fb_form' }), 'form');
check('old Facebook row with no channel is the form', ltc.leadBucket({ source: 'Facebook', channel: null }), 'form');
check('instagram DM is setter/DM', ltc.leadBucket({ source: 'Paid Social', channel: 'instagram' }), 'dm');
check('Social media with no channel is setter/DM', ltc.leadBucket({ source: 'Social media' }), 'dm');
check('whatsapp is setter/DM', ltc.leadBucket({ source: 'Social media', channel: 'whatsapp' }), 'dm');
check('self-serving calendar source is self-serve', ltc.leadBucket({ source: 'LinkedIn Flywheel - Self Serving', channel: 'other' }), 'self');
check('LinkedIn appointment calendar source is self-serve', ltc.leadBucket({ source: 'ONLY RON - LinkedIn Flywheel - Appointment' }), 'self');

// Booked call channel.
check('the lead card decides', ltc.apptBucket({ setterId: 's@x', leadSource: 'Facebook', leadRowBucket: 'dm' }), 'dm');
check('no card and no setter booked itself', ltc.apptBucket({ setterId: null, leadSource: 'Facebook' }), 'self');
check('no card, setter, Facebook source is the form', ltc.apptBucket({ setterId: 's@x', leadSource: 'Facebook' }), 'form');
check('no card, setter, other source is setter/DM', ltc.apptBucket({ setterId: 's@x', leadSource: 'ghl' }), 'dm');

// Country from the booking phone.
check('+506 is Costa Rica', ltc.countryFromPhone('+506 8888 0000'), 'CR');
check('+52 is Mexico', ltc.countryFromPhone('+52 55 1234 5678'), 'MX');
check('0052 is Mexico', ltc.countryFromPhone('0052 55 1234 5678'), 'MX');
check('+57 is other', ltc.countryFromPhone('+57 300 000 0000'), 'Other');
check('bare 8 digits are not guessed', ltc.countryFromPhone('83160000'), 'Other');
check('bare 506 with full length is Costa Rica', ltc.countryFromPhone('50688880000'), 'CR');
check('empty phone is unknown', ltc.countryFromPhone(''), 'Unknown');

// Tally.
const leads = [
  ...Array(40).fill({ month: '2026-10', bucket: 'form' }),
  ...Array(10).fill({ month: '2026-10', bucket: 'dm' }),
  ...Array(100).fill({ month: '2026-09', bucket: 'form' }),
  { month: '2026-08', bucket: 'form' }, // outside the two months: ignored
];
const a = (month, bucket, country, cls, won = false) => ({ month, bucket, country, cls, won });
const appts = [
  a('2026-10', 'form', 'CR', 'showed', true),
  a('2026-10', 'form', 'CR', 'showed'),
  a('2026-10', 'form', 'MX', 'no_show'),
  a('2026-10', 'dm', 'MX', 'pending'),
  a('2026-10', 'self', 'Other', 'showed'),
  a('2026-10', 'self', 'Unknown', 'no_show'),
  a('2026-09', 'form', 'CR', 'showed', true),
  a('2026-09', 'form', 'CR', 'showed', true),
  a('2026-09', 'form', 'MX', 'showed'),
  a('2026-09', 'form', 'MX', 'no_show'),
];
const T = ltc.tallyFunnel({ leads, appts, months: ['2026-09', '2026-10'] });
check('October total', T['2026-10'].total, { leads: 50, booked: 6, held: 3, noShow: 2, pending: 1, won: 1 });
check('September total', T['2026-09'].total, { leads: 100, booked: 4, held: 3, noShow: 1, pending: 0, won: 2 });
check('October form', T['2026-10'].channel.form, { leads: 40, booked: 3, held: 2, noShow: 1, pending: 0, won: 1 });
check('October self-serve booked without leads', T['2026-10'].channel.self, { leads: 0, booked: 2, held: 1, noShow: 1, pending: 0, won: 0 });
check('October Mexico', T['2026-10'].country.MX, { leads: 0, booked: 2, held: 0, noShow: 1, pending: 1, won: 0 });

// Rates: show excludes pending; lead rates blank when booked outruns leads.
check('October rates', ltc.rates(T['2026-10'].total), { leadToBook: '12%', show: '60%', close: '33%', leadToClose: '2.0%' });
check('self-serve lead rates are blank', ltc.rates(T['2026-10'].channel.self), { leadToBook: '-', show: '50%', close: '0%', leadToClose: '-' });
check('sub-1% keeps two decimals', ltc.pct(11, 1691), '0.65%');
check('zero denominator is a dash', ltc.pct(0, 0), '-');

// Format.
const msg = ltc.formatLeadToClose(T, W);
check('headline names the delta', msg.split('\n')[1],
  'Lead to close is 2.0% so far in Oct (1 win from 50 leads) against 2.0% in Sep (2 wins from 100).');
check('funnel table row for all, month to date',
  msg.includes('All         Oct 1-16     50       6     3    1   12%   60%    33%  2.0%'), true);
check('two tables', (msg.match(/```/g) || []).length, 4);
check('pending is called out', msg.includes('1 Oct call has no outcome yet'), true);
check('self-serve gap is explained', msg.includes('Self-serve calls often book without a lead card'), true);
check('no-phone row shows when a booking has no phone', msg.includes('No phone'), true);
check('no em dash in copy', /\u2014/.test(msg), false);
const noUnknown = ltc.formatLeadToClose(ltc.tallyFunnel({ leads, appts: appts.filter(x => x.country !== 'Unknown'), months: ['2026-09', '2026-10'] }), W);
check('no-phone row hides when empty', noUnknown.includes('No phone'), false);
check('missing month renders nothing', ltc.formatLeadToClose({}, W), '');

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall lead-to-close checks passed');
