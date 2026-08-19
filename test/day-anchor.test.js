// Rules test for the Day N anchor.  Run:  node test/day-anchor.test.js
//
// Extracts the real resolveDayAnchor straight out of index.js rather than copying
// it, so the test can never drift from shipped behaviour. index.js boots the Slack
// app on require, hence the extract-and-eval approach.
//
// What this pins down: the 14-day Build & Release clock must start at the REAL
// activation call date, read from the dashboard CRM. It used to start at the
// portal's "Activation Call Completed" checkbox, which is a fulfillment tick made
// days after the call — measured across 16 clients it lagged by a median of 6 days
// and by up to 40 (Hidro Vital: called May 15, ticked June 24). Every one of those
// days was invisible, so an overdue client still read as on-track. Cases 3 and 4
// are that bug.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('function resolveDayAnchor'),
  SRC.indexOf('// ─── PORTAL: CLIENT STATUS'),
);
const resolveDayAnchor = new Function(`${block}; return resolveDayAnchor;`)();

// Fixed "now" so day counts are deterministic. All fixtures are dated against it.
const NOW = Date.parse('2026-08-11T18:00:00.000Z');
const _realNow = Date.now;
Date.now = () => NOW;

const ACTIVATION_TEMPLATE = 't-activation';
const titleOf = id => (id === ACTIVATION_TEMPLATE ? 'Activation Call Completed' : 'Some Other Step');
const checkbox = completed_at => [{ template_id: ACTIVATION_TEMPLATE, completed_at }];

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// 1. Real activation date wins over the checkbox — the whole point of the change.
{
  const a = resolveDayAnchor(
    { customer_status: 'phase_1', created_at: '2026-07-01T00:00:00Z' },
    checkbox('2026-08-05T00:00:00Z'),
    titleOf,
    '2026-07-28T12:00:00-06:00',
  );
  check('1 real date beats checkbox', [a.anchorDate, a.label], ['2026-07-28', 'since activation call']);
}

// 2. No real date → checkbox fallback, and the label SAYS it is the checkbox so a
//    reader knows the number is soft.
{
  const a = resolveDayAnchor(
    { customer_status: 'phase_1', created_at: '2026-07-01T00:00:00Z' },
    checkbox('2026-08-05T12:00:00Z'),
    titleOf,
    null,
  );
  check('2 checkbox fallback is labelled', [a.anchorDate, a.label], ['2026-08-05', 'since activation call (checkbox)']);
}

// 3. THE BUG, in numbers. Hidro Vital's real call was 2026-05-15; the checkbox was
//    ticked 2026-06-24 — 40 days later. Anchored on the checkbox the client reads
//    as Day 48; the truth is Day 88. Both are past 14, but the same 40-day gap on a
//    fresher client is the difference between "Day 6, fine" and "Day 46, overdue".
{
  const dash = { customer_status: 'phase_1', created_at: '2026-05-01T00:00:00Z' };
  const real = resolveDayAnchor(dash, checkbox('2026-06-24T00:00:00Z'), titleOf, '2026-05-15T12:00:00-06:00');
  const old  = resolveDayAnchor(dash, checkbox('2026-06-24T00:00:00Z'), titleOf, null);
  check('3 real anchor counts the lost days', [real.daysSince, old.daysSince, real.daysSince - old.daysSince], [88, 48, 40]);
}

// 4. Overdue detection flips. A client called 20 days ago whose checkbox was ticked
//    5 days ago is past the 14-day promise, but the checkbox anchor calls them Day 5.
{
  const dash = { customer_status: 'phase_2', created_at: '2026-07-01T00:00:00Z' };
  const real = resolveDayAnchor(dash, checkbox('2026-08-06T12:00:00Z'), titleOf, '2026-07-22T12:00:00Z');
  const old  = resolveDayAnchor(dash, checkbox('2026-08-06T12:00:00Z'), titleOf, null);
  check('4 overdue is visible only with the real date', [real.daysSince >= 14, old.daysSince >= 14], [true, false]);
}

// 5. phase_3 still anchors on stabilization start — the activation date must NOT
//    hijack stabilization day counts.
{
  const a = resolveDayAnchor(
    { customer_status: 'phase_3', stabilization_started_at: '2026-08-01T12:00:00Z', created_at: '2026-05-01T12:00:00Z' },
    checkbox('2026-06-24T12:00:00Z'),
    titleOf,
    '2026-05-15T12:00:00-06:00',
  );
  check('5 phase_3 keeps stabilization anchor', [a.anchorDate, a.label], ['2026-08-01', 'since stabilization start']);
}

// 6. No activation data at all → portal creation, unchanged legacy behaviour.
{
  const a = resolveDayAnchor({ customer_status: 'phase_1', created_at: '2026-08-01T12:00:00Z' }, [], titleOf, null);
  check('6 falls back to portal creation', [a.anchorDate, a.label], ['2026-08-01', 'since portal creation']);
}

// 7. Nothing to anchor on at all — must not throw or invent a date.
{
  const a = resolveDayAnchor({ customer_status: 'phase_1' }, [], titleOf, null);
  check('7 no anchor returns nulls', [a.daysSince, a.anchorDate, a.label], [null, null, 'no anchor date']);
}

// 8. A date-only value stored at midday CR must not roll back a day when rendered.
//    This is why the sync writes T12:00:00-06:00 rather than bare YYYY-MM-DD.
{
  const a = resolveDayAnchor({ customer_status: 'phase_1' }, [], titleOf, '2026-08-20T12:00:00-06:00');
  check('8 midday CR does not roll the date back', a.anchorDate, '2026-08-20');
}

// 9. Documented edge: a bare midnight-UTC timestamp renders as the PREVIOUS day in
//    Costa Rica (UTC-6). Anchors are printed in CR deliberately, so an evening-UTC
//    stamp cannot show tomorrow's date to the team. This is why the activation sync
//    stores midday CR instead of a bare date — see case 8.
{
  const a = resolveDayAnchor({ customer_status: 'phase_1' }, [], titleOf, '2026-08-20T00:00:00Z');
  check('9 midnight UTC renders as the prior CR day', a.anchorDate, '2026-08-19');
}

// 10. The CRM stores a real timestamp, so the anchor must render the same calendar
//     day the call was held on. Maryia Proskouriakov's activation call was held
//     2026-08-14; whatever the CRM hands over must not drift a day in either
//     direction when printed in Costa Rica.
{
  const a = resolveDayAnchor({ customer_status: 'phase_1' }, [], titleOf, '2026-08-14T12:00:00-06:00');
  check('10 held date renders as the day it happened', a.anchorDate, '2026-08-14');
}

Date.now = _realNow;

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
