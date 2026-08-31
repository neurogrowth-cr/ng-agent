// Rules test for the Sales EOD booked-calls window.  Run:  node test/eod-booked-window.test.js
//
// Pins down the 9PM-to-9PM CR digest window that replaced the CR-calendar-day
// window. The calendar-day version had a dead zone: a call booked 21:00–24:00 CR
// landed after that evening's digest ran and before the next day's window began,
// so NO digest ever counted it. Sebastian booked a call at 22:31 CR on
// 2026-08-27 and the 08-28 digest said 3 while he counted 4 — that call is
// case 3 below. Extract-and-eval so the test can never drift from shipped code.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('function computeEodBookedWindow'),
  SRC.indexOf('// ─── END SALES EOD BOOKED-CALLS WINDOW'),
);
const computeEodBookedWindow = new Function(`${block}; return computeEodBookedWindow;`)();

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });

// CR is UTC-6, no DST. 21:00 CR == 03:00Z next day.
const CR = (y, mo, d, h, mi) => Date.UTC(y, mo - 1, d, h + 6, mi, 0);

// 1. Canonical run: 21:00:30 CR → window starts exactly yesterday 21:00 CR.
{
  const w = computeEodBookedWindow(CR(2026, 8, 28, 21, 0) + 30000);
  check('digest run at 21:00 covers back to yesterday 21:00 CR',
    w.startMs === CR(2026, 8, 27, 21, 0) && w.endMs === CR(2026, 8, 28, 21, 0) + 30000,
    { start: new Date(w.startMs).toISOString() });
}

// 2. Morning ad-hoc run (10:00 CR) anchors to the same boundary — never to midnight.
{
  const w = computeEodBookedWindow(CR(2026, 8, 28, 10, 0));
  check('10:00 CR run starts at yesterday 21:00 CR', w.startMs === CR(2026, 8, 27, 21, 0));
}

// 3. THE DEAD-ZONE CASE. Booked 22:31 CR on Aug 27; next digest runs 21:02 on Aug 28.
{
  const bookedAt = CR(2026, 8, 27, 22, 31);
  const w = computeEodBookedWindow(CR(2026, 8, 28, 21, 2));
  check('a 22:31 CR booking is inside the NEXT evening digest window',
    bookedAt >= w.startMs && bookedAt <= w.endMs);
}

// 4. Just after midnight CR the anchor is still the previous evening, not today's.
{
  const w = computeEodBookedWindow(CR(2026, 8, 29, 0, 10));
  check('00:10 CR run starts at the 21:00 CR that just passed', w.startMs === CR(2026, 8, 28, 21, 0));
}

// 5. No gap between consecutive daily digests, even with cron drift.
{
  const w1 = computeEodBookedWindow(CR(2026, 8, 28, 21, 7)); // yesterday ran 7 min late
  const w2 = computeEodBookedWindow(CR(2026, 8, 29, 21, 1));
  check('consecutive digest windows overlap — never a hole', w1.endMs >= w2.startMs);
}

// 6. Month boundary: Sep 1 run reaches back into Aug 31.
{
  const w = computeEodBookedWindow(CR(2026, 9, 1, 21, 3));
  check('month boundary: Sep 1 digest starts Aug 31 21:00 CR', w.startMs === CR(2026, 8, 31, 21, 0));
}

const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ' ' + JSON.stringify(c.detail || {})}`);
console.log(`${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
