// Rules test for how long Max keeps asking about an unlogged call.
// Run:  node test/outcome-reminder-window.test.js
//
// The old 14-day floor let four Sep 2-3 2026 calls age out with no outcome: Max
// stopped asking, and they sat on the Setter Leaderboard as "pending" with
// nobody responsible. Ron's rule (2026-09-21): a missing outcome is a data gap
// for 90 days and Max keeps reminding. Fresh calls nightly, older ones weekly.
// Extract-and-eval so the test can never drift from shipped code.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const from = SRC.indexOf('const OUTCOME_REMINDER_WINDOW_DAYS');
const block = SRC.slice(from, SRC.indexOf('async function runUnloggedOutcomeReminders', from));
const { isOutcomeReminderDueTonight, OUTCOME_REMINDER_WINDOW_DAYS } =
  new Function(`${block}; return { isOutcomeReminderDueTonight, OUTCOME_REMINDER_WINDOW_DAYS };`)();

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });
const NOW = Date.parse('2026-09-22T03:00:00.000Z'); // 9 PM CR
const daysAgo = d => NOW - d * 86400000;

check('the window is 90 days', OUTCOME_REMINDER_WINDOW_DAYS === 90);
check('a 2-day-old call is asked about on any night', isOutcomeReminderDueTonight(daysAgo(2), NOW, 'Thursday'));
check('a 14-day-old call is still nightly', isOutcomeReminderDueTonight(daysAgo(14), NOW, 'Thursday'));
check('a 19-day-old call is quiet on a Thursday', !isOutcomeReminderDueTonight(daysAgo(19), NOW, 'Thursday'));
check('a 19-day-old call is asked about on Monday', isOutcomeReminderDueTonight(daysAgo(19), NOW, 'Monday'));
check('an 89-day-old call is still asked about on Monday', isOutcomeReminderDueTonight(daysAgo(89), NOW, 'Monday'));
check('a 91-day-old call is out of the window, even on Monday', !isOutcomeReminderDueTonight(daysAgo(91), NOW, 'Monday'));
check('the run-once drain asks about aged calls on any night', isOutcomeReminderDueTonight(daysAgo(40), NOW, 'Thursday', true));
check('the drain still respects the 90-day window', !isOutcomeReminderDueTonight(daysAgo(91), NOW, 'Thursday', true));
check('the nightly run floors on the 90-day constant, not a literal 14',
  /OUTCOME_REMINDER_WINDOW_DAYS \* 24 \* 60 \* 60 \* 1000/.test(SRC.slice(SRC.indexOf('async function runUnloggedOutcomeReminders'))));

const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ' ' + JSON.stringify(c.detail || {})}`);
console.log(`${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
