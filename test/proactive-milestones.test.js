// Rules test for the launch-window warning thresholds.
// Run:  node test/proactive-milestones.test.js
//
// Extracts the real proactiveMilestoneDue straight out of index.js rather than
// copying it, so the test can never drift from shipped behaviour. index.js boots
// the Slack app on require, hence the extract-and-eval approach.
//
// What this pins down: the Day-7 and Day-14 warnings used to fire on EXACT day
// equality (daysSince === 13). Any day the job did not run — a deploy, an outage,
// a weekend, or the cron sitting disabled as it is today — was a warning nobody
// ever received, and nothing distinguished a missed warning from a client who
// never needed one. Cases 3 and 4 are that bug.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// BEGIN PROACTIVE-DM PURE HELPERS'),
  SRC.indexOf('// END PROACTIVE-DM PURE HELPERS'),
);
const proactiveMilestoneDue = new Function(`${block}; return proactiveMilestoneDue;`)();

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// 1. Nothing owed before the at-risk threshold.
check('1 quiet before day 6', [0, 3, 5].map(d => proactiveMilestoneDue(d, false, false)), [null, null, null]);

// 2. Day 7 warning arms at Day 6 (the day before the threshold).
check('2 day7 arms at 6', proactiveMilestoneDue(6, false, false), 'day7');

// 3. THE BUG. A job that misses a day used to skip the warning forever. Every day
//    from 6 up to 12 must still owe the Day-7 warning if it was never sent.
check('3 day7 still owed after missed runs',
  [7, 9, 12].map(d => proactiveMilestoneDue(d, false, false)),
  ['day7', 'day7', 'day7']);

// 4. Same for the deadline warning — owed from 13 onward, not only ON 13.
check('4 day14 still owed after missed runs',
  [13, 14, 20, 47].map(d => proactiveMilestoneDue(d, false, true)),
  ['day14', 'day14', 'day14', 'day14']);

// 5. Already notified → silence. This is what stops a daily re-nag.
check('5 notified milestones stay quiet',
  [proactiveMilestoneDue(8, false, true), proactiveMilestoneDue(15, true, false)],
  [null, null]);

// 6. A client who crossed BOTH while the job was down gets the urgent one only —
//    not two DMs about the same client in the same run.
check('6 day14 wins over an unsent day7', proactiveMilestoneDue(20, false, false), 'day14');

// 7. ...and once the deadline warning is sent, the stale Day-7 does not leak out
//    behind it. Past day 13 the Day-7 branch is unreachable by construction.
check('7 no day7 backfill once past the deadline', proactiveMilestoneDue(20, true, false), null);

// 8. A client with no usable anchor yields a non-finite daysSince — must return
//    null rather than NaN-comparing its way into a spurious warning.
check('8 unanchored clients are not warned about',
  [proactiveMilestoneDue(NaN, false, false), proactiveMilestoneDue(undefined, false, false)],
  [null, null]);

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
