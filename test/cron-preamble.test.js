// A scheduled task's reply is posted to the channel VERBATIM, so model
// narration written before the deliverable reaches the whole team. This tests
// the machine-checked guard against that, using the REAL message that leaked
// into #ng-sales-goats on 2026-08-18 as the fixture.
//
// Same extraction trick as the other tests: the block is sliced out of
// index.js and compiled with new Function, so this can never drift from
// shipped behaviour.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const TASK_LEAD_ANCHOR'),
  SRC.indexOf('function registerDynamicCron'),
);
const g = new Function(`${block}; return { TASK_LEAD_ANCHOR, trimToLeadAnchor };`)();

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}

const TASK = 'Daily Closer Outcome Reminder';
const DELIVERABLE = '<!subteam^S0BMAC61Q0G> Quick reminder before you sign off — 5 of today\'s 6 calls still need an outcome logged.';

console.log('trimToLeadAnchor — the 2026-08-18 leak');
// Verbatim shape of what posted to #ng-sales-goats.
const leaked = `6 calls today. Alberto García is already marked no-show (attendance already handled by REVI/Max). The remaining 5 still show as "scheduled" — meaning no outcome has been logged yet for them.

Here is the final reminder text:

---

${DELIVERABLE}`;
check('the real leaked reply is trimmed to the deliverable', g.trimToLeadAnchor(leaked, TASK), DELIVERABLE);
check('narration is gone', /Alberto García is already marked/.test(g.trimToLeadAnchor(leaked, TASK)), false);
check('the handoff phrase is gone', /Here is the final reminder text/.test(g.trimToLeadAnchor(leaked, TASK)), false);
check('the separator is gone', /^---/m.test(g.trimToLeadAnchor(leaked, TASK)), false);

console.log('trimToLeadAnchor — must not damage good output');
check('an already-clean reply is untouched', g.trimToLeadAnchor(DELIVERABLE, TASK), DELIVERABLE);
check('a task with no declared anchor is untouched', g.trimToLeadAnchor(leaked, 'Sales EOD Report'), leaked);
check('an unknown task is untouched', g.trimToLeadAnchor(leaked, 'Some Future Task'), leaked);

// An absent anchor is a DIFFERENT failure — the reply is not the deliverable at
// all. Swallowing it here would hide it from validateFinalReport's re-prompt.
const noAnchor = 'I was unable to reach the sales intelligence tool, so I have nothing to report.';
check('a reply with no anchor is passed through, not blanked', g.trimToLeadAnchor(noAnchor, TASK), noAnchor);

console.log('trimToLeadAnchor — edges');
check('empty string', g.trimToLeadAnchor('', TASK), '');
check('null never throws', g.trimToLeadAnchor(null, TASK), null);
check('undefined never throws', g.trimToLeadAnchor(undefined, TASK), undefined);
check('leading whitespace before the anchor is trimmed', g.trimToLeadAnchor(`\n\n  ${DELIVERABLE}`, TASK), DELIVERABLE);
check('only the FIRST anchor occurrence anchors the cut',
  g.trimToLeadAnchor(`blah ${DELIVERABLE} and again <!subteam^X> tail`, TASK),
  `${DELIVERABLE} and again <!subteam^X> tail`);

console.log('TASK_LEAD_ANCHOR — the map itself');
check('the renamed nudge is covered', g.TASK_LEAD_ANCHOR[TASK], '<!subteam^');
check('the OLD task name is deliberately absent (renamed 2026-08-18)',
  g.TASK_LEAD_ANCHOR['Daily Closer Attendance & Outcome Reminder'], undefined);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll cron-preamble tests passed.');
