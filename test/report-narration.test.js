// Narration guard for scheduled reports.  Run:  node test/report-narration.test.js
//
// Positives are the real Sonnet 5 shadow outputs that passed the header check
// on 2026-10-07 and 2026-10-09 while narrating the model's process. Negatives
// are real reports Max posts, including prospect quotes, so the denylist is
// proven not to bite ordinary text.
const fs = require('fs');
const path = require('path');
const { findNarration, cutToFirstHeader, NARRATION_PATTERNS } = require('../lib/reportNarration');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── 1. Real narration from the Sonnet 5 shadow (positives)
const eodOct7 = `The middle section is elided both times. I have enough to make a sound call on the visible conversations — the truncated middle appears to contain older/stale items similar in pattern. Based on what's fully visible, I'll finalize with what I can verify.

SALES EOD REPORT — October 7

LEADS TODAY

Total new leads today: 12`;
const cancelOct9 = `Jonathan Madriz departed 2026-07-19 and isn't showing up in this week's data at all — consistent with the fact that he's no longer on the team and shouldn't be appearing in any current-week stats.

For Jose Carranza this week (Oct 4–9): 1 call booked, 1 attended, 0 no-shows/cancellations logged. Cancellation rate is 0/1 = 0%, nowhere near the 50% threshold.`;
const wrapOct9 = `Franciny Vargas is confirmed as a long-standing block (Day 133, flagged since 9/29 as "long-standing, no recent activity") — excluding per the block filtering rule. Edwin Ulloa (Day 72, Phase 0 overdue since well past 14 days, no recent activity noted) also qualifies for exclusion.`;
check('1a  Oct 7 Sales EOD opening is narration', findNarration(eodOct7) !== null, true);
check('1b  Oct 9 cancellation alert is narration', findNarration(cancelOct9), 'consistent with the fact that');
check('1c  Oct 9 wrap-up is narration', findNarration(wrapOct9), 'excluding per the');
check('1d  "I\'ll finalize with what I can verify" alone is caught', findNarration("I'll finalize with what I can verify."), "I'll finalize");
check('1e  "based on what is visible" alone is caught', findNarration('Based on what is visible, 3 leads.'), 'Based on what is visible');
check('1f  "I do not have access" is caught', findNarration('I do not have access to the calendar tool.'), 'I do not have access');

// ── 2. Real clean reports (negatives), including quoted prospect text
const eodOct10 = `SALES EOD REPORT — October 10

LEADS TODAY
Total new leads today: 7
SEBASTIAN S: 5
OSCAR M: 2
Unclaimed (no setter has claimed yet): 0
Claim speed:
Claimed within 15 min: 4 of 7 (57%). Target 80%.
Median time to claim: 10 min. Prior 7 days: 5 min, 65% within 15 min.

STRATEGY CALLS BOOKED
Total booked today: 0
None booked today.

WORKED VS UNWORKED
Worked and moved forward: 15
About to book, pending on our side: 0`;
const roster = `DAILY CALL ROSTER — Sat Oct 10\n\nNo calls booked today.\n\nNothing on Jose's, Ron's, or Jonathan's calendar today.`;
const hotReplies = `Oscar, 3 unanswered replies waiting over an hour:

Emi AFer, Instagram, waiting ~6.5 hrs. Last message: "Está vigilando todos los movimientos."

Lourdes Yáñez, WhatsApp, waiting ~21.5 hrs. Last message: "Gracias por acompañarnos hoy. Nos vemos mañana a las 11."

Mark T, WhatsApp, waiting ~2 hrs. Last message: "Sounds good, let me know what time works and I'll send the details. I'm going to be in meetings until 3."`;
const digest = `WEEKLY OPS DIGEST — Week of October 5–9, 2026

DELIVERY
47 active clients, 35 live, 6 in build across Phase 1-3. Four clients blew past the 14-day activation window this week.

SALES
12 calls booked, 9 held, 2 won. Show rate 75% against 60% the prior week.

WHAT NEEDS YOUR ATTENTION
Cinthya Diaz UTEP is still unresolved from September's churn-risk flag.`;
check('2a  Oct 10 Sales EOD is clean', findNarration(eodOct10), null);
check('2b  call roster is clean', findNarration(roster), null);
check('2c  hot replies DM with a prospect saying "let me know" and "I\'m going to" is clean', findNarration(hotReplies), null);
check('2d  weekly ops digest is clean', findNarration(digest), null);
check('2e  empty text is clean', findNarration(''), null);
check('2f  every pattern is case-insensitive', NARRATION_PATTERNS.every((re) => re.flags.includes('i')), true);

// ── 3. Cut to the first declared header
const headers = ['LEADS TODAY', 'STRATEGY CALLS BOOKED', 'WORKED VS UNWORKED', "TOMORROW'S PRIORITY"];
const cut = cutToFirstHeader(eodOct7, ['SALES EOD REPORT', ...headers]);
check('3a  narration before the first header is cut, header line kept', cut.startsWith('SALES EOD REPORT — October 7'), true);
check('3b  nothing narrated survives the cut', findNarration(cut), null);
check('3c  a report that already starts at a header is unchanged', cutToFirstHeader(eodOct10, headers), eodOct10);
check('3d  header wrapped in backticks keeps its line', cutToFirstHeader('I have enough to work with.\n`LEADS TODAY`\nTotal: 3', headers), '`LEADS TODAY`\nTotal: 3');
check('3h  a title before the first header is kept when it is not narration', cutToFirstHeader('SALES EOD REPORT\nLEADS TODAY\nTotal: 3', headers), 'SALES EOD REPORT\nLEADS TODAY\nTotal: 3');
check('3e  no header found leaves the text alone (validation fails on headers instead)', cutToFirstHeader('Just prose.', headers), 'Just prose.');
check('3f  tasks without headers are left alone', cutToFirstHeader(cancelOct9, []), cancelOct9);
check('3g  earliest header wins, whatever the list order', cutToFirstHeader("I'll finalize now.\nWORKED VS UNWORKED\nLEADS TODAY", headers).startsWith('WORKED VS UNWORKED'), true);

// ── 4. Wiring in index.js: the validator checks narration, both the real and
// the shadow reply are cut, and the output rule is in the prompt.
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
check('4a  validateFinalReport checks narration after structure', /function validateFinalReport\(text, taskName\) \{[\s\S]*?findNarration/.test(SRC), true);
check('4b  the real reply is cut to its first header before validation', /reply = reportNarration\.cutToFirstHeader\(reply, TASK_HEADERS\[task\.name\]/.test(SRC), true);
check('4c  the shadow reply is cut the same way', /const shadowReply = reportNarration\.cutToFirstHeader\(/.test(SRC), true);
check('4d  the prompt carries the output rule', SRC.includes('OUTPUT RULE: Return only the finished report.'), true);
check('4e  no em dashes in the library', /—/.test(fs.readFileSync(path.join(__dirname, '..', 'lib', 'reportNarration.js'), 'utf8')), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall checks passed');
