// A report's identity is assigned when it is POSTED, never inferred from its
// text. Run:  node test/report-lessons.test.js
//
// Three defects this pins, all found 2026-08-27 with agent_knowledge holding
// zero report-feedback rows since April:
//
//   A. Lesson extraction only ran on `app_mention`, and Slack never delivers
//      app_mention for a DM.
//   B. The setter and closer briefs ARE DMs, so their two getReportLessons
//      reads were structurally guaranteed empty forever.
//   C. inferReportId() sniffed the report text. Run against Max's real
//      headers, DAILY CALL ROSTER / SETTER LEADERBOARD / SALES EOD REPORT /
//      PIPELINE AUTO-MOVER / WEEKLY CLOSER COMPARISON / CANCELLATION RATE all
//      fell through to the channel name — six reports, one lesson bucket.
//
// The first test below is the one that matters most: it makes defect B
// impossible to reintroduce by proving every id anyone READS has a writer.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}

console.log('every report id that is READ must be WRITTEN by some post site');
const ids = (re) => [...SRC.matchAll(re)].map(m => m[1]).sort();
const readIds  = ids(/getReportLessons\(\s*(reportIdForTask\(task\.name\)|'[^']+')\s*\)/g);
const writeIds = ids(/registerPostedReport\(\s*[A-Za-z_$][\w$]*\s*,\s*(reportIdForTask\(task\.name\)|'[^']+')\s*\)/g);
const orphanReads  = [...new Set(readIds)].filter(i => !writeIds.includes(i));
const orphanWrites = [...new Set(writeIds)].filter(i => !readIds.includes(i));

check('at least the 6 known reports are read', readIds.length >= 6, true);
// Set-comparison alone misses a single read regressing to channel scope, since
// the other dynamic-cron read keeps the task id in the set. Pin the count and
// ban channel variables outright.
check('BOTH dynamic-cron reads are scoped to the task, not the channel',
  (SRC.match(/getReportLessons\(reportIdForTask\(task\.name\)\)/g) || []).length, 2);
check('no getReportLessons call takes a channel',
  /getReportLessons\(\s*(taskChannel|targetChannel|channelName)/.test(SRC), false);
check('DEFECT B — no read id lacks a writer', orphanReads, []);
check('no write id is never read (dead registration)', orphanWrites, []);
for (const id of ["'sales-standup-setter'", "'sales-standup-closer'"]) {
  check(`${id} (a DM brief) now has a registered writer`, writeIds.includes(id), true);
}

console.log('\nDEFECT C — inferReportId is gone; nothing sniffs report text');
check('the function is deleted', /function inferReportId\s*\(/.test(SRC), false);
// It may still be NAMED in the comment explaining why it was removed; what must
// not survive is a call. Only comment lines are allowed to mention it.
check('no caller survives — only the explanatory comment mentions it',
  SRC.split('\n').filter(l => l.includes('inferReportId')).every(l => l.trim().startsWith('//')), true);
check('identity comes from the registry instead', /await lookupReportId\(/.test(SRC), true);

console.log('\nDEFECT A — extraction is no longer gated on an @mention alone');
check('the lesson block runs for tagged OR feedback-shaped replies',
  /\(tagged \|\| learnFromFeedback\) && maxRootedThread/.test(SRC), true);
check('the DM handler can set the gate',
  /const dmThread = await buildThreadContext\([\s\S]{0,400}?learnFromFeedback:/.test(SRC), true);
check('the channel handler can set the gate',
  /const chThread = await buildThreadContext\([\s\S]{0,400}?learnFromFeedback:/.test(SRC), true);
check('the gate reuses CORRECTION_HINT_RE rather than a second regex',
  (SRC.match(/learnFromFeedback: CORRECTION_HINT_RE\.test/g) || []).length, 2);
// Structural, not distance-based: lesson extraction must sit OUTSIDE and BEFORE
// the tagged-only block, and extractClientContext (an extra LLM call) must stay
// inside it.
const iLesson = SRC.indexOf('(tagged || learnFromFeedback) && maxRootedThread');
const iTagged = SRC.indexOf('if (tagged) {');
const iClient = SRC.indexOf('await extractClientContext(');
check('lesson extraction runs before, and outside, the tagged-only block',
  iLesson > 0 && iTagged > iLesson, true);
check('the expensive tagged-only side effects stay behind an explicit mention',
  iClient > iTagged, true);

console.log('\nreport identity helpers (sliced from index.js)');
const block = SRC.slice(SRC.indexOf('const REPORT_POST_CATEGORY'), SRC.indexOf('function registerReportPost'));
const g = new Function(`${block}; return { slugifyReportName, reportIdForTask, reportPostKey, REPORT_POST_CATEGORY };`)();

// The 10 live rows in scheduled_tasks on 2026-08-27.
const TASKS = ['Daily Sales Call Roster','Setter Leaderboard','Sales EOD Report','Fulfillment EOD Pulse',
  'Monthly Business Review','Ron Weekly Ops Digest','Friday Delivery Wrap-Up','Weekly Closer Comparison',
  'Cancellation Rate Alert','Daily Closer Outcome Reminder'];
const slugs = TASKS.map(g.reportIdForTask);
check('all 10 live task names produce distinct ids', new Set(slugs).size, 10);
check('ids are namespaced so a task can never collide with a channel name',
  slugs.every(s => s.startsWith('task:')), true);
check('the six #ng-sales-goats reports no longer share a bucket',
  new Set(['Daily Sales Call Roster','Setter Leaderboard','Sales EOD Report','Weekly Closer Comparison','Cancellation Rate Alert']
    .map(g.reportIdForTask)).size, 5);
check('a representative slug', g.reportIdForTask('Daily Sales Call Roster'), 'task:daily-sales-call-roster');
check('punctuation and case collapse', g.reportIdForTask('Friday Delivery Wrap-Up'), 'task:friday-delivery-wrap-up');
check('accents are folded, not dropped into garbage', g.reportIdForTask('Revisión Semanal'), 'task:revision-semanal');
check('renaming a task changes its id (documented, not a bug)',
  g.reportIdForTask('Daily Sales Call Roster') === g.reportIdForTask('Daily Sales Call Roster v2'), false);
check('empty name does not throw', g.reportIdForTask(''), 'task:');

console.log('\nregistry key: what is written is what is queried');
check('key is channel:ts', g.reportPostKey('C0AJANQBYUE', '1787947786.150859'), 'C0AJANQBYUE:1787947786.150859');
check('the write uses reportPostKey', /key: reportPostKey\(channel, ts\)/.test(SRC), true);
check('the read uses reportPostKey too', /\.eq\('key', reportPostKey\(channel, threadTs\)\)/.test(SRC), true);
check('both sides scope to the same category',
  (SRC.match(/category', REPORT_POST_CATEGORY|category: REPORT_POST_CATEGORY/g) || []).length >= 3, true);

console.log('\nfailure behaviour');
check('an unregistered root falls back to the channel, it does not drop the lesson',
  /return fallbackChannel \|\| 'general-report';/.test(SRC), true);
check('and the miss is logged rather than silent',
  /lookupReportId: no registry row for/.test(SRC), true);
check('registration failure cannot take down a report post',
  /registerReportPost\([\s\S]{0,900}?\.catch\(err => console\.error/.test(SRC), true);
check('a post result with no ts is reported, not silently skipped',
  /no channel\/ts on the post result/.test(SRC), true);
check('the registry is pruned so it cannot grow forever',
  /\.eq\('category', REPORT_POST_CATEGORY\)[\s\S]{0,80}\.lt\('updated_at', cutoff\)/.test(SRC), true);

console.log('\nthe feedback gate fires on corrections, not acknowledgements');
const reLine = SRC.match(/const CORRECTION_HINT_RE = (\/.*\/i);/);
const HINT = new Function(`return ${reLine[1]};`)();
for (const t of ['esto está mal', "that's wrong", 'this is incorrect', 'no es correcto',
                 'you said 7 but there are 10', 'sí existe, revisa de nuevo']) {
  check(`fires on "${t}"`, HINT.test(t), true);
}
for (const t of ['ok', 'gracias', 'listo', 'perfecto', 'thanks!', 'got it', 'dale']) {
  check(`stays quiet on "${t}"`, HINT.test(t), false);
}

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All report-lesson tests passed.');
