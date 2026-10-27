// Rules test for the report-feedback learning gate.  Run:  node test/report-lesson.test.js
//
// lib/reportLesson.js is pure. The verdicts below are the shapes Haiku returns;
// the 2026-10-24 case (a request to remove a contact, saved as a fabricated
// lesson) must save nothing. Synthetic text only (public repo).
const rl = require('../lib/reportLesson');
const fs = require('fs');
const path = require('path');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── 1. Verdict parsing (fails closed) ───────────────────────────────────────
const LESSON = 'The report overstated pending calls for one closer. Cross-check pending counts against the CRM before posting.';
check('1a real feedback saves the lesson', rl.parseReportLessonVerdict(JSON.stringify({ feedback: true, lesson: LESSON })), LESSON);
check('1b not feedback saves nothing', rl.parseReportLessonVerdict('{"feedback": false}'), null);
check('1c a request dressed as feedback:false saves nothing (the 2026-10-24 case)',
  rl.parseReportLessonVerdict(JSON.stringify({ feedback: false, lesson: 'Max should flag unsubscribe requests.' })), null);
check('1d prose instead of JSON saves nothing (the old behaviour saved it)', rl.parseReportLessonVerdict('# Lesson\n\nThe report included a request...'), null);
check('1e fenced JSON parses', rl.parseReportLessonVerdict('```json\n' + JSON.stringify({ feedback: true, lesson: LESSON }) + '\n```'), LESSON);
check('1f feedback:"true" as a string is not true', rl.parseReportLessonVerdict(JSON.stringify({ feedback: 'true', lesson: LESSON })), null);
check('1g a too-short lesson saves nothing', rl.parseReportLessonVerdict(JSON.stringify({ feedback: true, lesson: 'ok' })), null);
check('1h a long lesson is clipped', rl.parseReportLessonVerdict(JSON.stringify({ feedback: true, lesson: 'x'.repeat(2000) })).length, 800);
check('1i empty saves nothing', rl.parseReportLessonVerdict(''), null);

// ── 2. Prompt ───────────────────────────────────────────────────────────────
const p = rl.buildReportLessonPrompt({ channelName: 'ng-sales-goats', report: 'Respuesta por correo · Juan', message: '@Max puedes remover a Juan de la base de datos?' });
check('2a the prompt names requests as NOT feedback', /NOT feedback: a question, a request for Max to do something/.test(p), true);
check('2b the prompt carries the post and the message', [p.includes('Respuesta por correo · Juan'), p.includes('puedes remover a Juan')], [true, true]);
check('2c the prompt asks for JSON with a feedback flag', p.includes('{"feedback": false}'), true);

// ── 3. index.js wiring ──────────────────────────────────────────────────────
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const fn = SRC.slice(SRC.indexOf('async function extractAndSaveReportLesson('), SRC.indexOf('// Map a report\'s root message text'));
check('3a the extractor uses the gated prompt', fn.includes('reportLesson.buildReportLessonPrompt('), true);
check('3b the extractor saves only a parsed verdict', /const lesson = reportLesson\.parseReportLessonVerdict\(raw\);\s*if \(!lesson\)/.test(fn), true);
check('3c the old ungated prompt is gone', fn.includes('A team member gave feedback on a Max report'), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall report-lesson checks passed');
