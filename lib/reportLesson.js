'use strict';
// Report-feedback learning gate (2026-10-26). A message in a thread under a
// Max post used to be saved as a "lesson" no matter what it said: on
// 2026-10-24 Sebastián asked Max to remove a contact, Haiku turned the question
// into a fabricated lesson, it rode every interactive prompt, and Max told the
// team he had learned something. Now Haiku first decides whether the message
// is feedback on the post at all; anything else saves nothing.
//
// Pure (no network), tested in test/report-lesson.test.js. The call and the
// save live in index.js extractAndSaveReportLesson.

const LESSON_MIN = 20;
const LESSON_MAX = 800;

function buildReportLessonPrompt({ channelName, report, message }) {
  return `A team member wrote a message in a Slack thread under a post that Max (an internal ops bot) made in #${channelName || 'a DM'}.

Max's post:
${String(report || '').slice(0, 1500)}

The team member wrote:
${String(message || '').slice(0, 800)}

Is this message FEEDBACK on Max's post itself: it says something in the post is wrong, inaccurate, missing, badly formatted, too noisy, or should be done differently next time?
NOT feedback: a question, a request for Max to do something (look something up, change or remove a record or contact, send something), thanks, agreement, or a message aimed at a colleague.

Respond with JSON only, no markdown fences:
{"feedback": true, "lesson": "<2-3 sentences: what was wrong in the post and what Max should do differently in future posts of this kind>"}
or
{"feedback": false}`;
}

// Fails closed: anything that is not a clear {"feedback": true, "lesson": ...}
// saves nothing.
function parseReportLessonVerdict(raw) {
  let parsed;
  try {
    parsed = JSON.parse(String(raw || '').trim().replace(/^```(?:json)?\s*|\s*```$/g, ''));
  } catch (_) { return null; }
  if (!parsed || parsed.feedback !== true || typeof parsed.lesson !== 'string') return null;
  const lesson = parsed.lesson.replace(/\s+/g, ' ').trim();
  if (lesson.length < LESSON_MIN) return null;
  return lesson.length > LESSON_MAX ? `${lesson.slice(0, LESSON_MAX - 1).trimEnd()}…` : lesson;
}

module.exports = { buildReportLessonPrompt, parseReportLessonVerdict };
