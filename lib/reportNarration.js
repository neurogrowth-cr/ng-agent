'use strict';
// Narration guard for scheduled reports. A report posts to a team channel
// verbatim, so the model's own process notes ("I have enough to make a sound
// call", "the middle section is elided", "excluding per the rule") must never
// reach it. Two layers, both pure and tested in test/report-narration.test.js:
//
//   cutToFirstHeader  whitelist: a task that declares section headers starts
//                     at its first header; anything before it is cut (the
//                     TASK_LEAD_ANCHOR idea, generalised to TASK_HEADERS).
//   findNarration     denylist, kept narrow on purpose: first-person process
//                     phrases that a finished report never contains and a
//                     quoted prospect message is unlikely to. A hit fails
//                     validation so the existing re-prompt runs; it never
//                     silently rewrites the text.
//
// Why both: Sonnet 5 (shadow, 2026-10-07 to 10-09) passed every header check
// while narrating around the data. The cut handles narration before the first
// header; the denylist catches it inside the body.

const NARRATION_PATTERNS = [
  /\bI have enough (?:to|information|context|data)\b/i,
  /\bI(?:'ll| will) (?:now )?(?:finalize|finalise|compile|proceed|summarize|summarise|work with|go with)\b/i,
  /\bbased on what(?:'s| is) (?:fully |currently )?(?:visible|available|shown|loaded)\b/i,
  /\b(?:is|are|was|were|appears?) (?:elided|truncated)\b/i,
  /\bthe truncated (?:middle|section|part|output)\b/i,
  /\bexcluding (?:it |them |this |these )?per the\b/i,
  /\b(?:I|we) (?:can|cannot|can't|could not|couldn't) verify\b/i,
  /\bI (?:do not|don't) have (?:enough|access|the full|visibility)\b/i,
  /\bnow that I have\b/i,
  /\bconsistent with the fact that\b/i,
  /\b(?:here is|here's) the (?:final|compiled|finished) (?:report|version|text)\b/i,
  /\bI(?:'m| am) (?:unable|not able) to\b/i,
  /\bthe (?:tool|data|context) (?:output|result|window) (?:is|was) (?:cut off|incomplete|limited)\b/i,
];

/** The first narration phrase in the text, or null. */
function findNarration(text) {
  const t = String(text || '');
  for (const re of NARRATION_PATTERNS) {
    const m = re.exec(t);
    if (m) return m[0];
  }
  return null;
}

/**
 * For a task with declared section headers: when the text before its first
 * header is narration, the text from that header's line on. Unchanged when
 * no header is found (validation then fails on headers anyway), when the
 * text already starts at one, or when what precedes it is not narration (a
 * report title such as "SALES EOD REPORT" sits before the first header and
 * must survive).
 */
function cutToFirstHeader(text, headers) {
  const t = String(text || '');
  if (!Array.isArray(headers) || headers.length === 0) return t;
  const upper = t.toUpperCase();
  let first = -1;
  for (const h of headers) {
    const i = upper.indexOf(String(h).toUpperCase());
    if (i >= 0 && (first < 0 || i < first)) first = i;
  }
  if (first <= 0) return t;
  // Keep the line the header sits on (it may be wrapped in backticks or bold).
  const lineStart = t.lastIndexOf('\n', first) + 1;
  if (lineStart === 0 || !findNarration(t.slice(0, lineStart))) return t;
  return t.slice(lineStart);
}

module.exports = { NARRATION_PATTERNS, findNarration, cutToFirstHeader };
