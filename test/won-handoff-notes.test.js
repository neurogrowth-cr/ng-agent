// Rules test for the won-deal handoff note sweep.  Run:  node test/won-handoff-notes.test.js
//
// Extracts the real pure helpers straight out of index.js rather than copying
// them, so the test can never drift from shipped behaviour (same approach as
// appt-deletion-sweep.test.js — index.js boots the Slack app on require).
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const WON_HANDOFF_MARKER'),
  SRC.indexOf('async function generateWonHandoffSummary'),
) + SRC.slice(
  SRC.indexOf('const WON_HANDOFF_LOOKBACK_DAYS'),
  SRC.indexOf('async function findClosingCallRecording'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const { WON_HANDOFF_MARKER, decideWonHandoffStep, buildWonHandoffNote, pickClosingCall } = new Function(
  `${block}; return { WON_HANDOFF_MARKER, decideWonHandoffStep, buildWonHandoffNote, pickClosingCall };`
)();

const COACHING = { icp_fit: 'x' };
const at = (iso) => new Date(iso).getTime();

// ── Step decision (marker state machine) ────────────────────────────────────
check('no marker → match', decideWonHandoffStep(''), 'match');
check('null marker → match', decideWonHandoffStep(null), 'match');
check('placed is terminal', decideWonHandoffStep('placed|2026-08-18'), 'skip');
check('already_placed is terminal', decideWonHandoffStep('already_placed|2026-08-18'), 'skip');
check('gave_up is terminal', decideWonHandoffStep('gave_up|2026-08-18'), 'skip');
check('prospect_only keeps retrying placement', decideWonHandoffStep('prospect_only|2026-08-15'), 'retry_placement');
check('no_recording retries the match', decideWonHandoffStep('no_recording|3'), 'match');
check('unknown state falls back to match', decideWonHandoffStep('garbage'), 'match');

// ── Note assembly ───────────────────────────────────────────────────────────
const note = buildWonHandoffNote({
  summaryText: '• Cliente: Ferretería Corella.\n• Compró para generar leads B2B.',
  callDateStr: 'Aug 12, 2026',
  closerName: 'Jose Carranza',
  recordingUrl: 'https://fathom.video/calls/123',
});
check('note carries the RPC idempotency marker', note.includes(WON_HANDOFF_MARKER), true);
check('note header names date and closer',
  note.split('\n')[0].includes('Aug 12, 2026') && note.split('\n')[0].includes('Jose Carranza'), true);
check('note body survives verbatim', note.includes('• Compró para generar leads B2B.'), true);
check('recording link included when present', note.includes('https://fathom.video/calls/123'), true);

const noUrl = buildWonHandoffNote({ summaryText: '• Algo.', callDateStr: 'Aug 1, 2026', closerName: 'Ron' });
check('no recording line without a URL', noUrl.includes('Grabación'), false);

check('empty summary produces no note (never a header-only note)',
  buildWonHandoffNote({ summaryText: '  ', callDateStr: 'Aug 1, 2026', closerName: 'Ron' }), null);

const anon = buildWonHandoffNote({ summaryText: '• Algo.' });
check('missing metadata degrades to placeholders, not a crash',
  anon.includes('fecha desconocida') && anon.includes('closer desconocido'), true);

// ── Closing-call selection ──────────────────────────────────────────────────
// A deal that closes on a follow-up still needs the context of the call that
// sold it, so selection is by prospect + recency, never by the appointment the
// outcome row points at.

// The real Fernando Corella case: won Aug 6, appointment row dated Aug 12,
// actual closing call Aug 4. The old 36h appointment join threw this away.
check('picks the call before the win even when the appointment is later',
  pickClosingCall(
    [{ call_date: '2026-08-04T17:58:44Z', coaching_json: COACHING, recording_url: 'u' }],
    at('2026-08-06T01:09:16Z'),
  )?.call_date, '2026-08-04T17:58:44Z');

check('multi-call sale takes the LAST call before the win, not the first',
  pickClosingCall([
    { call_date: '2026-07-01T10:00:00Z', coaching_json: COACHING },
    { call_date: '2026-07-20T10:00:00Z', coaching_json: COACHING },
    { call_date: '2026-07-10T10:00:00Z', coaching_json: COACHING },
  ], at('2026-07-25T10:00:00Z'))?.call_date, '2026-07-20T10:00:00Z');

check('a call logged hours after the win still counts (close beats the scoring)',
  pickClosingCall(
    [{ call_date: '2026-07-25T18:00:00Z', coaching_json: COACHING }],
    at('2026-07-25T10:00:00Z'),
  )?.call_date, '2026-07-25T18:00:00Z');

check('a post-sale call days later is NOT the closing call',
  pickClosingCall([
    { call_date: '2026-07-20T10:00:00Z', coaching_json: COACHING },
    { call_date: '2026-08-05T10:00:00Z', coaching_json: COACHING }, // onboarding
  ], at('2026-07-25T10:00:00Z'))?.call_date, '2026-07-20T10:00:00Z');

check('scored-but-empty coaching is not usable material',
  pickClosingCall([{ call_date: '2026-07-20T10:00:00Z', coaching_json: {} }], at('2026-07-25T10:00:00Z')), null);
check('no calls at all → null', pickClosingCall([], at('2026-07-25T10:00:00Z')), null);
check('undefined input → null', pickClosingCall(undefined, at('2026-07-25T10:00:00Z')), null);
check('only post-win calls → null (never summarize a call that came after)',
  pickClosingCall([{ call_date: '2026-08-20T10:00:00Z', coaching_json: COACHING }], at('2026-07-25T10:00:00Z')), null);

// ── Marker constant is what the portal RPC greps for ────────────────────────
check('marker matches the SQL-side constant', WON_HANDOFF_MARKER, 'RESUMEN LLAMADA DE CIERRE (REVI');

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll won-handoff-note tests passed.');
