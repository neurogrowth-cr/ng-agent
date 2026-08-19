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
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const { WON_HANDOFF_MARKER, decideWonHandoffStep, buildWonHandoffNote } = new Function(
  `${block}; return { WON_HANDOFF_MARKER, decideWonHandoffStep, buildWonHandoffNote };`
)();

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

// ── Marker constant is what the portal RPC greps for ────────────────────────
check('marker matches the SQL-side constant', WON_HANDOFF_MARKER, 'RESUMEN LLAMADA DE CIERRE (REVI');

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll won-handoff-note tests passed.');
