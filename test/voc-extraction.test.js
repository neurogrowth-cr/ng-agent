// Tests the marketing VoC extraction helpers.  Run:  node test/voc-extraction.test.js
//
// The title filter decides which Fathom recordings count as SALES calls for
// voice-of-customer extraction. Fixtures use real meeting titles from the
// Fathom Sales team (Jun–Sep 2026). A false positive here pollutes the
// marketing_voc corpus with internal syncs or activation (post-sale) calls;
// a false negative silently drops real prospect calls from the monthly digest.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const VOC_TITLE_INCLUDE'),
  SRC.indexOf('// VoC extraction — Mondays 2:00 AM CR'),
);
const g = new Function(`${block}; return { vocIsSalesCallTitle, vocMeetingRecordingId, vocMeetingTranscriptText, vocProspectName, vocCountLabels, vocFormatCounts };`)();

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// ── Title filter — real titles from the Fathom Sales team ────────────────────
check('closer sales call passes',
  g.vocIsSalesCallTitle('LinkedIn Flywheel by Neurogrowth Consulting LLC  | David Lara'), true);
check('old-style appointment title passes',
  g.vocIsSalesCallTitle('LinkedIn Flywheel - Appointment  x Carina Borges  x on 24 Jul 2026 with Neurogrowth Consulting LLC Jose'), true);
check('activation (post-sale) call is excluded',
  g.vocIsSalesCallTitle('LinkedIn Flywheel Activation Call - CARLOS TRUJILLO'), false);
check('quick sync is excluded',
  g.vocIsSalesCallTitle('LinkedIn Flywheel Quick Sync'), false);
check('internal meeting without flywheel is excluded',
  g.vocIsSalesCallTitle('WBR Fulfilment Team'), false);
check('1:1 review is excluded even with flywheel in title',
  g.vocIsSalesCallTitle('Flywheel 1:1 Review Session'), false);
check('filter is case-insensitive',
  g.vocIsSalesCallTitle('LINKEDIN FLYWHEEL BY NEUROGROWTH | Gustavo'), true);
check('empty title is excluded', g.vocIsSalesCallTitle(''), false);

// ── Recording id resolution ──────────────────────────────────────────────────
check('explicit recording_id wins', g.vocMeetingRecordingId({ recording_id: 804583955, url: 'https://fathom.video/calls/1' }), '804583955');
check('falls back to url tail', g.vocMeetingRecordingId({ url: 'https://fathom.video/calls/804583955' }), '804583955');
check('no id and no url yields null', g.vocMeetingRecordingId({}), null);

// ── Prospect name from title ─────────────────────────────────────────────────
check('prospect name is the segment after the last pipe',
  g.vocProspectName({ title: 'LinkedIn Flywheel by Neurogrowth Consulting LLC  | David Lara' }), 'David Lara');
check('no pipe yields null', g.vocProspectName({ title: 'Some Meeting' }), null);

// ── Transcript flattening ────────────────────────────────────────────────────
check('transcript renders speaker-prefixed lines',
  g.vocMeetingTranscriptText({ transcript: [
    { speaker: { display_name: 'Jose' }, text: 'hola' },
    { text: 'qué tal' },
  ] }), 'Jose: hola\nSpeaker: qué tal');
check('missing transcript yields empty string', g.vocMeetingTranscriptText({}), '');

// ── Label counting (normalizes case + whitespace) ────────────────────────────
const rows = [
  { pains: [{ label: 'Liquidez' }, { label: 'referidos_sin_sistema' }] },
  { pains: [{ label: ' liquidez ' }] },
  { pains: [] },
];
check('labels count across rows, case/space-normalized',
  g.vocCountLabels(rows, r => (r.pains || []).map(p => p && p.label)),
  { liquidez: 2, referidos_sin_sistema: 1 });

// ── Delta formatting ─────────────────────────────────────────────────────────
check('rising pain shows +delta and underscores become spaces',
  g.vocFormatCounts({ leads_basura_meta: 3 }, { leads_basura_meta: 1 }, 8),
  '• leads basura meta: 3 menciones (+2 vs mes anterior)');
check('flat pain shows 0 delta',
  g.vocFormatCounts({ liquidez: 2 }, { liquidez: 2 }, 8),
  '• liquidez: 2 menciones (0 vs mes anterior)');
check('empty month renders a placeholder, not a crash',
  g.vocFormatCounts({}, {}, 8), '• (sin menciones este mes)');

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
