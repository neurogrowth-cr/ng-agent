// Rules test for the Max self-improvement backlog.  Run:  node test/max-backlog.test.js
//
// lib/maxBacklog.js is pure (no Slack, no network), so it is required directly.
// Texts below are synthetic (public repo): no real lead names, emails or phones.
const mb = require('../lib/maxBacklog');
const fs = require('fs');
const path = require('path');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── 1. Gate ─────────────────────────────────────────────────────────────────
// The 2026-10-24 refusal that started this, paraphrased with a fake name.
const REFUSAL = 'Eso es una acción sensible, Sebastian. No tengo autorización para eliminar contactos de GHL sin que Ron lo apruebe. Lo que sí te recomiendo: entra al perfil de Juan Pérez en GHL.';
check('1a the GHL delete refusal passes the gate', mb.looksLikeGap(REFUSAL), true);
check('1b a policy refusal passes the gate', mb.looksLikeGap('Ron tiene que corregirlo directamente, yo no puedo sobrescribir un outcome existente.'), true);
check('1c English refusal passes the gate', mb.looksLikeGap("I don't have access to the Stripe dashboard."), true);
check('1d a normal answer does not', mb.looksLikeGap('Esta semana hubo 14 llamadas agendadas y 9 asistidas. El show rate subió a 64%.'), false);
check('1e "no puedo" without a verb from the list does not', mb.looksLikeGap('No puedo creer lo bien que salió la semana.'), false);
check('1f empty is not a gap', mb.looksLikeGap(''), false);

// ── 2. Scrub and slug ───────────────────────────────────────────────────────
check('2a scrub removes emails and long numbers',
  mb.scrub('Escribir a ana.lopez@example.com o al +506 8888-1234 por $3500000'),
  'Escribir a [email] o al [número] por $[número]');
check('2b scrub removes em dashes and Slack formatting', mb.scrub('*Eliminar* contactos \u2014 en `GHL`'), 'Eliminar contactos , en GHL');
check('2c scrub clips', mb.scrub('a'.repeat(30), 10), 'aaaaaaaaa…');
check('2d slug normalises accents and spaces', mb.slugify('capability_gap', 'Eliminar Contacto GHL ñandú'), 'gap:eliminar-contacto-ghl-nandu');
check('2e slug prefix by kind', mb.slugify('check_broken', 'runHotReplyCheck'), 'check_broken:runhotreplycheck');
check('2f empty topic gives no slug', mb.slugify('capability_gap', '  '), null);

// ── 3. Haiku verdict parsing ────────────────────────────────────────────────
const verdict = (o) => JSON.stringify(o);
check('3a a capability gap parses',
  mb.parseGapJson(verdict({ kind: 'capability_gap', slug: '', topic: 'eliminar-contacto-ghl', title: 'Eliminar contactos de GHL', summary: 'Pidieron borrar a un contacto; Max no tiene herramienta de borrado.' })),
  { kind: 'capability_gap', slug: 'gap:eliminar-contacto-ghl', title: 'Eliminar contactos de GHL', summary: 'Pidieron borrar a un contacto; Max no tiene herramienta de borrado.' });
check('3b none is dropped', mb.parseGapJson(verdict({ kind: 'none' })), null);
check('3c junk is dropped', mb.parseGapJson('sure! here is the json'), null);
check('3d fenced JSON parses',
  mb.parseGapJson('```json\n' + verdict({ kind: 'policy_block', topic: 'sobrescribir-outcome', title: 'Sobrescribir un outcome' }) + '\n```').slug,
  'blocked:sobrescribir-outcome');
check('3e an existing slug is reused',
  mb.parseGapJson(verdict({ kind: 'capability_gap', slug: 'gap:borrar-contacto', topic: 'eliminar-contacto-ghl', title: 'Eliminar contacto' }), ['gap:borrar-contacto']).slug,
  'gap:borrar-contacto');
check('3f an invented slug is not trusted',
  mb.parseGapJson(verdict({ kind: 'capability_gap', slug: 'gap:made-up', topic: 'eliminar-contacto-ghl', title: 'Eliminar contacto' }), ['gap:borrar-contacto']).slug,
  'gap:eliminar-contacto-ghl');
check('3g a title with an email is scrubbed',
  mb.parseGapJson(verdict({ kind: 'capability_gap', topic: 'x', title: 'Escribir a ana@example.com' })).title,
  'Escribir a [email]');
check('3h the prompt lists open slugs and forbids lead data',
  (() => { const p = mb.buildGapPrompt({ userText: 'borra a X', replyText: REFUSAL, openItems: [{ slug: 'gap:a', title: 'A' }] });
    return [p.includes('- gap:a: A'), /Never include names of leads/.test(p)]; })(), [true, true]);

// ── 4. Merge ────────────────────────────────────────────────────────────────
const NOW = '2026-11-02T15:00:00.000Z';
const row = mb.newRow({ kind: 'capability_gap', slug: 'gap:x', title: 'X', times: ['2026-10-24T16:24:00.000Z'] }, NOW);
check('4a a new row starts at one occurrence, open',
  [row.occurrences, row.status, row.first_seen, row.last_seen], [1, 'open', '2026-10-24T16:24:00.000Z', '2026-10-24T16:24:00.000Z']);
check('4b a later event bumps the count',
  mb.mergeOccurrence({ occurrences: 1, last_seen: '2026-10-24T16:24:00.000Z', status: 'open' }, { times: ['2026-10-30T10:00:00.000Z'] }, NOW).occurrences, 2);
check('4c an event already counted is ignored (weekly re-read)',
  mb.mergeOccurrence({ occurrences: 3, last_seen: '2026-10-30T10:00:00.000Z', status: 'open' }, { times: ['2026-10-29T10:00:00.000Z', '2026-10-30T10:00:00.000Z'] }, NOW), null);
check('4d a done item that recurs reopens with a note',
  (() => { const p = mb.mergeOccurrence({ occurrences: 2, last_seen: '2026-10-01T00:00:00.000Z', status: 'done', notes: 'PR #400' }, { times: [NOW] }, NOW);
    return [p.status, p.notes]; })(), ['open', 'PR #400 | reabierto 2026-11-02']);
check('4e a dropped item stays dropped but counts',
  (() => { const p = mb.mergeOccurrence({ occurrences: 2, last_seen: '2026-10-01T00:00:00.000Z', status: 'dropped' }, { times: [NOW] }, NOW);
    return [p.status, p.occurrences]; })(), [undefined, 3]);
check('4f permalink fills only when empty',
  [mb.mergeOccurrence({ occurrences: 1, last_seen: '2026-10-01T00:00:00Z', permalink: null }, { permalink: 'https://x', times: [NOW] }, NOW).permalink,
   mb.mergeOccurrence({ occurrences: 1, last_seen: '2026-10-01T00:00:00Z', permalink: 'https://old' }, { permalink: 'https://x', times: [NOW] }, NOW).permalink],
  ['https://x', undefined]);

// ── 5. Mappers ──────────────────────────────────────────────────────────────
const acts = [
  { event_type: 'cron_run', action: 'runSetterHotRepliesDigest', status: 'error', error_message: 'statement timeout', created_at: '2026-10-21T00:00:05Z' },
  { event_type: 'cron_run', action: 'runSetterHotRepliesDigest', status: 'error', error_message: 'statement timeout again', created_at: '2026-10-22T00:00:05Z' },
  { event_type: 'audit', action: 'hot_reply_check', status: 'degraded', created_at: '2026-10-22T03:00:00Z' },
  { event_type: 'cron_run', action: 'runNightlyLearning', status: 'ok', created_at: '2026-10-22T11:30:00Z' },
];
const items = mb.activityRowsToItems(acts);
check('5a failed runs group by action, ok rows ignored', items.map(i => [i.slug, i.times.length]),
  [['check_broken:runsetterhotrepliesdigest', 2], ['check_broken:hot_reply_check', 1]]);
check('5b the newest error is the summary', items[0].summary, 'statement timeout again');
check('5c a degraded row without an error says so', items[1].summary, 'estado degraded');
const lessons = mb.lessonRowsToItems([
  { key: 'correction:closer-email:2026-10-20', value: 'Max used the old email. Use the company domain.', user_id: 'U1', updated_at: '2026-10-20T10:00:00Z' },
  { key: 'report_lesson:ng-sales-goats:2026-10-24', value: '# Lesson\n\nThe report missed an opt-out. Flag it.', user_id: 'U2', updated_at: '2026-10-24T16:24:31Z' },
  { key: 'something-else', value: 'x', updated_at: '2026-10-24T00:00:00Z' },
]);
check('5d correction and report lessons map, others skipped',
  lessons.map(l => [l.slug, l.requested_by]), [['correction:closer-email', 'U1'], ['correction:reporte-ng-sales-goats', 'U2']]);
check('5e the summary is the first sentence, heading stripped', lessons[1].summary, 'The report missed an opt-out.');

// ── 6. Dates (Costa Rica, UTC-6) ────────────────────────────────────────────
check('6a Mon 2026-11-02 09:00 CR is a first Monday', mb.isFirstMondayCR(new Date('2026-11-02T15:00:00Z')), true);
check('6b Mon 2026-11-09 is not', mb.isFirstMondayCR(new Date('2026-11-09T15:00:00Z')), false);
check('6c 00:30 UTC Tue is still Monday in CR', mb.isFirstMondayCR(new Date('2026-11-03T05:30:00Z')), true);
check('6d month label in Spanish', mb.monthLabelCR(new Date('2026-11-02T15:00:00Z')), 'NOVIEMBRE 2026');
check('6e next first Monday after Oct 26 is Nov 2', mb.nextFirstMondayCR(new Date('2026-10-26T15:00:00Z')).toISOString().slice(0, 10), '2026-11-02');
check('6f next first Monday after Nov 2 is Dec 7', mb.nextFirstMondayCR(new Date('2026-11-02T15:00:00Z')).toISOString().slice(0, 10), '2026-12-07');

// ── 7. Ranking and digest contract ──────────────────────────────────────────
const open = [
  { slug: 'a', kind: 'capability_gap', title: 'Eliminar contactos de GHL', summary: 'Pidieron borrar un contacto.', occurrences: 3, last_seen: '2026-10-30', requested_by: 'U1', permalink: 'https://slack/p1', status: 'open' },
  { slug: 'b', kind: 'tool_error', title: 'La herramienta x falló', summary: "Cannot read properties of null (reading 'id')", occurrences: 3, last_seen: '2026-10-31', status: 'open' },
  { slug: 'c', kind: 'check_broken', title: 'runFoo falló', occurrences: 1, last_seen: '2026-10-29', status: 'open' },
  { slug: 'd', kind: 'correction', title: 'Corrección: closer email', occurrences: 2, last_seen: '2026-10-20', status: 'open' },
  { slug: 'e', kind: 'policy_block', title: 'Sobrescribir outcome', occurrences: 1, last_seen: '2026-10-01', status: 'open' },
  { slug: 'f', kind: 'capability_gap', title: 'Leer Stripe', occurrences: 1, last_seen: '2026-09-01', status: 'open' },
  { slug: 'g', kind: 'capability_gap', title: 'Hecho', occurrences: 9, last_seen: '2026-10-31', status: 'done' },
];
check('7a rank by occurrences, then most recent, done excluded',
  mb.rankOpen(open).map(r => r.slug), ['b', 'a', 'd', 'c', 'e']);
const digest = mb.formatDigest({ rows: open, openTotal: 6, monthLabel: 'NOVIEMBRE 2026', ronId: 'U05' });
const lines = digest.split('\n');
check('7b header names the month and Ron', lines[0], '`MAX BACKLOG: NOVIEMBRE 2026` <@U05>');
check('7c item line carries kind, count, requester and link',
  lines.find(l => l.startsWith('2. ')), '2. *Eliminar contactos de GHL* · me falta la herramienta · 3 veces · pidió <@U01> · <https://slack/p1|hilo>'.replace('U01', 'U1'));
check('7d singular vez', /· 1 vez/.test(digest), true);
check('7e overflow line counts the rest', lines.includes('…y 1 más.'), true);
check('7f the digest passes its own contract', mb.validateDigest(digest), { ok: true, problems: [] });
check('7g "null" inside a summary does not fail the contract', digest.includes('properties of null'), true);
check('7h a long queue warns',
  mb.formatDigest({ rows: open, openTotal: 30, monthLabel: 'NOVIEMBRE 2026' }).includes('⚠️ La cola crece'), true);

// The contract must be able to go red (recipe standard: a test that cannot
// fail is not a criterion).
check('7i a broken header fails', mb.validateDigest(digest.replace('MAX BACKLOG:', 'BACKLOG')).ok, false);
check('7j an undefined count fails',
  mb.validateDigest(digest.replace('3 veces · pidió', 'undefined veces · pidió')).ok, false);
check('7k an em dash fails', mb.validateDigest(digest + '\nuno \u2014 dos').ok, false);
check('7l six items fail', mb.validateDigest(digest + '\n6. *X* · a · 1 vez').ok, false);
check('7m no items fail', mb.validateDigest('`MAX BACKLOG: NOVIEMBRE 2026`\nnada').ok, false);
check('7n the template itself has no em dash', /\u2014/.test(fs.readFileSync(path.join(__dirname, '..', 'lib', 'maxBacklog.js'), 'utf8').replace(/\\u2014/g, '')), false);

// ── 8. Weekly brief line ────────────────────────────────────────────────────
const nov2 = new Date(Date.UTC(2026, 10, 2));
check('8a counts with next digest', mb.formatBriefLine({ newThisWeek: 2, openTotal: 7, nextDigest: nov2 }),
  '• 2 new gaps logged this week, 7 open. Next digest Nov 2 in #ng-pm-agent.');
check('8b singular', mb.formatBriefLine({ newThisWeek: 1, openTotal: 1 }), '• 1 new gap logged this week, 1 open.');
check('8c quiet week', mb.formatBriefLine({ newThisWeek: 0, openTotal: 4 }), '• No new gaps this week, 4 open.');
check('8d empty', mb.formatBriefLine({ newThisWeek: 0, openTotal: 0 }), '• Nothing logged, nothing open.');

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall max-backlog checks passed');
