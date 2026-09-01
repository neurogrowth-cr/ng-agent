// Rules test for the transitions-only Monday gap report.  Run:  node test/gap-report-format.test.js
//
// Extracts the real formatting helpers straight out of index.js (same approach
// as day-anchor.test.js) so the test can never drift from shipped behaviour.
//
// What this pins down: Ron's 2026-08-31 rules, in two rounds. Round one: no
// walls of text — one sentence per client, counts over activity dumps, newest
// context only. Round two, same day: known customer-side blocks are NOT news —
// the team already knows why a blocked client is blocked, so a client earns a
// bullet only on a TRANSITION (newly flagged, newly crossed the 60-day line,
// moved off the list). What stays on every post: stale items (our assignees
// sitting on activities) and blocked clients with no documented blocker —
// those are on us. A week with no signal produces NO post at all.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// ─── GAP REPORT GROOMING (pure helpers)'),
  SRC.indexOf('// ─── END GAP REPORT GROOMING'),
);
const helpers = new Function(`${block};
  return { computeGapDelta, gapOrdinal, gapCtxSentence, gapListJoin, formatGapClientLine, formatGapReport,
           GAP_CHRONIC_DAYS, GAP_NEW_MAX_LINES, GAP_CTX_MAX_CHARS };`)();
const { computeGapDelta, gapOrdinal, gapCtxSentence, gapListJoin, formatGapClientLine, formatGapReport,
        GAP_CHRONIC_DAYS, GAP_NEW_MAX_LINES } = helpers;

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });

const blockedGap = (name, days, over = {}) =>
  ({ name, severity: 'blocked', status: 'blocked', daysSince: days, blockedCount: 5, oldestBlocked: 'Campaign Config', ctx: '', noNote: false, ...over });
const NO_SALES = { noShowCount: 0, staleLeadCount: 0 };

// ── computeGapDelta: the transition engine ───────────────────────────────────
{
  const prev = { 'Carried': { s: 3, d: 40 }, 'Crossing': { s: 8, d: 59 }, 'Gone': { s: 5, d: 100 }, 'Old Chronic': { s: 9, d: 150 } };
  const current = [blockedGap('Carried', 47), blockedGap('Crossing', 66), blockedGap('Old Chronic', 157), blockedGap('Fresh', 10)];
  const d = computeGapDelta(prev, current);
  check('unchanged client is neither new nor crossed', !d.newNames.includes('Carried') && !d.crossedNames.includes('Carried'), JSON.stringify(d));
  check('first appearance lands in newNames', d.newNames.includes('Fresh'), JSON.stringify(d.newNames));
  check('59→66 crosses the 60-day line exactly once', d.crossedNames.includes('Crossing'), JSON.stringify(d.crossedNames));
  check('already-chronic client does NOT re-cross', !d.crossedNames.includes('Old Chronic'), JSON.stringify(d.crossedNames));
  check('dropped client lands in resolvedNames', d.resolvedNames.length === 1 && d.resolvedNames[0] === 'Gone', JSON.stringify(d.resolvedNames));
  check('streak increments and days refresh', d.nextClients['Carried'].s === 4 && d.nextClients['Carried'].d === 47, JSON.stringify(d.nextClients['Carried']));
  check('new client starts at streak 1', d.nextClients['Fresh'].s === 1, null);
  check('resolved client leaves the state row', !('Gone' in d.nextClients), null);

  // A client whose FIRST appearance is already past 60 days goes straight to the
  // decision section — the escalate-or-close ask matters more than "new".
  const d2 = computeGapDelta({}, [blockedGap('Born Chronic', 90)]);
  check('chronic on first appearance routes to crossed, not new', d2.crossedNames.includes('Born Chronic') && !d2.newNames.includes('Born Chronic'), JSON.stringify(d2));

  const d3 = computeGapDelta(null, [blockedGap('A', 10)]);
  check('null prev (first run) still computes state', d3.nextClients['A'].s === 1, null);
}

// ── Client lines ─────────────────────────────────────────────────────────────
{
  const line = formatGapClientLine(blockedGap('Move Car', 36, { blockedCount: 19, oldestBlocked: 'Campaign Config en Prosp AI', streak: 1 }));
  check('blocked line = count + oldest step, never a title dump',
    line === '• *Move Car* — 19 steps blocked, oldest is Campaign Config en Prosp AI (Day 36).', line);
  const noNote = formatGapClientLine(blockedGap('Ghost Co', 20, { streak: 3, noNote: true }));
  check('no-note nag rides the line', noNote.includes('No blocker documented — add the note in the portal.') && noNote.includes('3rd week on this list.'), noNote);
  const stale = formatGapClientLine({ name: 'Maryia', severity: 'stale', status: 'phase_2', daysSince: 9, staleCount: 2, assignees: 'valeria', ctx: '', streak: 2 });
  check('stale line names assignees + streak', stale === '• *Maryia* — 2 activities untouched for 72h+ (Day 9). Assigned to valeria. 2nd week on this list.', stale);
  const clipped = gapCtxSentence('x'.repeat(300));
  check('context still clipped', clipped.length <= 145 && clipped.endsWith('…'), `len=${clipped.length}`);
  check('ordinals incl. 11th–13th', [1, 2, 3, 11, 12, 13, 21].map(gapOrdinal).join(' ') === '1st 2nd 3rd 11th 12th 13th 21st', null);
  check('list join grammar', gapListJoin(['A']) === 'A' && gapListJoin(['A', 'B']) === 'A and B' && gapListJoin(['A', 'B', 'C']) === 'A, B and C', null);
}

// ── The core rule: known holds are suppressed, transitions get named ─────────
{
  const clients = [
    blockedGap('Known Hold One', 157, { streak: 12 }),   // known, unchanged → suppressed
    blockedGap('Known Hold Two', 94,  { streak: 4 }),    // known, unchanged → suppressed
    blockedGap('Newly Blocked', 12,   { streak: 1 }),    // transition → New this week
    blockedGap('Just Crossed', 63,    { streak: 9 }),    // transition → decision section
    { name: 'Stale Sam', severity: 'stale', status: 'phase_1', daysSince: 10, staleCount: 3, assignees: 'josue', ctx: '', noNote: false, streak: 2 },
    blockedGap('Ghost Co', 30, { streak: 5, noNote: true }), // no documented blocker → on us
  ];
  const delta = {
    newNames: ['Newly Blocked'], crossedNames: ['Just Crossed'], resolvedNames: ['Winner A', 'Winner B'],
    nextClients: {},
  };
  const { message: r, hasSignal } = formatGapReport({ todayLabel: 'Monday, September 7', firstRun: false, clients, delta, phase0: [], sales: NO_SALES });

  check('transition week has signal', hasSignal === true, null);
  check('summary counts the transitions',
    r.includes('Since last Monday: 1 client newly flagged, 1 crossed the 60-day line, 2 moved forward.'), r.split('\n')[2]);
  check('known holds collapse to a count with no names',
    r.includes('2 remain in known customer-side holds — no change; ask Max for the full roster.')
      && !r.includes('Known Hold One') && !r.includes('Known Hold Two'), null);
  check('newly blocked client is named under New this week',
    r.indexOf('Newly Blocked') > r.indexOf('*New this week*'), null);
  check('crossing client gets the one-time escalate-or-close ask',
    r.indexOf('Just Crossed') > r.indexOf(`*Crossed ${GAP_CHRONIC_DAYS} days — needs an escalate-or-close call*`)
      && r.includes("Reply in-thread with the call — these won't move on follow-ups alone."), null);
  check('stale + no-note clients stay under On our side',
    r.indexOf('Stale Sam') > r.indexOf('*On our side*') && r.indexOf('Ghost Co') > r.indexOf('*On our side*'), null);
  check('wins are named', r.includes('*Moved forward*') && r.includes('Winner A and Winner B are no longer flagged.'), null);
  check('no pipe-separator spam', !r.includes(' | '), null);
}

// ── Steady-state week: NO post ───────────────────────────────────────────────
{
  const clients = [blockedGap('Known Hold', 157, { streak: 12 }), blockedGap('Other Hold', 94, { streak: 4 })];
  const delta = { newNames: [], crossedNames: [], resolvedNames: [], nextClients: {} };
  const { message: r, hasSignal } = formatGapReport({ todayLabel: 'X', firstRun: false, clients, delta, phase0: [], sales: NO_SALES });
  check('nothing-changed week has NO signal (post skipped)', hasSignal === false, r);
  check('…even though the summary would have said so', r.includes('No new gaps since last Monday.'), null);
}

// ── Wins-only week still posts ───────────────────────────────────────────────
{
  const delta = { newNames: [], crossedNames: [], resolvedNames: ['Comeback Kid'], nextClients: {} };
  const { message: r, hasSignal } = formatGapReport({ todayLabel: 'X', firstRun: false, clients: [], delta, phase0: [], sales: NO_SALES });
  check('wins-only week has signal', hasSignal === true, null);
  check('wins-only week names the win', r.includes('Comeback Kid is no longer flagged.'), null);
}

// ── First run: baseline, no false transition storm ───────────────────────────
{
  const clients = [blockedGap('A', 157, { streak: 1 }), blockedGap('B', 94, { streak: 1 })];
  const delta = computeGapDelta(null, clients);
  const { message: r, hasSignal } = formatGapReport({ todayLabel: 'X', firstRun: true, clients, delta, phase0: [], sales: NO_SALES });
  check('first run suppresses transitions despite delta calling all new/crossed',
    !r.includes('*New this week*') && !r.includes('escalate-or-close') && !r.includes('Moved forward'), r);
  check('first run announces the baseline', r.includes('2 clients are currently flagged — baseline set; from next Monday only changes get named.'), null);
  check('first run with only holds is silent (no post)', hasSignal === false, null);
  const staleClients = [...clients, { name: 'Stale Sam', severity: 'stale', status: 'phase_1', daysSince: 10, staleCount: 1, assignees: 'josue', ctx: '', noNote: false, streak: 1 }];
  const r2 = formatGapReport({ todayLabel: 'X', firstRun: true, clients: staleClients, delta: computeGapDelta(null, staleClients), phase0: [], sales: NO_SALES });
  check('first run with a team-side item still posts it', r2.hasSignal === true && r2.message.includes('Stale Sam'), null);
}

// ── New-section cap ──────────────────────────────────────────────────────────
{
  const many = Array.from({ length: 14 }, (_, i) => blockedGap(`C${i}`, 10 + i, { streak: 1 }));
  const delta = { newNames: many.map(c => c.name), crossedNames: [], resolvedNames: [], nextClients: {} };
  const { message: r } = formatGapReport({ todayLabel: 'X', firstRun: false, clients: many, delta, phase0: [], sales: NO_SALES });
  const bullets = (r.match(/^• /gm) || []).length;
  check('new section capped with remainder line',
    bullets === GAP_NEW_MAX_LINES && r.includes(`…plus ${14 - GAP_NEW_MAX_LINES} more in the portal.`), `bullets=${bullets}`);
}

// ── Phase 0 + sales unchanged ────────────────────────────────────────────────
{
  const delta = { newNames: [], crossedNames: [], resolvedNames: [], nextClients: {} };
  const { message: r, hasSignal } = formatGapReport({
    todayLabel: 'X', firstRun: false, clients: [], delta,
    phase0: [{ name: 'Andres Sandoval', company: 'DIY', stepLabel: 'awaiting activation call', days: 13 }],
    sales: { noShowCount: 2, staleLeadCount: 3 },
  });
  check('phase 0 single signup is one sentence', r.includes('One signup stuck in Phase 0: Andres Sandoval (DIY), Day 13, awaiting activation call.'), null);
  check('sales collapsed to counts pointing at thread', r.includes('Sales side: 2 no-shows without a reschedule and 3 inbound leads waiting 72h+ on a setter — details in the thread.'), null);
  check('phase0/sales alone still constitute signal', hasSignal === true, null);
}

// ── Report ───────────────────────────────────────────────────────────────────
const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✅' : '❌'} ${c.name}${c.ok || c.detail == null ? '' : `\n     got: ${c.detail}`}`);
console.log(`\n${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
