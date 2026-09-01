// Rules test for the groomed Monday gap report.  Run:  node test/gap-report-format.test.js
//
// Extracts the real formatting helpers straight out of index.js (same approach
// as day-anchor.test.js) so the test can never drift from shipped behaviour.
//
// What this pins down: the 2026-08-31 grooming after Ron's feedback that the
// report was an unreadable wall of text. The contract — one sentence per client,
// counts instead of activity-title dumps, only the newest context note (clipped),
// chronic clients (60+ days) routed to a "Needs a decision" section, and a
// consecutive-week streak counter that resets when a client drops off the list.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// ─── GAP REPORT GROOMING (pure helpers)'),
  SRC.indexOf('// ─── END GAP REPORT GROOMING'),
);
const helpers = new Function(`${block};
  return { computeGapStreaks, gapOrdinal, gapCtxSentence, formatGapClientLine, formatGapReport,
           GAP_CHRONIC_DAYS, GAP_WORK_MAX_LINES, GAP_CTX_MAX_CHARS };`)();
const { computeGapStreaks, gapOrdinal, gapCtxSentence, formatGapClientLine, formatGapReport,
        GAP_CHRONIC_DAYS, GAP_WORK_MAX_LINES, GAP_CTX_MAX_CHARS } = helpers;

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });

// ── Streaks ──────────────────────────────────────────────────────────────────
{
  // Carried clients increment, dropped clients vanish, reappearing ones reset.
  const prev = { 'Capital Brokers FA': 11, 'Old Client Gone': 4 };
  const next = computeGapStreaks(prev, ['Capital Brokers FA', 'Move Car Detailing']);
  check('streak increments for carried client', next['Capital Brokers FA'] === 12, JSON.stringify(next));
  check('streak starts at 1 for new client', next['Move Car Detailing'] === 1, JSON.stringify(next));
  check('streak drops client no longer flagged', !('Old Client Gone' in next), JSON.stringify(next));
  const reappear = computeGapStreaks(next, ['Old Client Gone']);
  check('streak resets to 1 on reappearance', reappear['Old Client Gone'] === 1, JSON.stringify(reappear));
  check('streak works with null prev (first run)', computeGapStreaks(null, ['A']).A === 1, null);
}

// ── Ordinals ─────────────────────────────────────────────────────────────────
{
  const got = [1, 2, 3, 4, 11, 12, 13, 21, 22, 23].map(gapOrdinal).join(' ');
  const want = '1st 2nd 3rd 4th 11th 12th 13th 21st 22nd 23rd';
  check('ordinals incl. 11th–13th special case', got === want, got);
}

// ── Context clipping ─────────────────────────────────────────────────────────
{
  check('empty context yields empty string', gapCtxSentence('') === '' && gapCtxSentence(null) === '', null);
  check('short context gets a period and leading space', gapCtxSentence('Client replied on WhatsApp') === ' Client replied on WhatsApp.', JSON.stringify(gapCtxSentence('Client replied on WhatsApp')));
  const long = 'Fully blocked across all portal activities including activation call, campaign config, Phase 1 and Phase 2 completion; client previously replied saying they would proceed but no movement logged since then at all.';
  const clipped = gapCtxSentence(long);
  check('long context clipped under limit + ellipsis',
    clipped.length <= GAP_CTX_MAX_CHARS + 3 && clipped.endsWith('…'), `len=${clipped.length}`);
  check('clip lands on a word boundary', !/\S{1,}…$/.test(clipped) || !long.slice(0, GAP_CTX_MAX_CHARS).endsWith(clipped.slice(1, -1)), clipped);
}

// ── Client lines: counts, never activity dumps ───────────────────────────────
{
  const blocked = { name: 'Move Car Detailing', severity: 'blocked', daysSince: 36, status: 'blocked', blockedCount: 19, oldestBlocked: 'Campaign Config en Prosp AI', ctx: '', streak: 1 };
  const line = formatGapClientLine(blocked, false);
  check('blocked line = count + oldest step only',
    line === '• *Move Car Detailing* — 19 steps blocked, oldest is Campaign Config en Prosp AI (Day 36). New this week.', line);

  const chronicBlocked = { ...blocked, daysSince: 157, streak: 12 };
  const cLine = formatGapClientLine(chronicBlocked, false);
  check('chronic blocked line: stuck N days + streak, no oldest-step detail',
    cLine === '• *Move Car Detailing* — stuck 157 days, 19 steps blocked. 12th week on this list.', cLine);

  const overdue = { name: 'Irazu Labs', severity: 'overdue', daysSince: 212, status: 'phase_2', ctx: '', streak: 9 };
  check('overdue line uses human phase label',
    formatGapClientLine(overdue, false) === '• *Irazu Labs* — 212 days in Phase 2, past the 14-day window. 9th week on this list.',
    formatGapClientLine(overdue, false));

  const stale = { name: 'Maryia P', severity: 'stale', daysSince: 9, status: 'phase_2', staleCount: 1, assignees: 'valeria', ctx: '', streak: 2 };
  check('stale line: singular activity + assignee',
    formatGapClientLine(stale, false) === '• *Maryia P* — 1 activity untouched for 72h+ (Day 9). Assigned to valeria. 2nd week on this list.',
    formatGapClientLine(stale, false));

  // First run after deploy: no baseline, so no "New this week" / streak noise.
  const firstRunLine = formatGapClientLine(blocked, true);
  check('first run suppresses new/streak phrasing',
    !firstRunLine.includes('New this week') && !firstRunLine.includes('week on this list'), firstRunLine);
}

// ── Whole report: sections, routing, caps ────────────────────────────────────
{
  const clients = [
    { name: 'Capital Brokers FA', severity: 'blocked', daysSince: 157, status: 'blocked', blockedCount: 18, oldestBlocked: 'Activation Call Completed', ctx: 'Client replied but Phase 2 is still not completed. Escalation or closure decision needed.', streak: 12 },
    { name: 'Move Car Detailing', severity: 'blocked', daysSince: 36, status: 'blocked', blockedCount: 19, oldestBlocked: 'Campaign Config', ctx: '', streak: 1 },
    { name: 'Creativa', severity: 'overdue', daysSince: 72, status: 'phase_1', ctx: '', streak: 3 },
    { name: 'Maryia P', severity: 'stale', daysSince: 9, status: 'phase_2', staleCount: 2, assignees: 'valeria', ctx: '', streak: 1 },
  ];
  const report = formatGapReport({
    todayLabel: 'Monday, August 31',
    firstRun: false,
    clients,
    phase0: [{ name: 'Andres Sandoval', company: 'DIY', stepLabel: 'awaiting activation call', days: 13 }],
    sales: { noShowCount: 2, staleLeadCount: 3 },
  });

  check('header present', report.startsWith('📋 *Monday Delivery Gaps — Monday, August 31*'), report.split('\n')[0]);
  check('summary counts severities', report.includes('4 clients need attention this week: 2 blocked, 1 overdue, 1 stale.'), null);
  check('summary counts new clients', report.includes('2 are new.'), null);
  check('chronic routed to decision section (157d and 72d, not 36d)',
    report.indexOf('*Needs a decision: escalate or close*') !== -1
      && report.indexOf('Capital Brokers FA') > report.indexOf('*Needs a decision')
      && report.indexOf('Capital Brokers FA') < report.indexOf('*Work this week*')
      && report.indexOf('Creativa') < report.indexOf('*Work this week*')
      && report.indexOf('Move Car Detailing') > report.indexOf('*Work this week*'), null);
  check('decision section carries the in-thread ask', report.includes("Reply in-thread with the call on each"), null);
  check('phase 0 single stuck signup is one sentence', report.includes('One signup stuck in Phase 0: Andres Sandoval (DIY), Day 13, awaiting activation call.'), null);
  check('sales collapsed to counts pointing at thread', report.includes('Sales side: 2 no-shows without a reschedule and 3 inbound leads waiting 72h+ on a setter — details in the thread.'), null);
  check('headers are real sections separated by blank lines', report.includes('\n\n*Work this week*\n\n'), null);
  check('no pipe-separator spam', !report.includes(' | '), null);

  // The regression that started all this: a Capital Brokers-style client must
  // never render its 18 activity titles.
  check('no activity-title dump', !report.includes('Activation Call Completed,'), null);
}

// ── Work-section cap ─────────────────────────────────────────────────────────
{
  const many = Array.from({ length: 14 }, (_, i) => ({ name: `Client ${i}`, severity: 'overdue', daysSince: 20 + i, status: 'phase_1', ctx: '', streak: 2 }));
  const report = formatGapReport({ todayLabel: 'X', firstRun: false, clients: many, phase0: [], sales: {} });
  const bullets = (report.match(/^• /gm) || []).length;
  check('work section capped with remainder line',
    bullets === GAP_WORK_MAX_LINES && report.includes(`…plus ${14 - GAP_WORK_MAX_LINES} more in the portal.`), `bullets=${bullets}`);
}

// ── Empty-section omission ───────────────────────────────────────────────────
{
  const report = formatGapReport({ todayLabel: 'X', firstRun: true, clients: [], phase0: [], sales: { noShowCount: 1, staleLeadCount: 0 } });
  check('no client sections when no clients flagged',
    !report.includes('Needs a decision') && !report.includes('Work this week') && report.includes('1 no-show without a reschedule'), report);
}

// ── Report ───────────────────────────────────────────────────────────────────
const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✅' : '❌'} ${c.name}${c.ok || c.detail == null ? '' : `\n     got: ${c.detail}`}`);
console.log(`\n${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
