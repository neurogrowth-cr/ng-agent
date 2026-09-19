// Rules test for reopening a confirmed outcome card whose portal row vanished.
//   Run:  node test/outcome-card-reopen.test.js
//
// Built 2026-09-18. Jose confirmed no_show for Danny Gomez on Aug 28. The next
// day the appointment was moved in Google Calendar, dash cleared the no_show as
// a stale echo, and the call went back to "unlogged". The card still carried
// Max's check mark, so taps were ignored, and the cron skipped it entirely:
// 14 nightly escalations to Ron, zero ways for Jose to answer. These checks
// pin the reopen rule and the guarantees that keep it from double-carding.
//
// Same extraction trick as the other tests: functions are sliced out of
// index.js and compiled with new Function, so this cannot drift from shipped
// behaviour.
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── shouldReopenConfirmedCard ────────────────────────────────────────────────
const fnStart = SRC.indexOf('async function shouldReopenConfirmedCard');
const fnEnd = SRC.indexOf('// Fires 9 PM CR every day.', fnStart);
if (fnStart < 0 || fnEnd < 0) { console.error('FAIL: could not extract shouldReopenConfirmedCard'); process.exit(1); }
const fnSrc = SRC.slice(fnStart, fnEnd);

function reopenFn(result) {
  const calls = [];
  const portalSupabase = {
    from: (table) => ({
      select: () => ({
        eq: (col, val) => ({
          limit: async () => {
            calls.push(`${table}.${col}=${val}`);
            if (result === 'throw') throw new Error('network');
            return result;
          },
        }),
      }),
    }),
  };
  const fn = new Function('portalSupabase', `${fnSrc}; return shouldReopenConfirmedCard;`)(portalSupabase);
  return { fn, calls };
}

// ── buildOutcomeCardText (same block the outcome-loop test compiles) ─────────
const block = SRC.slice(
  SRC.indexOf('const OUTCOME_STATUS_RANK'),
  SRC.indexOf('// Writes one outcome row + the matching prospect-status promotion'),
);
const g = new Function(`${block}; return { buildOutcomeCardText, GHL_PIPELINE };`)();

// ── the cron body, for ordering guarantees ───────────────────────────────────
const cronStart = SRC.indexOf('async function runUnloggedOutcomeReminders');
const cronEnd = SRC.indexOf('async function runOutcomeCardsOnceIfRequested', cronStart);
const cron = SRC.slice(cronStart, cronEnd);

(async () => {
  console.log('shouldReopenConfirmedCard: reopen only when the row is provably gone');
  let h = reopenFn({ data: [], error: null });
  check('1a no outcome row means reopen', await h.fn('appt-1'), true);
  check('1b it asks the outcomes table about THIS appointment', h.calls, ['revops_sales_outcomes.appointment_id=appt-1']);

  h = reopenFn({ data: [{ appointment_id: 'appt-1' }], error: null });
  check('1c a row that landed mid-run blocks the second card', await h.fn('appt-1'), false);

  h = reopenFn({ data: null, error: { message: 'timeout' } });
  check('1d a read error fails closed', await h.fn('appt-1'), false);

  h = reopenFn('throw');
  check('1e a thrown error fails closed', await h.fn('appt-1'), false);

  h = reopenFn({ data: null, error: null });
  check('1f null data with no error still means no row', await h.fn('appt-1'), true);

  console.log('buildOutcomeCardText: a reopened card says why it is back');
  const base = {
    prospectName: 'Danny Gómez', whenStr: 'Aug 29, 11:00 AM', heldDays: 1, nudgeCount: 1,
    rec: null, funnel: null, pipelineId: g.GHL_PIPELINE.APPT_SETTING,
    proposal: { outcome: null, confidence: 'none', wonHint: false },
  };
  const plain = g.buildOutcomeCardText(base);
  const reopened = g.buildOutcomeCardText({ ...base, reopened: { date: '2026-08-28' } });
  const undated = g.buildOutcomeCardText({ ...base, reopened: { date: null } });
  check('2a a normal card carries no reopen line', /already logged/.test(plain), false);
  check('2b the reopen line names the original log date', /You already logged this one on 2026-08-28/.test(reopened), true);
  check('2c it blames the moved appointment, not the closer', /portal cleared it when the appointment moved/.test(reopened), true);
  check('2d the reopen line sits right under the header', reopened.split('\n')[1].startsWith('♻️'), true);
  check('2e a missing date degrades to a clean sentence', /You already logged this one, but/.test(undated), true);
  check('2f a reopened first ask never claims past nudges', /nudged/.test(reopened), false);
  check('2g new copy carries no em dash', /—/.test(reopened.split('\n')[1]), false);

  console.log('runUnloggedOutcomeReminders: wiring and ordering');
  const iReopen = cron.indexOf("prior[0] === 'confirmed' && await shouldReopenConfirmedCard(appt.id)");
  const iSkip = cron.indexOf('if (prior && !reopened)');
  const iCard = cron.indexOf('buildOutcomeCardText({');
  check('3a the confirmed state is what triggers the portal re-check', iReopen > 0, true);
  check('3b the re-check runs before the existing-card branch can skip the call', iReopen < iSkip, true);
  check('3c a reopened call falls through to a fresh card', iSkip < iCard, true);
  check('3d the fresh card is told it is a reopen', /nudgeCount: entry\.count, reopened,/.test(cron), true);
  check('3e the nudge counter restarts so the card never says "nudged 14x"', /entry\.count = 1;/.test(cron), true);
  check('3f the escalation clock restarts with it', cron.includes("`outcome-reminder:${appt.id}`, `${todayISO}|1`"), true);
  check('3g a reopened call is kept out of tonight\'s escalation to Ron', cron.includes('escalations.filter(e => !reopenedIds.has(e.appt.id))'), true);
  check('3h the escalation sends the filtered list, not the raw one', /dueEscalations\.forEach/.test(cron) && !/\bescalations\.forEach/.test(cron), true);
  check('3i a dismissed card is still never reopened', /prior\[0\] === 'dismissed'/.test(cron), false);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nAll outcome-card-reopen tests passed.');
})();
