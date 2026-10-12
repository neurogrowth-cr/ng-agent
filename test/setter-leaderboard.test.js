// Rules test for the Setter Leaderboard numbers.  Run:  node test/setter-leaderboard.test.js
//
// Pins down the three faults found on the 2026-09-19 board, plus Ron's rule of
// 2026-09-21 for a booked call that never happened:
//   1. Calls cancelled in GHL were counted as "pending", so the board blamed
//      closers for 9 unlogged calls when 3 were cancellations nobody could log.
//      Rule: cancelled and never rebooked is a NO-SHOW; cancelled (or left
//      unlogged) and rebooked is a reschedule, and only the NEW appointment is
//      a booked call; deleted in GHL (QA bookings, duplicates) is not a call.
//   2. The claim-ownership read was capped at 1000 rows oldest-first, dropping
//      the newest claims, so calls were credited to the wrong setter. The tally
//      must honour a late claim no matter how many older claims precede it.
//   3. Distinct leads were keyed on email, so email-less prospects vanished and
//      the gap read as "prospects rebooked".
// Extract-and-eval so the test can never drift from shipped code.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const slice = (from, to) => SRC.slice(SRC.indexOf(from), SRC.indexOf(to, SRC.indexOf(from)));
const block = [
  slice('const SHOWED_OUTCOMES', '// prospect_id -> latest scheduled_start'),
  slice('function isAppointmentCancelled', '\n}\n') + '\n}\n',
  slice('function isAppointmentDeleted', '\n}\n') + '\n}\n',
  slice('function _newSetterSlot', '// ─── SETTER ATTRIBUTION RECONCILER'),
].join('\n');
const ROSTER = { 'seb@x.com': 'Sebastian S', 'oscar@x.com': 'Oscar M' };
// ghlFetch is stubbed per contact id so addFichaStats (the HEADLINE credit) runs
// against real lib/triageFicha parsing with no network.
const triageFicha = require('../lib/triageFicha');
let ghlNotesByContact = {};
let ghlCalls = 0;
const ghlFetchStub = async (url) => {
  ghlCalls++;
  const id = url.match(/contacts\/([^/]+)\/notes/)[1];
  const notes = ghlNotesByContact[id];
  if (notes === 'fail') return { ok: false, status: 500, json: async () => ({}) };
  return { ok: true, status: 200, json: async () => ({ notes }) };
};
const { tallySetterStats, formatSetterWeeklyStatsBlock, classifyUnheldAppt, latestStartFromRows, addFichaStats, _newSetterSlot } = new Function(
  'resolveSalesMember', 'isUnresolvedSalesId', 'ghlFetch', 'triageFicha',
  `${block}; return { tallySetterStats, formatSetterWeeklyStatsBlock, classifyUnheldAppt, latestStartFromRows, addFichaStats, _newSetterSlot };`,
)(id => ROSTER[id] || id, () => false, ghlFetchStub, triageFicha);

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });

const START = '2026-09-01T06:00:00.000Z';
const END   = '2026-09-19T13:00:00.000Z';
const NOW   = Date.parse(END);
let n = 0;
const appt = (o = {}) => ({
  id: `a${++n}`, prospect_id: `p${n}`, scheduled_start: '2026-09-05T16:00:00.000Z',
  setter_id: 'seb@x.com', qualification_snapshot: {}, prospect: { email: `lead${n}@x.com` }, ...o,
});

// 1. A booked call that never happened.
{
  const LATER = '2026-09-12T16:00:00.000Z';
  const appts = [
    appt({ id: 'showed' }),
    appt({ id: 'noshow' }),
    appt({ id: 'unlogged' }),
    appt({ id: 'cxl-final', qualification_snapshot: { ghl: { cancelled: true, opportunity_source: 'Facebook' } } }),
    appt({ id: 'cxl-rebooked', prospect_id: 'pRebook', qualification_snapshot: { ghl: { cancelled: true } } }),
    appt({ id: 'rebooked-new', prospect_id: 'pRebook', scheduled_start: LATER }),
    appt({ id: 'orphan', prospect_id: 'pOrphan' }),                       // no outcome, prospect rebooked later
    appt({ id: 'resched-row', prospect_id: 'pResched' }),                 // outcome literally `rescheduled`
    appt({ id: 'deleted-qa', qualification_snapshot: { cancelled: true, reason: 'deleted_in_ghl' } }),
    appt({ id: 'cxl-but-logged', qualification_snapshot: { ghl: { cancelled: true } } }),
    appt({ id: 'deleted-but-logged', qualification_snapshot: { cancelled: true, reason: 'deleted_in_ghl' } }),
  ];
  const outcomes = {
    showed: { outcome: 'follow_up' }, noshow: { outcome: 'no_show' }, 'rebooked-new': { outcome: 'lost' },
    'resched-row': { outcome: 'rescheduled' },
    'cxl-but-logged': { outcome: 'disqualified' }, 'deleted-but-logged': { outcome: 'follow_up' },
  };
  const latest = { pRebook: LATER, pOrphan: '2026-09-15T16:00:00.000Z', pResched: '2026-09-16T16:00:00.000Z' };
  const { stats, ownedCalls } = tallySetterStats(appts, outcomes, [], START, END, NOW, latest);
  const s = stats['Sebastian S'];
  // Counted: showed, noshow, unlogged, cxl-final, rebooked-new, cxl-but-logged, deleted-but-logged = 7
  check('rescheduled, orphaned and deleted rows are not booked calls', s.calls_booked === 7, s);
  check('cancelled and never rebooked is a no-show', s.no_shows === 2 && s.cancelled === 1, s);
  check('only the past, live, unlogged call is pending', s.pending === 1, s);
  check('a logged outcome outranks the cancel and delete flags', s.attended === 4 && s.aqc === 3, s);
  check('buckets add up to calls booked', s.attended + s.no_shows + s.pending === s.calls_booked, s);
  check('ownedCalls flags only the cancelled no-show', ownedCalls.filter(c => c.cancelled).length === 1 && ownedCalls.length === 7);
  check('ownedCalls marks held calls (the 4 attended) for the ficha check', ownedCalls.filter(c => c.held).length === 4 && ownedCalls.every(c => 'ghlContactId' in c), ownedCalls);

  const text = formatSetterWeeklyStatsBlock(stats, START, END);
  check('block folds cancellations into no-shows and says how many',
    /No-shows: 2 \(1 of them cancelled and never rebooked\)/.test(text) && /Awaiting closer outcome: 1/.test(text), text);
  check('show rate counts the cancellation as a no-show (4 of 6 decided)', /Show rate: 67% \(4 attended of 6 decided\)/.test(text), text);
  check('pod totals line carries the true pending count', /awaiting a closer-logged outcome: 1\./.test(text), text);
  check('lines I own carry no em or en dash', !text.split('\n').slice(2).join('\n').match(/[—–]/), text);

  check('classifier: real outcome always wins', classifyUnheldAppt(appts[9], outcomes['cxl-but-logged'], latest) === null);
  check('classifier: later appointment must be strictly later',
    classifyUnheldAppt(appts[5], undefined, latest) === null && classifyUnheldAppt(appts[4], undefined, latest) === 'skip');
}

// 2. A future-dated call is pending even if a leftover outcome row exists.
{
  const a = appt({ id: 'future', scheduled_start: '2026-09-25T16:00:00.000Z' });
  const { stats } = tallySetterStats([a], { future: { outcome: 'no_show' } }, [], START, END, NOW);
  check('reschedule-leftover outcome on a future call is ignored', stats['Sebastian S'].no_shows === 0 && stats['Sebastian S'].pending === 1);
}

// 3. Ownership: the latest claim wins, even when 1,200 older claims precede it.
{
  const filler = Array.from({ length: 1200 }, (_, i) => ({
    prospect_email: `old${i}@x.com`, claimed_by_setter_name: 'Oscar M', claimed_at: '2026-08-01T12:00:00+00:00',
  }));
  const claims = [
    ...filler,
    { prospect_email: 'Moved@X.com ', claimed_by_setter_name: 'Oscar M', claimed_at: '2026-09-08T21:35:00+00:00' },
    { prospect_email: 'selfbooked@x.com', claimed_by_setter_name: 'Sebastian S', claimed_at: '2026-09-04T01:13:00+00:00' },
    { prospect_email: null, claimed_by_setter_name: 'Sebastian S', claimed_at: '2026-09-10T12:00:00+00:00' },
  ];
  const appts = [
    appt({ id: 'moved', setter_id: 'seb@x.com', prospect: { email: 'moved@x.com' } }),       // booked by Seb, claimed by Oscar
    appt({ id: 'selfbooked', setter_id: null, prospect: { email: 'selfbooked@x.com' } }),    // widget booking, claimed by Seb
    appt({ id: 'nobody', setter_id: null, prospect: { email: 'nobody@x.com' } }),            // no claim, no booker
  ];
  const { stats } = tallySetterStats(appts, {}, claims, START, END, NOW);
  check('claim owner beats the GHL booker', stats['Oscar M'].calls_booked === 1, stats);
  check('claimed self-booking is credited to the claimer', stats['Sebastian S'].calls_booked === 1, stats);
  check('unclaimed, unbooked call is credited to nobody', Object.values(stats).reduce((t, s) => t + s.calls_booked, 0) === 2);
  check('leads claimed counts only in-window claims, email or not',
    stats['Sebastian S'].leads_claimed === 2 && stats['Oscar M'].leads_claimed === 1, stats);
}

// 4. Distinct leads: email-less prospects are still distinct; a rebook collapses.
{
  const appts = [
    appt({ prospect_id: 'pA', prospect: { email: null } }),
    appt({ prospect_id: 'pB', prospect: { email: null } }),
    appt({ prospect_id: 'pC', prospect: { email: 'c@x.com' } }),
    appt({ prospect_id: 'pC', prospect: { email: 'c@x.com' } }), // the one real rebook
  ];
  const { stats } = tallySetterStats(appts, {}, [], START, END, NOW);
  check('4 calls from 3 distinct leads (not 1)', stats['Sebastian S'].calls_booked === 4 && stats['Sebastian S'].distinct_leads === 3, stats);
}

// 5. A deleted duplicate never supersedes the real call (Juan Jose Hernandez,
//    2026-09-30): setter call at 17:30, self-serve duplicate at 18:00 deleted.
{
  const DEL = { cancelled: true, reason: 'deleted_in_ghl' };
  const rows = [
    { prospect_id: 'pJJ', scheduled_start: '2026-09-30 17:30:00+00', qualification_snapshot: {} },
    { prospect_id: 'pJJ', scheduled_start: '2026-09-30 18:00:00+00', qualification_snapshot: DEL },
    { prospect_id: 'pR',  scheduled_start: '2026-09-10 16:00:00+00', qualification_snapshot: { ghl: { cancelled: true } } },
    { prospect_id: 'pR',  scheduled_start: '2026-09-12 16:00:00+00', qualification_snapshot: {} },
  ];
  const latest = latestStartFromRows(rows);
  check('deleted later row is ignored for the latest start', latest.pJJ === '2026-09-30 17:30:00+00', latest);
  check('a live rebook still supersedes', latest.pR === '2026-09-12 16:00:00+00', latest);
  const real = appt({ id: 'jj-real', prospect_id: 'pJJ', scheduled_start: '2026-09-30 17:30:00+00' });
  const dup  = appt({ id: 'jj-dup',  prospect_id: 'pJJ', scheduled_start: '2026-09-30 18:00:00+00', qualification_snapshot: DEL });
  const { stats } = tallySetterStats([real, dup], {}, [], START, '2026-10-02T00:00:00.000Z', Date.parse('2026-10-02T00:00:00.000Z'), latest);
  check('the real call counts once and is pending', stats['Sebastian S']?.calls_booked === 1 && stats['Sebastian S'].pending === 1, stats);
}

// 6. HEADLINE (Ron, 2026-10-11, script audit decision 4): held calls with a
//    complete triage ficha lead the board; bookings, show rate and AQC follow.
{
  const stats = {
    'Oscar M':     { ..._newSetterSlot(), leads_claimed: 9, calls_booked: 6, attended: 4, no_shows: 1, pending: 1, aqc: 3, held_with_ficha: 1, ficha_incomplete: 2 },
    'Sebastian S': { ..._newSetterSlot(), leads_claimed: 4, calls_booked: 3, attended: 3, aqc: 2, held_with_ficha: 3 },
    'Josue D':     { ..._newSetterSlot(), leads_claimed: 2, calls_booked: 2, pending: 2 },
  };
  const text = formatSetterWeeklyStatsBlock(stats, START, END);
  const order = text.split('\n').filter(l => /^#\d /.test(l)).map(l => l.replace(/^#\d /, ''));
  check('ranked by held calls with a complete ficha, not by calls', order.join(',') === 'SEBASTIAN S,OSCAR M,JOSUE D', order);
  const lines = text.split('\n');
  const after = name => lines[lines.findIndex(l => l.endsWith(name)) + 1];
  check('HEADLINE is the first line under every setter',
    /^  HEADLINE: Held calls with complete ficha: 3 of 3 held \(100%\)$/.test(after('SEBASTIAN S')) &&
    /^  HEADLINE: Held calls with complete ficha: 1 of 4 held \(25%\) \| ficha started but a claro blank: 2$/.test(after('OSCAR M')) &&
    /^  HEADLINE: Held calls with complete ficha: 0 \(no held calls yet\)$/.test(after('JOSUE D')), [after('SEBASTIAN S'), after('OSCAR M'), after('JOSUE D')]);
  check('bookings, show rate and AQC still follow as diagnostics',
    /OWNED: Leads claimed: 9 \| Calls from their leads: 6/.test(text) && /Show rate: 80% \(4 attended of 5 decided\)/.test(text) && /Converted calls \(AQC, closer-logged\): 3/.test(text), text);
  check('intro names the ficha rule and the six claros', /COMPLETE TRIAGE FICHA/.test(lines[1]) && /COMPROMISOS/.test(lines[1]) && /Ranked by held calls with a complete ficha/.test(lines[1]));
  check('lines I own carry no em or en dash', !lines.slice(2).join('\n').match(/[—–]/), text);
}

// 7. addFichaStats credits the OWNER, reads each contact once, and never turns a
//    failed GHL read into "no ficha".
(async () => {
  const fs2 = require('fs');
  const TEMPLATE = fs2.readFileSync(path.join(__dirname, 'triage-ficha.test.js'), 'utf8');
  // Reuse the blank v1.2 template from the ficha test and fill one answer per claro.
  const blank = TEMPLATE.slice(TEMPLATE.indexOf('`FICHA DE TRIAGE v1.2') + 1, TEMPLATE.indexOf('decide solo.`') + 'decide solo.'.length);
  const complete = blank
    .replace('Vende: ______ a: ______', 'Vende: software a: bancos')
    .replace('Cobra por cliente: $______', 'Cobra por cliente: $2,500')
    .replace('Por qué ahora: ______', 'Por qué ahora: cerró un canal')
    .replace('[ ] Solo  [ ] Con: ______', '[x] Solo  [ ] Con: ______')
    .replace('Respuesta: ______', 'Respuesta: ok')
    .replace('[ ] Material visto', '[x] Material visto');
  const incomplete = complete.replace('[x] Material visto', '[ ] Material visto');
  ghlNotesByContact = {
    cA: [{ bodyText: 'nota suelta' }, { bodyText: complete }],
    cB: [{ bodyText: incomplete }],
    cC: 'fail',
    cD: [{ bodyText: 'sin ficha' }],
  };
  ghlCalls = 0;
  const stats = { 'Sebastian S': _newSetterSlot(), 'Oscar M': _newSetterSlot() };
  stats['Sebastian S'].attended = 4; stats['Oscar M'].attended = 2;
  const owned = [
    { owner: 'Sebastian S', held: true,  ghlContactId: 'cA' },
    { owner: 'Sebastian S', held: true,  ghlContactId: 'cA' },   // rebooked prospect: one read, credited twice
    { owner: 'Sebastian S', held: true,  ghlContactId: 'cB' },
    { owner: 'Sebastian S', held: true,  ghlContactId: null },   // iClosed-era prospect, no GHL contact id
    { owner: 'Sebastian S', held: false, ghlContactId: 'cD' },   // not held: never read
    { owner: 'Oscar M',     held: true,  ghlContactId: 'cC' },
    { owner: 'Oscar M',     held: true,  ghlContactId: 'cD' },
    { owner: 'Ghost',       held: true,  ghlContactId: 'cA' },   // no slot: ignored
  ];
  await addFichaStats(stats, owned);
  const s = stats['Sebastian S'], o = stats['Oscar M'];
  check('complete ficha credited to the owner once per held call', s.held_with_ficha === 2, s);
  check('ficha with a blank claro counts as incomplete', s.ficha_incomplete === 1, s);
  check('no GHL contact id is unchecked, not "no ficha"', s.ficha_unchecked === 1, s);
  check('a failed notes read is unchecked, a note without a ficha is nothing', o.ficha_unchecked === 1 && o.held_with_ficha === 0 && o.ficha_incomplete === 0, o);
  check('one GHL read per distinct held contact (cA, cB, cC, cD)', ghlCalls === 4, ghlCalls);
  const text = formatSetterWeeklyStatsBlock(stats, START, END);
  check('unchecked is said out loud on the board', /2 of 4 held \(50%\) \| ficha started but a claro blank: 1 \| not checked: 1 \(GHL contact notes unreadable\)/.test(text), text);

  const failed = cases.filter(c => !c.ok);
  for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ' ' + JSON.stringify(c.detail || {})}`);
  console.log(`${cases.length - failed.length}/${cases.length} passed`);
  if (failed.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
