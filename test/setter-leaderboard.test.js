// Rules test for the Setter Leaderboard numbers.  Run:  node test/setter-leaderboard.test.js
//
// Pins down the three faults found on the 2026-09-19 board:
//   1. Calls cancelled in GHL were counted as "pending", so the board blamed
//      closers for 9 unlogged calls when 3 were cancellations nobody could log.
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
  slice('const SHOWED_OUTCOMES', '// ─── CLOSER WEEKLY STATS'),
  slice('function isAppointmentCancelled', '\n}\n') + '\n}\n',
  slice('function _newSetterSlot', '// ─── SETTER ATTRIBUTION RECONCILER'),
  slice('function summarizeSetterInbox', '// Conversations ASSIGNED'),
].join('\n');
const ROSTER = { 'seb@x.com': 'Sebastian S', 'oscar@x.com': 'Oscar M' };
const { tallySetterStats, formatSetterWeeklyStatsBlock, summarizeSetterInbox } = new Function(
  'resolveSalesMember', 'isUnresolvedSalesId',
  `${block}; return { tallySetterStats, formatSetterWeeklyStatsBlock, summarizeSetterInbox };`,
)(id => ROSTER[id] || id, () => false);

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

// 1. Cancelled calls: both shapes dash writes, neither is ever "pending".
{
  const appts = [
    appt({ id: 'showed' }),
    appt({ id: 'noshow' }),
    appt({ id: 'unlogged' }),
    appt({ id: 'cxl-webhook', qualification_snapshot: { ghl: { cancelled: true, opportunity_source: 'Facebook' } } }),
    appt({ id: 'cxl-deleted', qualification_snapshot: { cancelled: true, reason: 'deleted_in_ghl' } }),
    appt({ id: 'cxl-but-logged', qualification_snapshot: { ghl: { cancelled: true } } }),
  ];
  const outcomes = {
    showed: { outcome: 'follow_up' }, noshow: { outcome: 'no_show' }, 'cxl-but-logged': { outcome: 'disqualified' },
  };
  const { stats, ownedCalls } = tallySetterStats(appts, outcomes, [], START, END, NOW);
  const s = stats['Sebastian S'];
  check('cancelled calls land in their own bucket', s.cancelled === 2, s);
  check('only the past, live, unlogged call is pending', s.pending === 1, s);
  check('a logged outcome outranks the cancel flag', s.attended === 2 && s.aqc === 1, s);
  check('buckets add up to calls booked', s.attended + s.no_shows + s.pending + s.cancelled === s.calls_booked, s);
  check('ownedCalls flags the cancelled calls', ownedCalls.filter(c => c.cancelled).length === 2);

  const text = formatSetterWeeklyStatsBlock(stats, START, END);
  check('block states cancelled separately from awaiting-outcome',
    /Cancelled before the call: 2/.test(text) && /Awaiting closer outcome: 1/.test(text), text);
  check('show rate excludes cancelled and pending (2 of 3 decided)', /Show rate: 67% \(2 attended of 3 decided\)/.test(text), text);
  check('pod totals line carries the true pending count', /awaiting a closer-logged outcome: 1 \| cancelled before the call: 2/.test(text), text);
  check('lines I own carry no em or en dash', !text.split('\n').slice(2).join('\n').match(/[—–]/), text);
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

// 5. Inbox hygiene: only in-window conversations count; 3d+ is a subset.
{
  const day = 24 * 60 * 60 * 1000;
  const r = summarizeSetterInbox([
    { lastMessageDate: NOW - 1 * day },
    { lastMessageDate: NOW - 4 * day },
    { lastMessageDate: Date.parse(START) - 1 }, // before the window
    {},                                          // malformed row
  ], Date.parse(START), NOW);
  check('hygiene counts in-window only, stale is 3d+', r.waiting === 2 && r.stale === 1, r);
}

const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ' ' + JSON.stringify(c.detail || {})}`);
console.log(`${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
