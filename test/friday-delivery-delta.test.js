// Rules test for the Friday delivery delta.  Run:  node test/friday-delivery-delta.test.js
//
// The LLM "Friday Delivery Wrap-Up" rebuilt the whole roster every week and got
// zero reactions in five weeks. Its replacement follows Ron's 2026-08-31 rule
// for the Monday gap report: name only what changed since last Friday, collapse
// standing holds to a count, and post NOTHING on a week with no change.
// Extracts the real helpers out of index.js so the test cannot drift.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const slice = (a, b) => SRC.slice(SRC.indexOf(a), SRC.indexOf(b));
const gap    = slice('// ─── GAP REPORT GROOMING (pure helpers)', '// ─── END GAP REPORT GROOMING');
const friday = slice('// ─── FRIDAY DELIVERY DELTA (pure helpers)', '// ─── END FRIDAY DELIVERY DELTA');
if (gap.length < 100 || friday.length < 100) { console.error('FAIL: could not extract helper blocks'); process.exit(1); }
const { computeFridayDelta, fridayOwnersFrom, fridayUpcoming, formatFridayDelta, FRIDAY_MAX_PRIORITIES } =
  new Function(`${gap}\n${friday};
  return { computeFridayDelta, fridayOwnersFrom, fridayUpcoming, formatFridayDelta, FRIDAY_MAX_PRIORITIES };`)();

let failures = 0;
function check(label, ok, detail) {
  if (ok) console.log(`ok   ${label}`);
  else { failures++; console.error(`FAIL ${label}${detail !== undefined ? `\n  ${typeof detail === 'string' ? detail : JSON.stringify(detail)}` : ''}`); }
}

const TODAY = '2026-10-16';
const owners = fridayOwnersFrom({
  U1: { name: 'Josue',  role: 'tech_ops' },
  U2: { name: 'Gerald', role: 'campaigns' },
  U3: { name: 'Valeria', role: 'fulfillment' },
});
const c = (name, status, daysSince) => ({ name, status, daysSince });
const prev = {
  date: '2026-10-09',
  clients: {
    'Crosser':     { d: 12, s: 'phase_2' },
    'Old Overdue': { d: 20, s: 'phase_1' },
    'Blocker':     { d: 10, s: 'blocked' },
    'Unblocked':   { d: 30, s: 'blocked' },
    'Launched Out': { d: 18, s: 'phase_2' },
    'Churned':     { d: 40, s: 'phase_1' },
    'Was At Risk': { d: 9, s: 'phase_1' },
    'Fixed':       { d: 25, s: 'phase_1' },
  },
  launched: { 'Seen Before': '2026-10-08T15:00:00Z', 'Ancient': '2026-08-01T15:00:00Z' },
};
const current = [
  c('Crosser', 'phase_2', 15),
  c('Old Overdue', 'phase_1', 27),
  c('Blocker', 'blocked', 17),
  c('Unblocked', 'phase_2', 10),
  c('Brand New Late', 'phase_1', 21),
  c('Soon', 'phase_1', 12),
  c('Later', 'phase_2', 8),
  c('Fresh', 'phase_1', 3),
];
const launches = [
  { name: 'Launched Out', launchedAt: '2026-10-14T18:00:00Z', daysToLaunch: 21 },
  { name: 'Seen Before',  launchedAt: '2026-10-08T15:00:00Z', daysToLaunch: 12 },
  { name: 'No Anchor',    launchedAt: '2026-10-13T18:00:00Z', daysToLaunch: null },
];
const d = computeFridayDelta(prev, { clients: current, launches, stillActive: ['Launched Out', 'Was At Risk', 'Fixed'], todayISO: TODAY });
const names = arr => arr.map(x => x.name || x);

// ── 1. The transition engine ────────────────────────────────────────────────
check('1a Day 12 to 15 is a window crossing', names(d.crossed).includes('Crosser'), names(d.crossed));
check('1b Day 20 to 27 does not cross again', !names(d.crossed).includes('Old Overdue'), names(d.crossed));
check('1c a blocked client never crosses, it is a hold', !names(d.crossed).includes('Blocker'), names(d.crossed));
check('1d a client new to the list already past Day 14 is a crossing', names(d.crossed).includes('Brand New Late'));
check('1e crossings sort by days, most overdue first', names(d.crossed).join() === 'Brand New Late,Crosser', names(d.crossed));
check('1f unblocked and under Day 14 moved forward', d.resolved.includes('Unblocked'), d.resolved);
check('1g left the list by launching moved forward', d.resolved.includes('Launched Out'), d.resolved);
check('1h left the portal (churned or deactivated) is NOT moved forward', !d.resolved.includes('Churned'), d.resolved);
check('1i never flagged last week is never "moved forward"', !d.resolved.includes('Was At Risk'), d.resolved);
check('1j still flagged is not resolved', !d.resolved.includes('Old Overdue') && !d.resolved.includes('Blocker'), d.resolved);
check('1k at risk is Day 7 to 13 only, closest to Day 14 first', names(d.atRisk).join() === 'Soon,Unblocked,Later', names(d.atRisk));
// flagged now: Crosser, Old Overdue, Blocker, Brand New Late = 4; minus 2 crossings
check('1l holds count excludes this week\'s crossings', d.holdsCount === 2, d.holdsCount);
check('1m a launch already announced is not announced again', names(d.launched).join() === 'Launched Out,No Anchor', names(d.launched));
check('1n launch memory drops entries past 28 days', !('Ancient' in d.nextState.launched) && 'Seen Before' in d.nextState.launched, d.nextState.launched);
check('1o next state carries every in-flight client with day and status',
  Object.keys(d.nextState.clients).length === current.length && d.nextState.clients.Crosser.d === 15 && d.nextState.clients.Blocker.s === 'blocked');
check('1p next state is dated today', d.nextState.date === TODAY);
{
  const z = computeFridayDelta(null, { clients: current, launches: [], stillActive: [], todayISO: TODAY });
  check('1q no previous state still computes a full next state', Object.keys(z.nextState.clients).length === current.length && z.resolved.length === 0);
}

// ── 2. Owners and the coming week ───────────────────────────────────────────
{
  const many = { atRisk: [c('A', 'phase_1', 13), c('B', 'phase_2', 12), c('C', 'phase_1', 9), c('D', 'phase_1', 7)] };
  const u = fridayUpcoming(many, owners);
  check('2a capped', u.length === FRIDAY_MAX_PRIORITIES, u);
  check('2b closest to Day 14 first', /^• A hits Day 14 in 1 day /.test(u[0]) && /^• B /.test(u[1]), u);
  check('2c Phase 2 routes to the campaigns owner', /B .*<@U2>/.test(u[1]), u[1]);
  check('2d Phase 1 routes to tech ops', /A .*<@U1>/.test(u[0]), u[0]);
  const lone = fridayUpcoming(many, fridayOwnersFrom({ U1: { name: 'Josue', role: 'tech_ops' } }));
  check('2e with no campaigns owner, Phase 2 falls back to tech ops', /B .*<@U1>/.test(lone[1]), lone[1]);
}

// ── 3. The post ─────────────────────────────────────────────────────────────
{
  const { message, hasSignal } = formatFridayDelta({ todayLabel: 'October 16', firstRun: false, delta: d, owners });
  check('3a a week with changes posts', hasSignal === true);
  const order = ['*Friday delivery, October 16*', '*Launched this week*', '*Missed the 14-day window*', '*Moved forward*', 'flagged in earlier weeks', '*Hits Day 14 before next Friday*'];
  const idx = order.map(h => message.indexOf(h));
  check('3b sections in order', idx.every((v, i) => v >= 0 && (i === 0 || v > idx[i - 1])), idx);
  check('3c launch line shows days from the activation call', /Launched Out launched, 21 days from the activation call\./.test(message));
  check('3d launch without an anchor says so instead of a number', /No Anchor launched\. No activation call date on file\./.test(message));
  check('3e holds are a count, never names', /2 clients flagged in earlier weeks are unchanged/.test(message) && !/Old Overdue/.test(message), message);
  check('3e2 a miss carries its owner on its own line', /Crosser crossed Day 14 in Phase 2 \(now Day 15\)\. Owner: <@U2>\./.test(message), message);
  const mentions = ['Brand New Late', 'Crosser', 'Soon', 'Unblocked', 'Launched Out'].map(n => message.split(n).length - 1);
  check('3e3 no client is named twice', mentions.every(n => n <= 1), mentions);
  check('3e4 moved forward keeps the plain case and drops the launched and the at-risk', /\*Moved forward\*\nFixed is no longer flagged\./.test(message), message);
  check('3f no em or en dashes', !/[\u2013\u2014]/.test(message), message);
  check('3g no emoji', !/\p{Extended_Pictographic}/u.test(message), message);
  check('3h no pipe separator chains', !/ \| /.test(message));
  const words = message.split(/\s+/).filter(Boolean).length;
  check(`3i a busy week stays under 150 words (${words})`, words < 150, message);
}

// ── 4. No change, no post ───────────────────────────────────────────────────
{
  const steady = { clients: { 'Hold A': { d: 40, s: 'blocked' }, 'Late B': { d: 30, s: 'phase_1' } }, launched: {} };
  const s = computeFridayDelta(steady, { clients: [c('Hold A', 'blocked', 47), c('Late B', 'phase_1', 37), c('Risky', 'phase_1', 9)], launches: [], stillActive: [], todayISO: TODAY });
  const out = formatFridayDelta({ todayLabel: 'October 16', firstRun: false, delta: s, owners });
  check('4a holds and at-risk alone do not post', out.hasSignal === false, out.message);
  const l = computeFridayDelta(steady, { clients: [c('Hold A', 'blocked', 47)], launches: [{ name: 'Win', launchedAt: '2026-10-15T12:00:00Z', daysToLaunch: 13 }], stillActive: ['Late B'], todayISO: TODAY });
  check('4b a launch-only week posts', formatFridayDelta({ todayLabel: 'x', firstRun: false, delta: l, owners }).hasSignal === true);
}

// ── 5. First run sets the baseline ──────────────────────────────────────────
{
  const f = computeFridayDelta(null, { clients: current, launches: [], stillActive: [], todayISO: TODAY });
  const quiet = formatFridayDelta({ todayLabel: 'October 16', firstRun: true, delta: f, owners });
  check('5a first run with no launches is silent', quiet.hasSignal === false);
  check('5b first run never names misses', !/Missed the 14-day window/.test(quiet.message));
  check('5c first run counts every flagged client as a hold', /4 clients flagged in earlier weeks/.test(quiet.message), quiet.message);
  const fl = computeFridayDelta(null, { clients: current, launches: [launches[0]], stillActive: [], todayISO: TODAY });
  const loud = formatFridayDelta({ todayLabel: 'October 16', firstRun: true, delta: fl, owners });
  check('5d first run with a launch posts it, with the baseline note', loud.hasSignal && /Launched Out launched/.test(loud.message) && /sets the baseline/.test(loud.message), loud.message);
  check('5e first run never names an unannounced miss anywhere', !/Brand New Late/.test(loud.message), loud.message);
}

console.log(failures ? `\n${failures} failure(s).` : '\nAll Friday delivery delta checks passed.');
process.exit(failures ? 1 : 0);
