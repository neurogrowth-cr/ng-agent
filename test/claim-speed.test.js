// Rules test for the Sales EOD claim-speed block.  Run:  node test/claim-speed.test.js
//
// Pins down how speed-to-lead is measured. The raw seconds_to_claim column counts
// wall-clock time, so a lead that arrives at 11 PM and is claimed at 7:05 AM reads
// as 8 hours and drowns the signal. The report counts BUSINESS minutes only
// (7 AM to 9 PM CR). It also divides "claimed within 15 min" by every lead, not
// only claimed ones, so an unclaimed lead can never flatter the rate.
// Extract-and-eval so the test can never drift from shipped code.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const CLAIM_SPEED_TARGET_MIN'),
  SRC.indexOf('// ─── END LEAD CLAIM SPEED'),
);
const { businessMinutesBetween, computeClaimSpeed, formatClaimMinutes, formatClaimSpeedLines } =
  new Function(`${block}; return { businessMinutesBetween, computeClaimSpeed, formatClaimMinutes, formatClaimSpeedLines };`)();

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });

// CR is UTC-6, no DST.
const CR = (y, mo, d, h, mi) => Date.UTC(y, mo - 1, d, h + 6, mi, 0);

// 1. Same-day, inside business hours: plain difference.
check('10:00 to 10:12 CR is 12 business minutes',
  businessMinutesBetween(CR(2026, 9, 18, 10, 0), CR(2026, 9, 18, 10, 12)) === 12);

// 2. THE OVERNIGHT CASE. Arrives 23:00, claimed 07:05 next morning: 5 minutes, not 8 hours.
check('lead at 23:00 claimed 07:05 next day is 5 business minutes',
  businessMinutesBetween(CR(2026, 9, 17, 23, 0), CR(2026, 9, 18, 7, 5)) === 5);

// 3. Spans the close: 20:50 to 07:10 next day is 10 + 10.
check('20:50 to 07:10 next day is 20 business minutes',
  businessMinutesBetween(CR(2026, 9, 17, 20, 50), CR(2026, 9, 18, 7, 10)) === 20);

// 4. Claimed before the day opens: zero, never negative.
check('lead at 05:00 claimed 06:30 is 0 business minutes',
  businessMinutesBetween(CR(2026, 9, 18, 5, 0), CR(2026, 9, 18, 6, 30)) === 0);

// 5. A full idle day in between counts its 14 business hours.
check('09:00 Monday to 09:00 Wednesday is 28 business hours',
  businessMinutesBetween(CR(2026, 9, 14, 9, 0), CR(2026, 9, 16, 9, 0)) === 28 * 60);

// 6. Clock skew (claim stamped before the post) is 0, not negative.
check('claim before post is 0',
  businessMinutesBetween(CR(2026, 9, 18, 10, 5), CR(2026, 9, 18, 10, 0)) === 0);

// 7. Month boundary.
check('Aug 31 20:55 to Sep 1 07:05 is 10 business minutes',
  businessMinutesBetween(CR(2026, 8, 31, 20, 55), CR(2026, 9, 1, 7, 5)) === 10);

// 8. Aggregation: rate is over ALL leads, median over claimed, per-setter split.
const now = CR(2026, 9, 18, 21, 0);
const leads = [
  { postedAtMs: CR(2026, 9, 18, 9, 0),  claimedAtMs: CR(2026, 9, 18, 9, 5),   setter: 'Oscar M' },      // 5
  { postedAtMs: CR(2026, 9, 18, 10, 0), claimedAtMs: CR(2026, 9, 18, 10, 15), setter: 'Oscar M' },      // 15 (on target: inclusive)
  { postedAtMs: CR(2026, 9, 18, 11, 0), claimedAtMs: CR(2026, 9, 18, 16, 30), setter: 'Sebastian S' },  // 330 (slow)
  { postedAtMs: CR(2026, 9, 18, 18, 0), claimedAtMs: null, setter: null },                              // unclaimed 3h
];
const r = computeClaimSpeed(leads, now);
check('counts: 4 total, 3 claimed, 1 unclaimed', r.total === 4 && r.claimed === 3 && r.unclaimed === 1, r);
check('15 minutes exactly counts as within target', r.withinTarget === 2, r);
check('one slow claim over 4 business hours', r.slow === 1, r);
check('median of claimed is 15', r.medianMin === 15, r);
check('longest unclaimed wait is 180 minutes', r.longestWaitMin === 180, r);
check('per-setter split, busiest first',
  r.bySetter[0].setter === 'Oscar M' && r.bySetter[0].claims === 2 && r.bySetter[0].medianMin === 10 &&
  r.bySetter[1].setter === 'Sebastian S' && r.bySetter[1].withinTarget === 0, r.bySetter);

// 9. Rendering: unclaimed leads sit in the denominator (2 of 4 = 50%, not 2 of 3).
const lines = formatClaimSpeedLines(r, null);
check('rate line uses all leads as denominator',
  lines[1] === 'Claimed within 15 min: 2 of 4 (50%). Target 80%.', lines[1]);
check('unclaimed line shows the longest wait',
  lines.some(l => l === 'Still unclaimed: 1 (longest waiting 3h 0m)'), lines);

// 10. Baseline appears only when it has enough claims to mean something.
const thin = computeClaimSpeed(leads.slice(0, 2), now);
check('a thin baseline (under 5 claims) is not rendered',
  !formatClaimSpeedLines(r, thin).some(l => l.includes('Prior 7 days')));
const thick = computeClaimSpeed([...leads, ...leads, ...leads], now);
check('a real baseline is rendered with its median and rate',
  formatClaimSpeedLines(r, thick).some(l => l.includes('Prior 7 days: 15 min, 50% within 15 min.')),
  formatClaimSpeedLines(r, thick));

// 11. No leads today: no section at all (no-change day stays quiet).
check('zero leads renders nothing', formatClaimSpeedLines(computeClaimSpeed([], now), thick).length === 0);

// 12. No em dashes in anything the team reads.
check('rendered lines contain no em or en dash', !formatClaimSpeedLines(r, thick).join('\n').match(/[—–]/));

check('formatClaimMinutes: 59 min, 1h 0m, n/a',
  formatClaimMinutes(59) === '59 min' && formatClaimMinutes(60) === '1h 0m' && formatClaimMinutes(null) === 'n/a');

const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ' ' + JSON.stringify(c.detail || {})}`);
console.log(`${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
