// The department drift metrics (2026-08-31) and the ad-sweep ranking. Run:
//   node test/department-metrics.test.js
//
// Two invariant families:
//  1. Registry hygiene — keys unique and frozen ones untouched, every new 7d
//     metric wired to a real scraper, domains legal for ANOMALY_ROUTING.
//  2. The ad sweep — since 2026-10-28 it ranks by bookings and closes through
//     lib/adAttribution.js sweepLines (tested here on rows; the gather is
//     tested in test/ad-attribution.test.js). Ads get a ranking, never
//     baseline rows (creative churn would sever history), and it must be
//     honest on degenerate weeks.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}

console.log('registry hygiene');
const regBlock = SRC.slice(SRC.indexOf('const METRIC_REGISTRY'), SRC.indexOf('async function runAnomalyDetection'));
const names   = [...regBlock.matchAll(/name: '([^']+)'/g)].map(m => m[1]);
const domains = [...regBlock.matchAll(/domain: '([^']+)'/g)].map(m => m[1]);
check('all metric keys are unique', names.length, new Set(names).size);
check('the frozen iclosed_* keys survive untouched',
  ['iclosed_calls_booked_yest', 'iclosed_calls_held_yest', 'iclosed_sales_yest'].every(k => names.includes(k)), true);
for (const k of ['roas_meta_7d', 'cpa_true_7d', 'leads_total_7d', 'revenue_won_7d',
                 'close_rate_7d', 'show_rate_7d', 'booking_rate_7d',
                 'fulfillment_days_to_launch_avg', 'fulfillment_pending_activations']) {
  check(`new key registered: ${k}`, names.includes(k), true);
}
const routingBlock = SRC.slice(SRC.indexOf('const ANOMALY_ROUTING'), SRC.indexOf('}', SRC.indexOf('const ANOMALY_ROUTING')));
check('every registry domain has anomaly routing',
  [...new Set(domains)].filter(d => !routingBlock.includes(`${d}:`)), []);
check('every new scraper exists as a function',
  ['_scrapeRoasMeta7d', '_scrapeCpaTrue7d', '_scrapeLeadsTotal7d', '_scrapeRevenueWon7d',
   '_scrapeCloseRate7d', '_scrapeShowRate7d', '_scrapeBookingRate7d',
   '_scrapeDaysToLaunchAvg', '_scrapePendingActivations']
    .filter(f => !SRC.includes(`async function ${f}(`)), []);

console.log('\nanchor discipline (source-level)');
{
  // The scraper's own comment NAMES go_live_at to explain why it is not used,
  // so the assertion is against code: no select() in the launch scraper may
  // read the column. Comments are stripped before checking.
  const fnBlock = SRC.slice(SRC.indexOf('async function _scrapeDaysToLaunchAvg'), SRC.indexOf('async function _scrapePendingActivations'));
  const code = fnBlock.replace(/\/\/[^\n]*/g, '');
  check('launch matches on the QA/validation templates', /campaign qa check\|campaign validation/.test(fnBlock), true);
  check('and never selects go_live_at', code.includes('go_live_at'), false);
}
check('show rate is outcome-derived, not attended-gated',
  /_scrapeShowRate7d[\s\S]{0,900}classifyOutcome/.test(SRC), true);
check('ROAS revenue anchors on the CRM won outcomes',
  /_wonOutcomes7d[\s\S]{0,400}revops_sales_outcomes/.test(SRC), true);
check('the ad sweep cron is in the liveness registry',
  /runWinningAdsSweep:\s+'0 20 \* \* 0'/.test(SRC), true);
{
  const sweep = SRC.slice(SRC.indexOf('async function runWinningAdsSweep'), SRC.indexOf('// ── Metric registry'));
  check('the ad sweep never mutates campaigns (no POST, no write call)',
    !/method:\s*'POST'/.test(sweep) && sweep.includes('getAdAttribution('), true);
  check('the ad sweep ranks bookings and closes, not form CPL',
    sweep.includes('adAttribution.sweepLines(') && !SRC.includes('function rankAdsForSweep'), true);
}

console.log('\nsweepLines — the ranking');
const lib = require('../lib/adAttribution');
const ROW = (adKey, adName, o) => ({ adKey, adId: adKey, adName, leads: 0, booked: 0, held: 0, wins: 0, revenue: 0, spend: 0, ...o });
const withRatios = (rows) => rows.map(r => ({ ...r, costPerBooked: r.spend && r.booked ? r.spend / r.booked : null }));
{
  const rows = withRatios([
    ROW('a', 'Closer ad', { booked: 3, wins: 1, spend: 300 }),
    ROW('b', 'Cheap booker', { booked: 4, spend: 120 }),
    ROW('c', 'Dud', { spend: 400 }),
    ROW('d', 'Closer ad', { booked: 1, wins: 1, spend: 50 }),   // same creative, other ad set
    ROW('organic', 'Organic (no ad)', { booked: 9, wins: 3 }),
  ]);
  const r = lib.sweepLines(rows, { prevScale: new Set(['Cheap booker']) });
  check('closers first, same-name ads grouped', r.lines[1], 'SCALE: Closer ad — 2 won, 4 booked at $87.50/booking');
  check('repeat scale is marked', r.lines[2], 'SCALE: Cheap booker — 0 won, 4 booked at $30.00/booking (2nd week running)');
  check('spend with zero bookings → kill', r.lines[3], 'KILL: Dud — $400 spend, 0 bookings');
  check('organic is never ranked', r.lines.some(l => l.includes('Organic')), false);
  check('header names the window and the basis', r.lines[0].startsWith('Ad ranking, last 30 days (first-touch credit'), true);
  check('the next sweep can parse SCALE names', [...r.lines.join('\n').matchAll(/SCALE: (.+?) —/g)].map(m => m[1]), ['Closer ad', 'Cheap booker']);
}
check('empty window is honest, not a crash',
  lib.sweepLines([]).lines[1], 'No ad stands out for bookings or closes in the last 30 days.');

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All department-metrics tests passed.');
