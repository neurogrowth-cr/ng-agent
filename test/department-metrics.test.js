// The department drift metrics (2026-08-31) and the ad-sweep ranking. Run:
//   node test/department-metrics.test.js
//
// Two invariant families:
//  1. Registry hygiene — keys unique and frozen ones untouched, every new 7d
//     metric wired to a real scraper, domains legal for ANOMALY_ROUTING.
//  2. rankAdsForSweep — pure ranking math, sliced from index.js. Ads get a
//     ranking, never baseline rows (creative churn would sever history), and
//     the function must be honest on degenerate weeks.
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
check('the ad sweep never mutates campaigns (read-only Graph call)',
  /runWinningAdsSweep[\s\S]{0,1200}graph\.facebook\.com[\s\S]{0,200}\/ads\?fields=/.test(SRC)
    && !/runWinningAdsSweep[\s\S]{0,3000}method:\s*'POST'/.test(SRC), true);

console.log('\nrankAdsForSweep — the ranking math');
const rankBlock = SRC.slice(SRC.indexOf('function rankAdsForSweep'), SRC.indexOf('// ─── end ad sweep ranking'));
const g = new Function(`${rankBlock}; return { rankAdsForSweep };`)();
const AD = (name, spend, leads) => ({ name, spend, leads });

{
  // Account: $1000 across 4 ads, 100 leads → $10 CPL average.
  const ads = [
    AD('winner', 250, 50),    // $5 CPL, 25% spend share → scale
    AD('loser', 250, 10),     // $25 CPL, 25% share → kill
    AD('average', 400, 36),   // ~$11 CPL → neither
    AD('tiny-loser', 100, 4), // $25 CPL but 10% share — above min, kill
  ];
  const r = g.rankAdsForSweep(ads);
  check('account CPL computed across lead-firing ads', r.accountCpl, 10);
  check('cheap ad with real spend → scale', r.scale.map(a => a.name), ['winner']);
  check('expensive ads with real spend → kill, worst first', r.kill.map(a => a.name), ['loser', 'tiny-loser']);
}
{
  const r = g.rankAdsForSweep([AD('winner', 250, 50), AD('dust', 3, 1), AD('rest', 750, 55)]);
  check('an ad below the spend-share floor is never ranked either way',
    [r.scale.some(a => a.name === 'dust'), r.kill.some(a => a.name === 'dust')], [false, false]);
}
{
  // VSL ads fire no `lead` action → leads 0 → excluded from CPL math entirely.
  const r = g.rankAdsForSweep([AD('vsl-ad', 500, 0), AD('form-ad', 100, 20)]);
  check('zero-lead (VSL) ads are excluded from the account CPL', r.accountCpl, 5);
}
check('empty week is honest, not a crash',
  g.rankAdsForSweep([]), { accountCpl: null, scale: [], kill: [] });
check('null input does not throw',
  g.rankAdsForSweep(null), { accountCpl: null, scale: [], kill: [] });
{
  // One ad IS the account average — never both lists, never either.
  const r = g.rankAdsForSweep([AD('only', 100, 10)]);
  check('a single ad cannot deviate from itself', [r.scale.length, r.kill.length], [0, 0]);
}

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All department-metrics tests passed.');
