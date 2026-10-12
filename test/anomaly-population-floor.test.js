// Rules test for the anomaly population floor.  Run:  node test/anomaly-population-floor.test.js
//
// On 2026-09-30 the EOD Pulse reported "a systemic throughput problem" from
// phase cycle-time medians taken over 2 to 4 clients. A median over a handful of
// clients is one client's story, not a trend: the detector must stay quiet below
// ANOMALY_MIN_POPULATION, and the client-set scrapers must report how many
// clients they summarised.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(SRC.indexOf('// ─── ANOMALY POPULATION FLOOR (pure helpers)'), SRC.indexOf('// ─── END ANOMALY POPULATION FLOOR'));
if (block.length < 100) { console.error('FAIL: could not extract the population floor block'); process.exit(1); }
const { normalizeScrape, populationTooThin, ANOMALY_MIN_POPULATION } =
  new Function(`${block}; return { normalizeScrape, populationTooThin, ANOMALY_MIN_POPULATION };`)();

let failures = 0;
const check = (label, ok, detail) => {
  if (ok) console.log(`ok   ${label}`);
  else { failures++; console.error(`FAIL ${label}${detail !== undefined ? `\n  ${JSON.stringify(detail)}` : ''}`); }
};
const fnBody = name => { const i = SRC.indexOf(`async function ${name}(`); return SRC.slice(i, SRC.indexOf('\n}\n', i)); };

// 1. The floor
check('1a floor is 5 clients', ANOMALY_MIN_POPULATION === 5);
check('1b 3 clients is too thin', populationTooThin({ n: 3 }) === true);
check('1c 5 clients is enough', populationTooThin({ n: 5 }) === false);
check('1d readings without a count (sales, money) are never blocked', populationTooThin(null) === false && populationTooThin({}) === false);

// 2. Scraper results normalise either shape
check('2a {value, n} keeps n as meta', JSON.stringify(normalizeScrape({ value: 123, n: 3 })) === JSON.stringify({ value: 123, meta: { n: 3 } }));
check('2b a plain number has no meta', JSON.stringify(normalizeScrape(42)) === JSON.stringify({ value: 42, meta: null }));
check('2c null stays null (skipped upstream)', normalizeScrape(null).value === null);
check('2d zero is a value, not a skip', normalizeScrape({ value: 0, n: 8 }).value === 0);

// 3. Wiring: the client-set scrapers report n, the runner stores it, the detector checks it
for (const name of ['_scrapePhaseCycleP50', '_scrapeDaysToLaunchAvg', '_scrapePhase0ToPhase1Conv7d']) {
  check(`3a ${name} returns {value, n}`, /return \{ value: [^}]+, n: /.test(fnBody(name)), name);
}
check('3b the runner records meta with the observation', /recordObservation\(m\.name, m\.domain, value, 'anomaly-cron', meta\)/.test(SRC));
check('3c detectAnomaly applies the floor before z-scoring', (() => {
  const b = fnBody('detectAnomaly');
  return b.indexOf('populationTooThin(latest.meta)') > 0 && b.indexOf('populationTooThin') < b.indexOf('const z =');
})());
check('3d fulfillment anomalies reach the campaigns owner', /fulfillment:\s*\[[^\]]*'campaigns'/.test(SRC));

console.log(failures ? `\n${failures} failure(s).` : '\nAll anomaly population floor checks passed.');
process.exit(failures ? 1 : 0);
