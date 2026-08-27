// The catalog of record must never live in this repo. Run:
//   node test/offer-catalog.test.js
//
// ng-agent is PUBLIC; the repo that owns the catalog
// (neurogrowth-cr/roi-rm-okr-reporting, docs/plan-of-record.md §2) is PRIVATE.
// On 2026-08-26 the locked catalog was written straight into the system prompt
// to close an overdue north-star item, and that published the founding
// $325/$975 prices, the 60/40 payment split and the upfront→day-120 lever to
// anyone on the internet. The catalog now lives in one Supabase row loaded at
// boot; scripts/sync-offer-catalog.js copies §2 there from the private clone.
//
// This test is the thing that stops it coming back. It reads TRACKED SOURCE —
// index.js and everything in scripts/ — and fails on any price literal, any
// retired SKU, and any dead price. It also pins the loader contract: the prompt
// carries the token, both prompt paths get substituted, the catalog loads
// before the socket opens, and a failed load makes Max refuse rather than
// improvise.
const fs   = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const INDEX = process.argv[2] || path.join(ROOT, 'index.js');
const SRC = fs.readFileSync(INDEX, 'utf8');

// Every tracked source file that ships publicly. scripts/ is in the repo too —
// the first version of the sync script hardcoded the catalog, which would have
// defeated the entire change.
const SCRIPTS_DIR = path.join(ROOT, 'scripts');
const PUBLIC_FILES = [['index.js', SRC]].concat(
  fs.existsSync(SCRIPTS_DIR)
    ? fs.readdirSync(SCRIPTS_DIR).filter(f => f.endsWith('.js'))
        .map(f => [`scripts/${f}`, fs.readFileSync(path.join(SCRIPTS_DIR, f), 'utf8')])
    : [],
);

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}
// Reports which public file leaked, not just that something did.
function nowhereInPublicSource(label, re) {
  const hits = PUBLIC_FILES.filter(([, body]) => re.test(body)).map(([name]) => name);
  check(label, hits, []);
}

console.log('no price or term from the catalog may appear in tracked source');
nowhereInPublicSource('Build & Release price $4,997',        /\$4,?997/);
nowhereInPublicSource('ROLEX price $475',                    /\$475/);
nowhereInPublicSource('PATEK price $1,297',                  /\$1,?297/);
nowhereInPublicSource('embedded-tier value $1,425',          /\$1,?425/);
nowhereInPublicSource('founding ROLEX price $325',           /\$325/);
nowhereInPublicSource('founding PATEK price $975',           /\$975/);
nowhereInPublicSource('the 60/40 payment split',             /\b60\/40\b/);
nowhereInPublicSource('unconfirmed US placeholders',         /\$997|\$2,?497|\$8,?000|\$10,?000/);
// Catch-all for a price nobody thought to list here — a future rung, a changed
// number. Scoped to three digits or more, or a thousands comma, or a /mo rate:
// broad enough to catch any plausible catalog price, narrow enough to ignore
// SQL placeholders ($1, $2), regex backreferences, and the $25 vendor-spend
// escalation threshold, which is a rule rather than something we sell.
nowhereInPublicSource('any amount that could be a price',
  /\$\s?\d[\d,]*,\d{3}|\$\s?\d{3,}|\$\s?\d+\s*\/\s*mo/);

console.log('\nretired SKUs are not named in tracked source either');
nowhereInPublicSource('OMEGA',                               /\bOMEGA\b/);
nowhereInPublicSource('the old 6-month DWY/DFY tiers',       /6-month Done-(With|For)-You/);
nowhereInPublicSource('seat counts',                         /15 slots|8 slots/);

console.log('\nthe prompt carries the token, not the catalog');
check('the {{OFFER_CATALOG}} token is in the prompt', SRC.includes('{{OFFER_CATALOG}}'), true);
check('the token is a literal, not a template interpolation',
  /\$\{OFFER_CATALOG\}/.test(SRC), false);
check('ROLEX and PATEK are still named as tier names (Max needs the vocabulary)',
  /ROLEX/.test(SRC) && /PATEK/.test(SRC), true);

console.log('\nthe loader contract');
check('the catalog is read from agent_knowledge config/offer_catalog',
  /category', 'config'[\s\S]{0,200}OFFER_CATALOG_KEY|OFFER_CATALOG_KEY = 'offer_catalog'/.test(SRC), true);
check('loadOfferCatalog runs BEFORE slack.start()',
  SRC.indexOf('await loadOfferCatalog()') < SRC.indexOf('await slack.start()')
    && SRC.indexOf('await loadOfferCatalog()') !== -1, true);
check('a failed load alerts Ron rather than passing silently',
  /CATALOG LOAD FAILED[\s\S]{0,600}RON_SLACK_ID/.test(SRC), true);

console.log('\nsubstitution behaviour (sliced from index.js, so it cannot drift)');
const block = SRC.slice(SRC.indexOf('const OFFER_CATALOG_KEY'), SRC.indexOf('async function loadOfferCatalog'));
const g = new Function(`${block}; return { getOfferCatalog, injectOfferCatalog, OFFER_CATALOG_UNAVAILABLE, setCatalog: v => { OFFER_CATALOG = v; } };`)();

check('with no catalog loaded, the token becomes the refusal text',
  g.injectOfferCatalog('A {{OFFER_CATALOG}} B'), `A ${g.OFFER_CATALOG_UNAVAILABLE} B`);
g.setCatalog('ROLEX is $1/mo.');
check('with a catalog loaded, the token becomes the catalog',
  g.injectOfferCatalog('A {{OFFER_CATALOG}} B'), 'A ROLEX is $1/mo. B');
check('every occurrence is substituted, not just the first',
  g.injectOfferCatalog('{{OFFER_CATALOG}}|{{OFFER_CATALOG}}'), 'ROLEX is $1/mo.|ROLEX is $1/mo.');
check('a prompt without the token is returned untouched',
  g.injectOfferCatalog('no token here'), 'no token here');
check('null prompt does not throw', g.injectOfferCatalog(null), '');
g.setCatalog('   ');
check('a blank catalog row counts as unavailable, not as an empty catalog',
  g.injectOfferCatalog('{{OFFER_CATALOG}}').trim(), g.OFFER_CATALOG_UNAVAILABLE);

console.log('\nthe unavailable text refuses instead of guessing');
const U = g.OFFER_CATALOG_UNAVAILABLE;
check('it forbids quoting a price from memory', /Do NOT state a price/i.test(U), true);
check('it closes the "but I am confident" loophole', /confident/i.test(U), true);
check('it routes to Ron', /Ron/.test(U), true);
check('it does not disable Max entirely', /Answer everything else normally/i.test(U), true);
check('it contains no price of its own', /\$\s?\d/.test(U), false);

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All offer-catalog tests passed.');
