// Rules test for Max's closer alias resolution in the monthly scorecard.
//   Run:  node test/closer-aliases.test.js
//
// Jose's GHL email changed in late Aug 2026, so closer_id carries two addresses and
// the scorecard split him in two. The dash view now merges them through
// public.revops_closer_aliases; Max's REVI overlay compares raw ids itself and must
// resolve them the same way, or the merged row gets a partial overlay.
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const start = SRC.indexOf('function buildCloserAliasMap');
const end = SRC.indexOf('async function getCloserMonthlyScorecard');
if (start < 0 || end < 0) { console.error('FAIL: could not extract alias helpers'); process.exit(1); }
const { buildCloserAliasMap, canonicalCloserId } =
  new Function(`${SRC.slice(start, end)}; return { buildCloserAliasMap, canonicalCloserId };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const map = buildCloserAliasMap([
  { alias_email: 'jose.carranza@neurogrowth.io', canonical_email: 'jose.neurogrowth@gmail.com' },
]);

check('1a alias resolves to canonical', canonicalCloserId(map, 'jose.carranza@neurogrowth.io'), 'jose.neurogrowth@gmail.com');
check('1b case and whitespace insensitive', canonicalCloserId(map, ' Jose.Carranza@NeuroGrowth.io '), 'jose.neurogrowth@gmail.com');
check('1c canonical stays put', canonicalCloserId(map, 'jose.neurogrowth@gmail.com'), 'jose.neurogrowth@gmail.com');
check('1d unrelated closer stays put', canonicalCloserId(map, 'ronny.duarte@neurogrowth.io'), 'ronny.duarte@neurogrowth.io');
check('1e null id is empty, not a throw', canonicalCloserId(map, null), '');
check('2a empty table merges nothing (pre-migration behavior)', canonicalCloserId(buildCloserAliasMap([]), 'jose.carranza@neurogrowth.io'), 'jose.carranza@neurogrowth.io');
check('2b malformed and self rows are dropped',
  buildCloserAliasMap([{ alias_email: null, canonical_email: 'a@x.io' }, { alias_email: 'C@x.io', canonical_email: 'c@x.io' }]), {});

if (failures) { console.error(`\n${failures} check(s) failed.`); process.exit(1); }
console.log('\nAll closer-aliases checks passed.');
