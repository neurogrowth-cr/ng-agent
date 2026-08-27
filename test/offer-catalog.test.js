// The catalog of record, machine-checked against Max's system prompt. Run:
//   node test/offer-catalog.test.js
//
// The North Star says retired offers "must not reappear anywhere". Max briefs
// the team daily and closers borrow his language, so if a dead SKU or a dead
// price survives in this prompt it goes straight into customer-facing talk.
// Prose review missed exactly that: the prompt still defined OMEGA and the old
// 6-month DWY/DFY ROLEX and PATEK on 2026-08-26, thirteen days after the
// catalog was locked and the build brief marked the update due.
//
// Source of truth: roi-rm-okr-reporting docs/plan-of-record.md §2. When the
// catalog changes there, this test is the thing that fails until the prompt
// follows.
//
// Reads the system-prompt constants as source text rather than requiring
// index.js, which boots the Slack app on require.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
// Every SYSTEM_PROMPT_* constant, start of the first to the end of the last.
const PROMPT = SRC.slice(
  SRC.indexOf('const SYSTEM_PROMPT_BASE'),
  SRC.indexOf("const RON_SLACK_ID = 'U05HXGX18H3';"),
);

// The RETIRED block names the dead SKUs and dead prices on purpose — Max has to
// recognise them to refuse them. So "must not reappear" is asked of everything
// OUTSIDE that block: a dead price is a guardrail there and a live quote
// anywhere else. Losing the block itself is checked separately below.
const RETIRED_START = 'RETIRED — never quote these';
const RETIRED_END   = 'Core promise:';
const retiredBlock = PROMPT.slice(PROMPT.indexOf(RETIRED_START), PROMPT.indexOf(RETIRED_END));
const LIVE = PROMPT.replace(retiredBlock, '');

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}
const has = (s) => PROMPT.includes(s);
const hasRe = (re) => re.test(PROMPT);
// Asked of the prompt minus the RETIRED block.
const liveHas   = (s) => LIVE.includes(s);
const liveHasRe = (re) => re.test(LIVE);

check('the prompt block was actually located (guards the slice markers)',
  PROMPT.length > 5000, true);

console.log('the three rungs and their prices are stated');
check('Build & Release at $4,997', has('$4,997'), true);
check('ROLEX at $475/mo', has('$475/mo'), true);
check('PATEK at $1,297/mo', has('$1,297/mo'), true);
check('the embedded 90 days are named', has('90 days of ROLEX'), true);
check('100% upfront extends to day 120', has('day 120'), true);
check('continuation at day 91 is automatic unless cancelled',
  hasRe(/day[- ]91[\s\S]{0,120}unless the client cancels/), true);
check('PATEK carries its three-month minimum', hasRe(/[Tt]hree-month minimum/), true);
check('CAPI is gated on a GHL sub-account', hasRe(/CAPI[\s\S]{0,200}GHL sub-account/), true);

console.log('\nthe RETIRED guardrail block itself exists');
check('the block is present', retiredBlock.length > 200, true);
check('it names OMEGA so Max can refuse it', retiredBlock.includes('OMEGA'), true);
check('it names the founding prices so Max can refuse them',
  retiredBlock.includes('$325') && retiredBlock.includes('$975'), true);
check('it kills the seat caps', /slots/.test(retiredBlock), true);

console.log('\nretired offers must not reappear OUTSIDE that block (killed 2026-08-05)');
check('OMEGA is not offered anywhere', /\bOMEGA\b/.test(LIVE), false);
for (const dead of ['3-month community', '6-month Done-With-You', '6-month Done-For-You']) {
  check(`"${dead}" is gone`, liveHas(dead), false);
}
check('Full Service is not framed as a winding-down product line',
  liveHasRe(/Full[- ]service SDR management is no longer offered/), false);
check("Josue's role no longer allocates 40% to Full Service",
  hasRe(/40% Full Service/), false);
check('no DFY portfolio language left in the role blocks',
  hasRe(/DFY portfolio/), false);

console.log('\ndead prices and dead scarcity are never quoted as live');
check('founding ROLEX price $325 is not quoted', liveHasRe(/\$325\b/), false);
check('founding PATEK price $975 is not quoted', liveHasRe(/\$975\b/), false);
check('no seat count is quoted as available',
  liveHasRe(/15 slots|8 slots|15 ROLEX slots/), false);

console.log('\nthe US price card does not exist and must not be invented');
check('the prompt says so explicitly', has('THERE IS NO US PRICE CARD'), true);
check('and forbids estimating one', hasRe(/[Nn]ever estimate it/), true);
// The placeholder US numbers live in plan-of-record §4 marked [confirm]. If any
// of them ever appear here, someone has copied an unconfirmed number into the
// thing that talks to the team every morning.
for (const notYet of ['$997', '$2,497', '$8,000', '$10,000']) {
  check(`unconfirmed US placeholder ${notYet} is not quoted`, has(notYet), false);
}

console.log('\nKai is internal-only in customer-facing copy');
check('the prompt says what a client hears instead',
  has('automated outreach replies in your voice'), true);
check('and flags Kai as the internal name', hasRe(/"Kai"[\s\S]{0,120}internal name/), true);

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All offer-catalog tests passed.');
