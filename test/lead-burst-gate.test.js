// Lead burst gate.  Run:  node test/lead-burst-gate.test.js
//
// On 2026-10-03 a WhatsApp reconnect synced 609 phone contacts into GHL and
// Max posted 601 New Lead cards in 14 minutes. createLeadBurstGate (extracted
// from index.js, never copied) must let normal traffic through, close on a
// burst, stay closed while the burst continues, and reopen after a quiet window.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const fnBlock = SRC.match(/function createLeadBurstGate\(\{ limit, windowMs \}\) \{[\s\S]*?\n\}\n/);
if (!fnBlock) { console.log('FAIL  could not extract createLeadBurstGate from index.js'); process.exit(1); }
const createLeadBurstGate = new Function(`${fnBlock[0]}; return createLeadBurstGate;`)();

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });
const MIN = 60 * 1000;

{ // A normal day: a lead every few minutes always gets through.
  const g = createLeadBurstGate({ limit: 8, windowMs: MIN });
  const out = [0, 3, 9, 20, 21, 45].map(m => g.admit(m * MIN));
  check('spread-out leads all admitted', out, [true, true, true, true, true, true]);
}
{ // Exactly the limit inside a minute is still fine (a busy ad hour).
  const g = createLeadBurstGate({ limit: 8, windowMs: MIN });
  const out = Array.from({ length: 8 }, (_, i) => g.admit(i * 5000));
  check('8 in a minute all admitted', out.every(Boolean), true);
}
{ // The 2026-10-03 shape: 609 contacts in ~2 minutes.
  const g = createLeadBurstGate({ limit: 8, windowMs: MIN });
  const out = Array.from({ length: 609 }, (_, i) => g.admit(i * 200));
  check('sync burst: only the first 8 post', out.filter(Boolean).length, 8);
  check('sync burst: 601 held', out.filter(x => !x).length, 601);
}
{ // Stays closed while arrivals keep trickling in under a minute apart,
  // even when the rolling count drops below the limit.
  const g = createLeadBurstGate({ limit: 3, windowMs: MIN });
  for (let i = 0; i < 4; i++) g.admit(i * 1000);          // 4th closes it
  check('trickle 50s later still held', g.admit(50 * 1000), false);
  check('trickle 100s later still held', g.admit(100 * 1000), false);
  check('after a quiet minute it reopens', g.admit(100 * 1000 + MIN + 1), true);
  check('and the next normal lead posts', g.admit(100 * 1000 + 3 * MIN), true);
}

let fail = 0;
for (const c of cases) {
  if (c.ok) console.log(`PASS  ${c.name}`);
  else { fail++; console.log(`FAIL  ${c.name}\n      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - fail}/${cases.length} passed`);
process.exit(fail ? 1 : 0);
