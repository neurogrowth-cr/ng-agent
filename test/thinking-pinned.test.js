// Every Claude call must pin `thinking` off. Sonnet 4.6 ran thinking-off when
// the field was omitted; Sonnet 5 runs ADAPTIVE thinking on the same request and
// spends max_tokens on it. The 2026-09-30 to 10-05 shadow (PR #234) lost every
// Sales EOD Report that way: 4096 tokens of thinking, no text block, empty reply,
// and the cheaper model cost 30% more. A call site that forgets the pin fails
// here, not in production after the next model switch.
//
// Static scan on purpose: index.js cannot be required in a test (it boots Slack),
// and the thing under test is request construction, which is exactly the source.
const fs   = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const SRC  = fs.readFileSync(process.argv[2] || path.join(ROOT, 'index.js'), 'utf8');

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}

console.log('THINKING_OFF definition');
check('THINKING_OFF is the disabled thinking config, defined once',
  (SRC.match(/^const THINKING_OFF = \{ type: 'disabled' \};$/gm) || []).length, 1);

// Every file that can hold a Claude call: index.js plus lib/*.js.
const libDir = path.join(ROOT, 'lib');
const files = [['index.js', SRC]];
if (fs.existsSync(libDir)) {
  for (const f of fs.readdirSync(libDir).filter(f => f.endsWith('.js'))) {
    files.push([path.join('lib', f), fs.readFileSync(path.join(libDir, f), 'utf8')]);
  }
}

console.log('every messages.create call passes thinking: THINKING_OFF');
let total = 0;
for (const [rel, src] of files) {
  const re = /messages\.create\(/g;
  let m;
  while ((m = re.exec(src))) {
    total += 1;
    // The call's argument object ends at the first `});` after the opening
    // paren; none of the request bodies contain one themselves.
    const end  = src.indexOf('});', m.index);
    const call = src.slice(m.index, end === -1 ? m.index + 800 : end);
    const line = src.slice(0, m.index).split('\n').length;
    check(`${rel}:${line} pinned`, call.includes('thinking: THINKING_OFF'), true);
  }
}
check('the scan found the call sites (22 when this test was written; a drop below 20 means the pattern changed, not the code)', total >= 20, true);

console.log('max_tokens is loud');
check('callClaude logs a response that stopped on max_tokens',
  SRC.includes("if (response.stop_reason === 'max_tokens')") && SRC.includes('response stopped on max_tokens'), true);
check('a scheduled report with an empty reply logs instead of returning silently',
  SRC.includes('model returned an empty reply, nothing posted'), true);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
