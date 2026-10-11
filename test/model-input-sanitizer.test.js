// Lone UTF-16 surrogates must never reach the Claude API. An emoji is a
// surrogate pair (two JS "chars"); any `.slice(0, n)` can cut between them and
// the API rejects the whole request as invalid JSON ("no low surrogate in
// string"). Regression under test (2026-09-25): the Friday Delivery Wrap-Up
// failed all 3 attempts that way and the report never posted.
//
// Run:  node test/model-input-sanitizer.test.js
//
// Same extraction trick as the other tests: the sanitizer block and capText are
// sliced out of index.js and compiled with new Function, so this can never
// drift from shipped behaviour.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const sanitizerBlock = SRC.slice(
  SRC.indexOf('// ─── MODEL INPUT SANITIZER'),
  SRC.indexOf('// ─── end model input sanitizer'),
);
const capStart = SRC.indexOf('function capText(');
const capBlock = SRC.slice(capStart, SRC.indexOf('\n}\n', capStart) + 3);
const g = new Function(`${sanitizerBlock}\n${capBlock}; return { LONE_SURROGATE_RE, stripLoneSurrogates, clipText, sanitizeModelInput, capText };`)();

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}
// "Valid" the way the API sees it: JSON round-trips unchanged and
// encodeURIComponent (which throws URIError on a lone surrogate) accepts it.
function isWellFormed(s) {
  try { encodeURIComponent(s); } catch { return false; }
  return JSON.parse(JSON.stringify(s)) === s && !new RegExp(g.LONE_SURROGATE_RE.source).test(s);
}

console.log('the raw cut really is the bug');
const raw = 'hola 😀'.slice(0, 6);           // ends on the high half of 😀
check('a character-count slice leaves a lone high surrogate', raw.charCodeAt(5), 0xD83D);
check('that string is rejected by encodeURIComponent', isWellFormed(raw), false);

console.log('clipText: surrogate-safe cut');
const clipped = g.clipText('hola 😀', 6);
check('split emoji is dropped, not half-sent', clipped, 'hola ');
check('result is well-formed', isWellFormed(clipped), true);
check('an intact pair at the boundary survives', g.clipText('😀😀', 4), '😀😀');
check('cutting inside the second pair keeps only the first', g.clipText('😀😀', 3), '😀');
check('short strings pass through untouched', g.clipText('ok ✅ 👍', 50), 'ok ✅ 👍');
check('null reads as empty', g.clipText(null, 5), '');
check('undefined reads as empty', g.clipText(undefined, 5), '');
check('numbers are stringified', g.clipText(12345, 3), '123');

console.log('stripLoneSurrogates');
check('lone low surrogate mid-string is removed', g.stripLoneSurrogates('a\uDE00b'), 'ab');
check('lone high surrogate mid-string is removed', g.stripLoneSurrogates('a\uD83Db'), 'ab');
check('valid pairs are untouched', g.stripLoneSurrogates('🇨🇷 ok 👩‍💻'), '🇨🇷 ok 👩‍💻');
check('non-strings pass through', g.stripLoneSurrogates(42), 42);

console.log('sanitizeModelInput: the one place every request goes through');
const messages = [
  { role: 'user', content: 'report please \uD83D' },
  { role: 'assistant', content: [{ type: 'tool_use', id: 't1', name: 'read_slack_channel', input: { channelName: 'x', messageCount: 50 } }] },
  { role: 'user', content: [{ type: 'tool_result', tool_use_id: 't1', content: '[Sep 25] gran semana \uD83C' }] },
];
const system = [{ type: 'text', text: 'static \uDE00', cache_control: { type: 'ephemeral', ttl: '1h' } }];
const before = JSON.stringify(messages);
const cleanMsgs = g.sanitizeModelInput(messages);
const cleanSys  = g.sanitizeModelInput(system);
check('tool_result content is cleaned', cleanMsgs[2].content[0].content, '[Sep 25] gran semana ');
check('plain user text is cleaned', cleanMsgs[0].content, 'report please ');
check('system block text is cleaned', cleanSys[0].text, 'static ');
check('cache_control and non-string fields survive the walk', cleanSys[0].cache_control, { type: 'ephemeral', ttl: '1h' });
check('tool_use input (numbers, ids) survives the walk', cleanMsgs[1].content[0], { type: 'tool_use', id: 't1', name: 'read_slack_channel', input: { channelName: 'x', messageCount: 50 } });
check('the input is not mutated', JSON.stringify(messages), before);
check('the whole request body is well-formed', isWellFormed(JSON.stringify({ system: cleanSys, messages: cleanMsgs })), true);

console.log('capText: head and tail cuts are both safe');
const head = 'a'.repeat(5999) + '😀';            // the 6000-char head cut lands inside 😀
const tail = '😀' + 'b'.repeat(1999);            // the 2000-char tail cut lands inside 😀
const capped = g.capText(head + 'x'.repeat(500) + tail, 8000);
check('capped output is well-formed', isWellFormed(capped), true);
check('the elision marker is present', /chars elided/.test(capped), true);
check('under the cap nothing changes', g.capText('short 😀', 8000), 'short 😀');

console.log('the client wrapper is wired');
check('anthropic.messages.create is wrapped once', (SRC.match(/anthropic\.messages\.create = /g) || []).length, 1);
check('the wrapper sanitizes system and messages',
  SRC.includes("clean.system = sanitizeModelInput(clean.system)") && SRC.includes("clean.messages = sanitizeModelInput(clean.messages)"), true);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
