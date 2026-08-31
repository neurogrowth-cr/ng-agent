// The pattern-reflection guardrails: the deterministic code that makes "full
// autonomy, no dry run" safe. Run:  node test/pattern-reflection.test.js
//
// Ron authorized Max to act on recurring patterns without approval
// (2026-08-31). The safety story is NOT the prompt — it is this decision
// table: evidence parsed and counted (never trusted), a hard nightly action
// cap, a dedup window per pattern, and a whitelisted action vocabulary.
// These tests pin every guardrail; the mutations in the PR prove each one
// actually fires when removed.
//
// Same extraction trick as the other tests: the pure block is sliced out of
// index.js and compiled with new Function, so this cannot drift from shipped
// behaviour. slugifyReportName is sliced from its own block and injected.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const slugBlock = SRC.slice(
  SRC.indexOf('function slugifyReportName'),
  SRC.indexOf('function reportPostKey'),
);
const block = SRC.slice(
  SRC.indexOf('const PATTERN_MIN_DISTINCT_DATES'),
  SRC.indexOf('// ─── end pattern reflection guardrails'),
);
const g = new Function(`${slugBlock}\n${block}; return { parsePatternLine, decidePatternActions, PATTERN_MIN_DISTINCT_DATES, PATTERN_MAX_ACTIONS_PER_NIGHT, PATTERN_DEDUP_DAYS };`)();

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}

const NOW = Date.parse('2026-08-31T12:00:00Z');
const DAY = 86400000;
// A well-formed actionable pattern line, evidence on three distinct days.
const line = (slug, dates, action, payload) => `PATTERN | ${slug} | ${dates} | ${action} | ${payload}`;
const GOOD = line('incomplete-activations', '2026-08-25, 2026-08-28, 2026-08-30', 'notion_task', 'Audit activation checklist :: Three clients stuck.');

console.log('parsePatternLine — the protocol');
{
  const p = g.parsePatternLine(GOOD);
  check('slug parsed and slugified', p.slug, 'incomplete-activations');
  check('three distinct dates', p.dates.length, 3);
  check('action lowercased', p.action, 'notion_task');
  check('payload intact', p.payload, 'Audit activation checklist :: Three clients stuck.');
}
check('payload may contain pipes',
  g.parsePatternLine(line('x', '2026-08-01, 2026-08-02, 2026-08-03', 'notion_task', 'a | b | c')).payload, 'a | b | c');
check('EVIDENCE IS COUNTED, NOT TRUSTED — one date three times is ONE day',
  g.parsePatternLine(line('x', '2026-08-30, 2026-08-30, 2026-08-30', 'notion_task', 'p')).dates.length, 1);
check('dates extracted from prose too',
  g.parsePatternLine(line('x', 'seen 2026-08-01 and again 2026-08-05', 'monitor', 'p')).dates, ['2026-08-01', '2026-08-05']);
check('slug is normalized like task names', g.parsePatternLine(line('Incomplete Activations!', '2026-08-01', 'none', 'p')).slug, 'incomplete-activations');
check('non-PATTERN line → null', g.parsePatternLine('NO_PATTERNS'), null);
check('prose line → null', g.parsePatternLine('Here are the patterns I found:'), null);
check('too few fields → null', g.parsePatternLine('PATTERN | slug | dates'), null);
check('empty slug → null', g.parsePatternLine(line('!!!', '2026-08-01', 'monitor', 'p')), null);
check('null input does not throw', g.parsePatternLine(null), null);

console.log('\ndecidePatternActions — the guardrails');
const parse = (...lines) => lines.map(g.parsePatternLine);
{
  const d = g.decidePatternActions(parse(GOOD), {}, NOW);
  check('a clean pattern executes', [d.execute.length, d.dropped.length], [1, 0]);
}
{
  const d = g.decidePatternActions(parse(line('weak', '2026-08-30, 2026-08-29', 'notion_task', 'p')), {}, NOW);
  check('two days of evidence is NOT enough', d.execute.length, 0);
  check('and the drop says why', d.dropped[0]?.reason, 'insufficient_evidence');
}
{
  const d = g.decidePatternActions(parse(line('fake', '2026-08-30, 2026-08-30, 2026-08-30', 'notion_task', 'p')), {}, NOW);
  check('a fabricated triple of the same date is rejected', d.dropped[0]?.reason, 'insufficient_evidence');
}
{
  const three = parse(
    line('p1', '2026-08-01, 2026-08-02, 2026-08-03', 'notion_task', 'a'),
    line('p2', '2026-08-01, 2026-08-02, 2026-08-03', 'notion_task', 'b'),
    line('p3', '2026-08-01, 2026-08-02, 2026-08-03', 'notion_task', 'c'),
  );
  const d = g.decidePatternActions(three, {}, NOW);
  check('THE CAP — third action is dropped', [d.execute.length, d.dropped.length], [2, 1]);
  check('capped drop says so', d.dropped[0]?.reason, 'nightly_cap');
  check('cap counts only executes — monitors do not consume it',
    g.decidePatternActions(parse(
      line('m1', '2026-08-01, 2026-08-02, 2026-08-03', 'monitor', 'a'),
      line('m2', '2026-08-01, 2026-08-02, 2026-08-03', 'monitor', 'b'),
      line('p1', '2026-08-01, 2026-08-02, 2026-08-03', 'notion_task', 'c'),
      line('p2', '2026-08-01, 2026-08-02, 2026-08-03', 'notion_task', 'd'),
    ), {}, NOW).execute.length, 2);
}
{
  const tracker = { 'incomplete-activations': { lastActioned: NOW - 5 * DAY } };
  const d = g.decidePatternActions(parse(GOOD), tracker, NOW);
  check('THE DEDUP — actioned 5 days ago is not re-actioned', d.dropped[0]?.reason, 'recently_actioned');
  const stale = { 'incomplete-activations': { lastActioned: NOW - 15 * DAY } };
  check('but 15 days ago is fair game again',
    g.decidePatternActions(parse(GOOD), stale, NOW).execute.length, 1);
  check('dedup boundary is exclusive at exactly 14 days',
    g.decidePatternActions(parse(GOOD), { 'incomplete-activations': { lastActioned: NOW - g.PATTERN_DEDUP_DAYS * DAY } }, NOW).execute.length, 1);
}
{
  const d = g.decidePatternActions(parse(line('rogue', '2026-08-01, 2026-08-02, 2026-08-03', 'send_email', 'p')), {}, NOW);
  check('THE WHITELIST — an action outside the vocabulary is dropped', d.dropped[0]?.reason, 'unknown_action');
  check('and never executed', d.execute.length, 0);
}
{
  const d = g.decidePatternActions(parse(
    line('resolved', '2026-08-01, 2026-08-02, 2026-08-03', 'none', 'gone'),
    line('watch', '2026-08-01, 2026-08-02, 2026-08-03', 'monitor', 'w'),
  ), {}, NOW);
  check('none is a no-op, monitor is tracked', [d.execute.length, d.monitor.length, d.dropped.length], [0, 1, 0]);
  check('monitor still requires real evidence',
    g.decidePatternActions(parse(line('vibe', '2026-08-30', 'monitor', 'w')), {}, NOW).dropped[0].reason, 'insufficient_evidence');
}
check('empty and null inputs do not throw',
  [g.decidePatternActions([], {}, NOW).execute.length, g.decidePatternActions(null, {}, NOW).execute.length], [0, 0]);
check('nulls from failed parses are skipped',
  g.decidePatternActions([null, g.parsePatternLine(GOOD), null], {}, NOW).execute.length, 1);

console.log('\nconstants are what Ron signed off on');
check('minimum evidence: 3 distinct days', g.PATTERN_MIN_DISTINCT_DATES, 3);
check('nightly action cap: 2', g.PATTERN_MAX_ACTIONS_PER_NIGHT, 2);
check('dedup window: 14 days', g.PATTERN_DEDUP_DAYS, 14);

console.log('\nwiring (source-level)');
check('reflection runs inside the nightly cycle',
  /await runPatternReflection\(correlationId, savedEntries\)/.test(SRC), true);
check('a reflection failure is isolated from the learning cycle',
  /Pattern reflection failed[\s\S]{0,400}learning itself succeeded/.test(SRC), true);
check('confidential rows are excluded at the QUERY, not by prompt',
  (SRC.match(/eq\('visibility', 'shared'\)[\s\S]{0,80}gte\('updated_at'/g) || []).length >= 2, true);
check('auto-actions write an audit activity row',
  /action: 'pattern_auto_action'/.test(SRC), true);
check('Ron is informed of every action taken',
  /Pattern reflection — autonomous actions tonight/.test(SRC), true);
check('the cap being hit is surfaced, not swallowed',
  /hit the \$\{PATTERN_MAX_ACTIONS_PER_NIGHT\}\/night cap/.test(SRC), true);

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All pattern-reflection tests passed.');
