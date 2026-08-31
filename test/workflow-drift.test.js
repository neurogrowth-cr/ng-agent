// Tests the GHL workflow drift watchdog.  Run:  node test/workflow-drift.test.js
//
// Same extraction trick as the other suites: the evaluator and renderer are
// sliced out of index.js and compiled with new Function, so this can never
// drift from shipped behaviour. Both are pure, so this suite needs no stubs and
// makes no network or database calls.
//
// What it is really guarding: a watchdog whose job is to notice breakage must
// not itself invent breakage. The two ways this one could go wrong are
// (a) alerting on ordinary activity until someone mutes it, and (b) reading
// zero workflows and reporting that as mass deletion. Both are pinned below.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const START = '// ─── GHL WORKFLOW DRIFT WATCHDOG ─';
const END   = 'async function fetchGhlWorkflows';
if (SRC.indexOf(START) === -1 || SRC.indexOf(END) === -1) {
  console.error('FAIL  could not extract the workflow drift block — did the anchors change?');
  process.exit(1);
}
const block = SRC.slice(SRC.indexOf(START), SRC.indexOf(END));

const { evaluateWorkflowDrift, renderWorkflowDriftAlert } =
  new Function(`${block}; return { evaluateWorkflowDrift, renderWorkflowDriftAlert };`)();

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual), e = JSON.stringify(expected);
  if (a === e) console.log(`  ok  ${name}`);
  else { failures++; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}

const wf = (id, status, version = 1, updatedAt = '2026-08-01T00:00:00Z') =>
  ({ id, name: `WF ${id}`, status, version, updatedAt });

// ── The alertable findings ────────────────────────────────────────────────
{
  const previous = [wf('a', 'published'), wf('b', 'published')];
  const current  = [wf('a', 'published'), wf('b', 'draft')];
  const d = evaluateWorkflowDrift({ current, previous });
  check('published→draft is alertable', d.alertable, 1);
  check('  and names the workflow', d.unpublished[0].name, 'WF b');
  check('  and records both sides of the flip', [d.unpublished[0].from, d.unpublished[0].to], ['published', 'draft']);
}
{
  const previous = [wf('a', 'published'), wf('b', 'published')];
  const current  = [wf('a', 'published')];
  const d = evaluateWorkflowDrift({ current, previous });
  check('a disappeared workflow is alertable', d.alertable, 1);
  check('  and is reported as disappeared, not unpublished', [d.disappeared.length, d.unpublished.length], [1, 0]);
}

// ── The NON-alertable findings — this is the anti-noise contract ──────────
{
  const previous = [wf('a', 'published', 1)];
  const current  = [wf('a', 'published', 2, '2026-08-31T00:00:00Z')];
  const d = evaluateWorkflowDrift({ current, previous });
  check('a version bump is recorded but NOT alertable', [d.edited.length, d.alertable], [1, 0]);
}
{
  const previous = [wf('a', 'published')];
  const current  = [wf('a', 'published'), wf('new', 'draft')];
  const d = evaluateWorkflowDrift({ current, previous });
  check('a new workflow is recorded but NOT alertable', [d.appeared.length, d.alertable], [1, 0]);
}
{
  // A draft that stays a draft is somebody's work in progress, not breakage.
  const previous = [wf('a', 'draft')];
  const current  = [wf('a', 'draft')];
  check('draft→draft is not a flip', evaluateWorkflowDrift({ current, previous }).alertable, 0);
}
{
  // Re-publishing is a fix, not a fault.
  const previous = [wf('a', 'draft')];
  const current  = [wf('a', 'published')];
  check('draft→published is not alertable', evaluateWorkflowDrift({ current, previous }).alertable, 0);
}
{
  const previous = [wf('a', 'published'), wf('b', 'published')];
  const d = evaluateWorkflowDrift({ current: previous, previous });
  check('an unchanged fleet reports nothing', d.alertable, 0);
  check('  and reports no edits or appearances either', [d.edited.length, d.appeared.length], [0, 0]);
}

// ── Warmup and the mass-deletion trap ─────────────────────────────────────
{
  const d = evaluateWorkflowDrift({ current: [wf('a', 'published')], previous: null });
  check('no prior snapshot reports baseline, not drift', [d.baseline, d.alertable], [true, 0]);
  check('  and does not report the whole fleet as new', d.appeared.length, 0);
}
{
  // The dangerous case. An empty read must never render as "everything was
  // deleted" — the caller treats it as a failed read before reaching here, and
  // this pins that the evaluator would not manufacture 22 findings from it.
  const previous = [wf('a', 'published'), wf('b', 'published')];
  const d = evaluateWorkflowDrift({ current: [], previous });
  check('empty current DOES surface as disappearances at evaluator level', d.disappeared.length, 2);
  // ...which is exactly why runGhlWorkflowDriftCheck refuses to call this on an
  // empty read. Assert that guard exists in the shipped source, since the
  // evaluator alone cannot enforce it.
  const guard = SRC.includes('if (!current.length)') && SRC.includes('not as mass deletion');
  check('  and the caller guards against ever evaluating an empty read', guard, true);
}

// ── Structural contract on the rendered alert (criteria 4a) ───────────────
{
  const previous = [wf('a', 'published'), wf('b', 'published')];
  const current  = [wf('a', 'draft')];
  const d = evaluateWorkflowDrift({ current, previous });
  const text = renderWorkflowDriftAlert(d, { count: 1, since: '2026-08-30' });
  check('alert carries the verdict line', text.includes('WORKFLOW DRIFT'), true);
  check('alert names the unpublished workflow', text.includes('WF a'), true);
  check('alert names the disappeared workflow', text.includes('WF b'), true);
  check('alert states the comparison date', text.includes('2026-08-30'), true);
  check('alert says a human has to fix it in GHL', text.includes('GHL UI'), true);
  for (const bad of ['undefined', 'null', '[object Object]', 'NaN']) {
    check(`alert contains no "${bad}"`, text.includes(bad), false);
  }
}

// ── Meta: the recipe standard's non-negotiables, asserted on the source ───
{
  check('the watchdog declares itself in STATIC_CRON_SCHEDULES',
    /runGhlWorkflowDriftCheck:\s*'45 7 \* \* \*'/.test(SRC), true);
  check('it never self-terminates', /workflow drift[\s\S]{0,4000}?disable yourself/i.test(SRC), false);
  check('a broken read says explicitly that it is not an all-clear',
    SRC.includes('this is not an all-clear'), true);
  check('the dedup key is content-derived, not date-only',
    SRC.includes('ghl-wf-drift:') && SRC.includes('createHash'), true);
}

console.log(failures ? `\n${failures} failure(s)` : '\nAll workflow drift tests passed.');
process.exit(failures ? 1 : 0);
