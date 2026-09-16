// Rules test for the one-shot outcome-card drain.
//   Run:  node test/outcome-cards-run-once.test.js
//
// Built 2026-09-16 when Ron wanted Jose's 11 never-sent cards delivered at once
// instead of five per night. A drain that fires on boot is only safe if a
// restart can never repeat it, so these checks are about that guarantee.
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const start = SRC.indexOf('async function runOutcomeCardsOnceIfRequested');
const end = SRC.indexOf("console.log('Outcome cards run-once: done.');", start);
if (start < 0 || end < 0) { console.error('FAIL: could not extract runOutcomeCardsOnceIfRequested'); process.exit(1); }
const fnSrc = SRC.slice(start, SRC.indexOf('}', end) + 1);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

function harness(env, store = {}) {
  const events = [];
  const supabase = {
    from: () => ({ select: () => ({ eq: (_c, key) => ({ limit: async () => ({ data: key in store ? [{ value: store[key] }] : [] }) }) }) }),
  };
  const upsertKnowledge = async (_cat, key, value) => { store[key] = value; events.push(`upsert:${value.split('|')[0]}`); };
  const runUnloggedOutcomeReminders = async (_c, opts) => { events.push('run'); events.opts = opts; };
  const fn = new Function('process', 'supabase', 'upsertKnowledge', 'runUnloggedOutcomeReminders', 'newCorrelationId', 'console',
    `${fnSrc}; return runOutcomeCardsOnceIfRequested;`)(
    { env }, supabase, upsertKnowledge, runUnloggedOutcomeReminders, () => 'cid', { log() {}, error() {} });
  return { fn, events, store };
}

(async () => {
  // 1. Unarmed is a no-op
  let h = harness({});
  await h.fn();
  check('1a no token, nothing runs', h.events, []);

  // 2. Armed: claim BEFORE sending, uncapped, no escalation, scoped
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'jose-2026-09-16', OUTCOME_CARDS_RUN_ONCE_CLOSERS: 'jose.carranza@neurogrowth.io, jose.neurogrowth@gmail.com' });
  await h.fn();
  check('2a claim is written before any card is sent', h.events.slice(0, 2), ['upsert:claimed', 'run']);
  check('2b marked done afterwards', h.events[2], 'upsert:done');
  check('2c uncapped', h.events.opts.perCloserCap, Infinity);
  check('2d escalation left to the nightly run', h.events.opts.skipEscalation, true);
  check('2e scoped to the named closers', h.events.opts.onlyClosers, ['jose.carranza@neurogrowth.io', 'jose.neurogrowth@gmail.com']);

  // 3. THE safety property: a restart with the same token never re-sends
  const store = { 'outcome-cards-run-once:jose-2026-09-16': 'claimed|2026-09-16T20:40:00Z' };
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'jose-2026-09-16' }, store);
  await h.fn();
  check('3a a claimed token (even mid-run) never sends again', h.events.includes('run'), false);

  // 4. No closer list means everyone
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'all-1' });
  await h.fn();
  check('4a empty closer list drains all closers', h.events.opts.onlyClosers, null);

  if (failures) { console.error(`\n${failures} check(s) failed.`); process.exit(1); }
  console.log('\nAll outcome-cards-run-once checks passed.');
})();
