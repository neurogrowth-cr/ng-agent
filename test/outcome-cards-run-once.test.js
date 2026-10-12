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
const cStart = SRC.indexOf('const RUN_ONCE_STALE_MS');
const cEnd = SRC.indexOf('async function upsertKnowledge', cStart);
if (start < 0 || end < 0 || cStart < 0 || cEnd < 0) { console.error('FAIL: could not extract runOutcomeCardsOnceIfRequested / claimRunOnce'); process.exit(1); }
const fnSrc = SRC.slice(cStart, cEnd) + SRC.slice(start, SRC.indexOf('}', end) + 1);

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
    { env }, supabase, upsertKnowledge, runUnloggedOutcomeReminders, () => 'cid', { log() {}, error() {}, warn() {} });
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
  check('2a claim is written before any card is sent', h.events.slice(0, 2), ['upsert:started', 'run']);
  check('2b marked done afterwards', h.events[2], 'upsert:done');
  check('2c uncapped', h.events.opts.perCloserCap, Infinity);
  check('2d escalation left to the nightly run', h.events.opts.skipEscalation, true);
  check('2e scoped to the named closers', h.events.opts.onlyClosers, ['jose.carranza@neurogrowth.io', 'jose.neurogrowth@gmail.com']);

  // 3. THE safety property: a restart with the same token never re-sends
  //    while the first boot may still be sending, and never after it finished.
  const fresh = new Date(Date.now() - 5000).toISOString();
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'jose-2026-09-16' }, { 'outcome-cards-run-once:jose-2026-09-16': `started|${fresh}` });
  await h.fn();
  check('3a a token started seconds ago (another boot mid-run) never sends again', h.events, []);
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'jose-2026-09-16' }, { 'outcome-cards-run-once:jose-2026-09-16': 'done|2026-09-16T20:41:00Z' });
  await h.fn();
  check('3b a finished token never sends again', h.events, []);
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'jose-2026-09-16' }, { 'outcome-cards-run-once:jose-2026-09-16': 'claimed|2026-09-16T20:40:00Z' });
  await h.fn();
  check('3c a legacy claimed|<iso> that never reached done is a burned token: retried', h.events, ['upsert:started', 'run', 'upsert:done']);
  // 2026-10-11: the first deploy claimed and was SIGTERM'd by the second a
  // second later; the second skipped and the token was burned. A started claim
  // older than 15 minutes with no done is retried.
  const stale = new Date(Date.now() - 16 * 60 * 1000).toISOString();
  const store = { 'outcome-cards-run-once:burned': `started|${stale}` };
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'burned' }, store);
  await h.fn();
  check('3d a started claim older than 15 min with no done is retried', h.events, ['upsert:started', 'run', 'upsert:done']);
  check('3e the retry leaves the token finished', store['outcome-cards-run-once:burned'].startsWith('done|'), true);

  // 4. No closer list means everyone
  h = harness({ OUTCOME_CARDS_RUN_ONCE: 'all-1' });
  await h.fn();
  check('4a empty closer list drains all closers', h.events.opts.onlyClosers, null);

  if (failures) { console.error(`\n${failures} check(s) failed.`); process.exit(1); }
  console.log('\nAll outcome-cards-run-once checks passed.');
})();
