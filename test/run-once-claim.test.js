// Rules test for the boot-time run-once claim shared by OUTCOME_CARDS_RUN_ONCE
// and CALL_FIELDS_BACKFILL_RUN_ONCE.
//   Run:  node test/run-once-claim.test.js
//
// Built 2026-10-11 after two Railway deployments booted a second apart (two
// env-var upserts in a row). The first claimed the backfill token and was
// SIGTERM'd 5 seconds later by the second, which read the claim and skipped.
// The backfill never ran and Ron had to set a new token. The claim is now
// `started|<iso>`, only `done|...` counts as used, and a started claim older
// than 15 minutes with no done is retried on the next boot.
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
function slice(from, to, label) {
  const a = SRC.indexOf(from);
  const b = SRC.indexOf(to, a);
  if (a < 0 || b < 0) { console.error(`FAIL: could not extract ${label}`); process.exit(1); }
  return SRC.slice(a, b);
}
const helperSrc = slice('const RUN_ONCE_STALE_MS', 'async function upsertKnowledge', 'claimRunOnce');
const backfillSrc = slice('async function runCallFieldsBackfillOnceIfRequested', '\nslack.action(callFields.OPEN_ACTION_ID', 'runCallFieldsBackfillOnceIfRequested');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MIN = 60 * 1000;
const now = Date.parse('2026-10-11T18:00:00Z');
const iso = (msAgo) => new Date(now - msAgo).toISOString();

// --- The state machine on its own -----------------------------------------
const { runOnceClaimState, RUN_ONCE_STALE_MS } = new Function('supabase', 'upsertKnowledge', 'console',
  `${helperSrc}; return { runOnceClaimState, RUN_ONCE_STALE_MS };`)({}, async () => {}, console);

check('stale window is 15 minutes', RUN_ONCE_STALE_MS, 15 * MIN);
check('no row = free', runOnceClaimState('', now), 'free');
check('started 5s ago = running (the other boot owns it)', runOnceClaimState(`started|${iso(5000)}`, now), 'running');
check('started 14m59s ago = still running', runOnceClaimState(`started|${iso(15 * MIN - 1000)}`, now), 'running');
check('started 15m ago, no done = stale', runOnceClaimState(`started|${iso(15 * MIN)}`, now), 'stale');
check('started yesterday, no done = stale', runOnceClaimState(`started|${iso(24 * 60 * MIN)}`, now), 'stale');
check('done = done regardless of age', runOnceClaimState('done|2026-09-16T20:41:00Z|3/7', now), 'done');
check('done written a second ago = done', runOnceClaimState(`done|${iso(1000)}`, now), 'done');
check('legacy claimed|<old iso> (pre-fix, finished) = stale and retried, which the pre-fix row never allowed', runOnceClaimState('claimed|2026-09-16T20:40:00Z', now), 'stale');
check('started with a garbage timestamp = stale, not stuck forever', runOnceClaimState('started|not-a-date', now), 'stale');
check('unknown non-empty value = done (never re-run on a value we do not understand)', runOnceClaimState('whatever', now), 'done');

// --- The backfill job end to end against a fake store ---------------------
function harness(env, store = {}, { readError = null } = {}) {
  const events = [];
  const supabase = {
    from: () => ({ select: () => ({ eq: (_c, key) => ({ limit: async () => ({ data: key in store ? [{ value: store[key] }] : [] }) }) }) }),
  };
  const portalSupabase = {
    from: () => ({ select: () => ({ eq: async () => readError ? { error: { message: readError } } : { data: [
      { appointment_id: 'a1', appt: { scheduled_start: new Date(Date.now() - 86400000).toISOString(), prospect: { full_name: 'Ana' } } },
    ] } }) }),
  };
  const upsertKnowledge = async (_cat, key, value) => { store[key] = value; events.push(`upsert:${value.split('|')[0]}`); };
  const autofillCallFieldsFromRevi = async () => { events.push('autofill'); return { ok: true, fields: { objection: 'price' }, out: { ghl: { ok: true } } }; };
  const slack = { client: { chat: { postMessage: async () => { events.push('dm'); } } } };
  const fn = new Function('process', 'supabase', 'portalSupabase', 'upsertKnowledge', 'autofillCallFieldsFromRevi', 'slack', 'RON_SLACK_ID', 'console',
    `${helperSrc}${backfillSrc}; return runCallFieldsBackfillOnceIfRequested;`)(
    { env }, supabase, portalSupabase, upsertKnowledge, autofillCallFieldsFromRevi, slack, 'U_RON', { log() {}, error() {}, warn() {} });
  return { fn, events, store };
}

(async () => {
  const KEY = 'call-fields-backfill:t1';

  let h = harness({});
  await h.fn();
  check('b1 unarmed boot does nothing', h.events, []);

  h = harness({ CALL_FIELDS_BACKFILL_RUN_ONCE: 't1' });
  await h.fn();
  check('b2 fresh token: started before the work, done after', h.events, ['upsert:started', 'autofill', 'upsert:done', 'dm']);
  check('b2 done value carries the tally', h.store[KEY].startsWith('done|') && h.store[KEY].endsWith('|1/1'), true);

  // The 2026-10-11 incident, second deploy's point of view: first boot wrote
  // started 1s ago and is about to be SIGTERM'd. This boot must NOT run twice
  // in parallel with it...
  h = harness({ CALL_FIELDS_BACKFILL_RUN_ONCE: 't1' }, { [KEY]: `started|${new Date(Date.now() - 1000).toISOString()}` });
  await h.fn();
  check('b3 a token started a second ago is left to the boot that started it', h.events, []);

  // ...but the NEXT boot, 15+ minutes later with the claim still at started,
  // retries instead of calling the token used.
  const store = { [KEY]: `started|${new Date(Date.now() - 16 * MIN).toISOString()}` };
  h = harness({ CALL_FIELDS_BACKFILL_RUN_ONCE: 't1' }, store);
  await h.fn();
  check('b4 a started claim older than 15 min with no done is retried', h.events, ['upsert:started', 'autofill', 'upsert:done', 'dm']);
  check('b4 retry finishes the token', store[KEY].startsWith('done|'), true);

  h = harness({ CALL_FIELDS_BACKFILL_RUN_ONCE: 't1' }, { [KEY]: 'done|2026-10-11T17:00:00Z|3/7' });
  await h.fn();
  check('b5 a finished token is never re-run', h.events, []);

  // A read failure leaves the claim at started, so the next boot after the
  // window retries instead of burning the token (the pre-fix code lost it).
  const failStore = {};
  h = harness({ CALL_FIELDS_BACKFILL_RUN_ONCE: 't1' }, failStore, { readError: 'boom' });
  await h.fn();
  check('b6 a failed run stops after the claim', h.events, ['upsert:started']);
  check('b6 and leaves the claim at started for a later retry', failStore[KEY].startsWith('started|'), true);

  if (failures) { console.error(`\n${failures} check(s) failed.`); process.exit(1); }
  console.log('\nAll run-once-claim checks passed.');
})();
