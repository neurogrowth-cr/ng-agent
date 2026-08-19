// Tests the shared GHL retry wrapper (ghlFetch).  Run:  node test/ghl-retry.test.js
//
// Same extraction trick as the other tests: the block is sliced out of index.js
// and compiled with new Function, so this can never drift from shipped behaviour.
// `fetch` and `setTimeout` are injected as fakes, so the suite is offline and
// instant — the real backoff is 1s/3s/8s.
//
// Guards the 2026-08-17 20:00 UTC incident: the hourly strike sweep ate GHL's
// burst budget, a setter's lead-claim PUT came back 429, and the claim was dropped
// on the floor with no retry.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const GHL_RETRY_DELAYS_MS'),
  SRC.indexOf('async function ghlGetConversationMessages'),
);

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}

// A Response stand-in with just the surface ghlFetch touches.
const resp = (status, retryAfter = null) => ({
  status,
  ok: status >= 200 && status < 300,
  headers: { get: (h) => (h.toLowerCase() === 'retry-after' ? retryAfter : null) },
});

// Builds ghlFetch over a scripted sequence of responses. Returns the function plus
// the recorders, so each case can assert on call count and slept durations.
function build(sequence) {
  const calls = [], sleeps = [];
  const fakeFetch = async (url, init) => {
    calls.push({ url, init });
    return sequence[Math.min(calls.length - 1, sequence.length - 1)];
  };
  const fakeSetTimeout = (fn, ms) => { sleeps.push(ms); fn(); };
  const fakeConsole = { warn() {}, log() {}, error() {} };
  const ghlFetch = new Function('fetch', 'setTimeout', 'console', `${block}; return ghlFetch;`)(
    fakeFetch, fakeSetTimeout, fakeConsole,
  );
  return { ghlFetch, calls, sleeps };
}

const URL = 'https://services.leadconnectorhq.com/contacts/faew9KFlhcL4TC0ECiAq';

(async () => {
  console.log('ghlFetch — transient failures are retried');

  // The actual incident: one 429, then GHL recovers.
  {
    const { ghlFetch, calls, sleeps } = build([resp(429), resp(200)]);
    const res = await ghlFetch(URL, { method: 'PUT' });
    check('429 then 200 → returns the 200', res.status, 200);
    check('429 then 200 → exactly one retry', calls.length, 2);
    check('429 then 200 → first backoff is 1s', sleeps, [1000]);
  }

  // 5xx is transient too — GHL 502s under load.
  {
    const { ghlFetch, calls } = build([resp(503), resp(200)]);
    const res = await ghlFetch(URL, {});
    check('503 is retried', [res.status, calls.length], [200, 2]);
  }

  // Retry-After wins over our own schedule.
  {
    const { ghlFetch, sleeps } = build([resp(429, '2'), resp(200)]);
    await ghlFetch(URL, {});
    check('Retry-After: 2 → sleeps 2s, not the default 1s', sleeps, [2000]);
  }

  // …but can never park a Slack reaction handler for minutes.
  {
    const { ghlFetch, sleeps } = build([resp(429, '600'), resp(200)]);
    await ghlFetch(URL, {});
    check('Retry-After is capped at 30s', sleeps, [30000]);
  }

  // A garbage header must not poison the backoff (NaN → setTimeout fires instantly).
  {
    const { ghlFetch, sleeps } = build([resp(429, 'Wed, 21 Oct 2026 07:28:00 GMT'), resp(200)]);
    await ghlFetch(URL, {});
    check('unparseable Retry-After falls back to the default schedule', sleeps, [1000]);
  }

  console.log('ghlFetch — permanent failures are not retried');

  // A bad token or a deleted contact will never come good; retrying just burns
  // budget and delays the error the caller needs to see.
  {
    const { ghlFetch, calls } = build([resp(404)]);
    const res = await ghlFetch(URL, {});
    check('404 returns immediately', [res.status, calls.length], [404, 1]);
  }
  {
    const { ghlFetch, calls } = build([resp(401)]);
    await ghlFetch(URL, {});
    check('401 is not retried', calls.length, 1);
  }
  {
    const { ghlFetch, calls } = build([resp(200)]);
    await ghlFetch(URL, {});
    check('200 makes exactly one call', calls.length, 1);
  }

  console.log('ghlFetch — the retry budget is bounded');

  // GHL genuinely down: give up and hand the last response back so the caller can
  // build its own error message (the claim handler needs the status code).
  {
    const { ghlFetch, calls, sleeps } = build([resp(429)]);
    const res = await ghlFetch(URL, {});
    check('unrelenting 429 → returns the final 429', res.status, 429);
    check('unrelenting 429 → 1 attempt + 3 retries', calls.length, 4);
    check('unrelenting 429 → full backoff schedule', sleeps, [1000, 3000, 8000]);
  }

  // Callers can opt out — a low-value background read shouldn't wait 12s.
  {
    const { ghlFetch, calls } = build([resp(429)]);
    await ghlFetch(URL, {}, { retries: 1 });
    check('retries option is honoured', calls.length, 2);
  }
  {
    const { ghlFetch, calls } = build([resp(429)]);
    await ghlFetch(URL, {}, { retries: 0 });
    check('retries: 0 disables retrying entirely', calls.length, 1);
  }

  // The wrapper must be transparent — method/headers/body reach fetch untouched.
  {
    const { ghlFetch, calls } = build([resp(429), resp(200)]);
    const init = { method: 'PUT', headers: { Version: '2021-07-28' }, body: '{"assignedTo":"u1"}' };
    await ghlFetch(URL, init, { label: 'PUT /contacts/x (claim)' });
    check('init is passed through unchanged on every attempt',
      calls.map(c => [c.url, c.init.method, c.init.body]),
      [[URL, 'PUT', '{"assignedTo":"u1"}'], [URL, 'PUT', '{"assignedTo":"u1"}']]);
  }

  console.log(failures === 0 ? '\nAll ghl-retry tests passed.' : `\n${failures} test(s) FAILED.`);
  process.exit(failures === 0 ? 0 : 1);
})();
