// Rules test for the wrapCronJob watchdog.  Run:  node test/cron-watchdog.test.js
//
// Extracts the real wrapCronJob out of index.js rather than copying it, so the test
// cannot drift from shipped behaviour (same approach as make-watchdog and
// cron-liveness — index.js boots the Slack app on require).
//
// The case this exists for: on 2026-08-18 the Fulfillment EOD Pulse wrote a
// `started` row at 00:00:01 and never a terminal one. No deploy restarted the
// process for 22 hours, so it was not killed — it hung. A hung cron is invisible:
// not a failure, not a success, just absence.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const CRON_DEFAULT_TIMEOUT_MS'),
  SRC.indexOf('function newCorrelationId') > 0 ? SRC.indexOf('function newCorrelationId') : SRC.indexOf('// ─── '),
);
const wrapSrc = SRC.slice(SRC.indexOf('const CRON_DEFAULT_TIMEOUT_MS'), SRC.indexOf('\n}', SRC.indexOf('function wrapCronJob')) + 2);
if (!wrapSrc.includes('Promise.race')) { console.error('FAIL: could not extract wrapCronJob with its watchdog'); process.exit(1); }

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

function build() {
  const logged = [];
  const errors = [];
  const factory = new Function('logActivity', 'newCorrelationId', 'process', 'console',
    `${wrapSrc}; return wrapCronJob;`);
  const wrapCronJob = factory(
    (row) => { logged.push(row); },
    () => 'corr-1',
    { env: {} },
    { log: () => {}, warn: () => {}, error: (...a) => { errors.push(a.join(' ')); } },
  );
  return { wrapCronJob, logged, errors };
}

(async () => {
  // 1. A job that completes logs started + ok, and does not throw.
  {
    const { wrapCronJob, logged } = build();
    await wrapCronJob('happyJob', async () => {})();
    check('1a a successful run logs started then ok',
      logged.map(r => r.status), ['started', 'ok']);
    check('1b it records a duration', typeof logged[1].duration_ms, 'number');
  }

  // 2. A job that HANGS is abandoned and recorded as an error rather than vanishing.
  {
    const { wrapCronJob, logged } = build();
    const started = Date.now();
    await wrapCronJob('hangingJob', () => new Promise(() => {}), { timeoutMs: 60 })();
    const elapsed = Date.now() - started;
    check('2a a hung run still reaches a terminal row',
      logged.map(r => r.status), ['started', 'error']);
    check('2b the terminal row is an error, not a silent ok', logged[1].status, 'error');
    check('2c the error says it was abandoned',
      /exceeded|abandoned/.test(logged[1].error_message || ''), true);
    check('2d it gave up near the timeout rather than waiting forever', elapsed < 5000, true);
  }

  // 3. A throwing job is contained — the process must not be taken down by one cron.
  //    Before this, wrapCronJob rethrew into node-cron with nothing above it to
  //    catch the rejection, and Node 20 exits on an unhandled one.
  {
    const { wrapCronJob, logged, errors } = build();
    let threw = false;
    try { await wrapCronJob('throwingJob', async () => { throw new Error('boom'); })(); }
    catch { threw = true; }
    check('3a a failing cron does not propagate an unhandled rejection', threw, false);
    check('3b the failure is recorded, not swallowed', logged[1].status, 'error');
    check('3c and the message is preserved', logged[1].error_message, 'boom');
    check('3d and it is logged to the console too', errors.some(e => /boom/.test(e)), true);
  }

  // 4. A long job inside its budget is NOT abandoned. runAutoStrikeMover really does
  //    run for up to 17 minutes; a flat default would kill a healthy sweep.
  {
    const { wrapCronJob, logged } = build();
    await wrapCronJob('slowButFineJob', () => new Promise(r => setTimeout(r, 120)), { timeoutMs: 5000 })();
    check('4a a slow job inside its budget still succeeds', logged[1].status, 'ok');
  }

  // 5. The default budget is generous enough for every non-sweep cron (max observed 77s)
  //    and the override exists for the one that legitimately exceeds it.
  check('5a the default timeout is at least 5 minutes',
    /CRON_DEFAULT_TIMEOUT_MS\s*=\s*Number\(process\.env\.CRON_DEFAULT_TIMEOUT_MS\s*\|\|\s*10 \* 60 \* 1000\)/.test(SRC), true);
  check('5b the strike mover gets its own longer budget',
    /runAutoStrikeMover[\s\S]{0,160}timeoutMs:\s*30 \* 60 \* 1000/.test(SRC), true);

  console.log(failures ? `\n${failures} check(s) failed.` : '\nAll cron-watchdog checks passed.');
  process.exit(failures ? 1 : 0);
})();
