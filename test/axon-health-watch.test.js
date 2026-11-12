// Rules test for the AXON health watch.  Run:  node test/axon-health-watch.test.js
//
// lib/axonHealth.js is pure (no Slack, no DB). The key fixture is the
// 2026-11-12 Supabase outage: the heartbeat READ failed with "upstream request
// timeout" and a Cloudflare 522 page, AXON was alive, and Max told the team to
// redeploy it. A failed read must never produce the AXON-down alert.
const ax = require('../lib/axonHealth');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MIN = 60 * 1000;
const T0 = new Date('2026-11-12T01:42:00Z').getTime();
const fresh = [{ beat_at: new Date(T0 - 3 * MIN).toISOString() }];
const stale = [{ beat_at: new Date(T0 - 45 * MIN).toISOString() }];
const HTML_522 = '<!DOCTYPE html>\n<html><head><title>zbuqpdwjpxgsetduhjeo.supabase.co | 522: Connection timed out</title></head>' +
  '<body><span>Error code 522</span>\n<p>Visit cloudflare.com</p></body></html>';

// ── 1. errorOneLine
check('1a  plain message kept', ax.errorOneLine({ message: 'upstream request timeout' }), 'upstream request timeout');
check('1b  522 page collapses', ax.errorOneLine({ message: HTML_522 }), 'Supabase did not respond (522, connection timed out)');
check('1c  html without code uses title', ax.errorOneLine('<html><head><title>Bad things</title></head></html>'), 'The server returned an error page: Bad things');
check('1d  newlines flattened', ax.errorOneLine('a\n\n  b'), 'a b');
check('1e  null', ax.errorOneLine(null), 'error with no message');
check('1f  clipped', ax.errorOneLine('x'.repeat(500), 20).length, 20);

// ── 2. classify
const c = (o) => ax.classifyAxonHeartbeat({ now: T0, ...o });
check('2a  fresh beat ok', c({ data: fresh }).kind, 'ok');
check('2b  stale beat down', c({ data: stale }), { kind: 'down', reason: 'last beat 45 min ago (cadence is 5 min)' });
check('2c  no row down', c({ data: [] }).kind, 'down');
check('2d  read error blind', c({ error: { message: 'upstream request timeout' } }), { kind: 'blind', reason: 'heartbeat read failed: upstream request timeout' });
check('2e  throw blind', c({ thrown: new Error('fetch failed') }), { kind: 'blind', reason: 'heartbeat check threw: fetch failed' });
check('2f  html error is one line', c({ error: { message: HTML_522 } }).reason, 'heartbeat read failed: Supabase did not respond (522, connection timed out)');

// ── 3. the 2026-11-12 incident: three failed reads, then recovery
function run(kinds, start = ax.newAxonWatchState()) {
  let state = start;
  const out = [];
  kinds.forEach((r, i) => {
    const step = ax.stepAxonWatch(state, r, T0 + i * 10 * MIN);
    state = step.state;
    out.push(step.posts);
  });
  return { state, out };
}
const BLIND = { kind: 'blind', reason: 'heartbeat read failed: upstream request timeout' };
const DOWN = { kind: 'down', reason: 'last beat 45 min ago (cadence is 5 min)' };
const OK = { kind: 'ok', reason: null };

const inc = run([BLIND, BLIND, BLIND, OK]);
check('3a  no post before streak 3', [inc.out[0].length, inc.out[1].length], [0, 0]);
check('3b  third blind poll alerts once', inc.out[2].length, 1);
check('3c  blind alert never says AXON looks DOWN', /AXON looks DOWN/.test(inc.out[2][0]), false);
check('3d  blind alert says do not redeploy', /Do not redeploy AXON/.test(inc.out[2][0]), true);
check('3e  blind alert points at Supabase', /status\.supabase\.com/.test(inc.out[2][0]), true);
check('3f  blind alert is one reason line', inc.out[2][0].split('\n')[0], "🟠 *Max cannot read AXON's heartbeat*: heartbeat read failed: upstream request timeout.");
check('3g  recovery matches blind', inc.out[3], [ax.BLIND_RECOVERY_TEXT]);
check('3h  down streak untouched by blind polls', inc.state.down, { streak: 0, alerted: false, lastAlertAt: 0 });

// ── 4. a real stale heartbeat still says AXON looks DOWN, recovery matches
const dn = run([DOWN, DOWN, DOWN, OK]);
check('4a  down alert on third poll', /AXON looks DOWN/.test(dn.out[2][0]) && /redeploy it/.test(dn.out[2][0]), true);
check('4b  down recovery text', dn.out[3], [ax.DOWN_RECOVERY_TEXT]);

// ── 5. blips never page; blind does not reset or advance the down streak
check('5a  two blind then ok: silent', run([BLIND, BLIND, OK]).out.flat(), []);
const mixed = run([DOWN, DOWN, BLIND, DOWN]);
check('5b  blind between down polls keeps down streak', mixed.out[3].length, 1);
check('5c  only the down alert fired', /AXON looks DOWN/.test(mixed.out[3][0]), true);

// ── 6. 6h re-alert per kind
let s = run([BLIND, BLIND, BLIND]).state;
let st = ax.stepAxonWatch(s, BLIND, T0 + 60 * MIN);
check('6a  no re-alert inside 6h', st.posts.length, 0);
st = ax.stepAxonWatch(st.state, BLIND, T0 + 20 * MIN + ax.AXON_WATCH_REALERT_MS);
check('6b  re-alert after 6h', st.posts.length, 1);

// ── 7. outage alerted, DB returns and AXON really is down: both lines, in order
const both = run([BLIND, BLIND, BLIND, DOWN, DOWN, DOWN]);
check('7a  blind recovery on first good read', both.out[3], [ax.BLIND_RECOVERY_TEXT]);
check('7b  then the down alert after its own streak', /AXON looks DOWN/.test((both.out[5] || [])[0] || ''), true);

// ── 8. no em dashes in anything we post
const texts = [ax.downAlertText('x'), ax.blindAlertText('x'), ax.DOWN_RECOVERY_TEXT, ax.BLIND_RECOVERY_TEXT];
check('8a  no em dash in alert copy', texts.some((t) => t.includes(String.fromCharCode(0x2014))), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall axon health watch checks passed');
