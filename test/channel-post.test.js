// Regression test for executeChannelPost.  Run:  node test/channel-post.test.js
//
// Guards the two faults that silently killed the Monday delivery gap report for
// months. Both were invisible: the report returns early when there are no gaps, so
// the broken path only ran on the weeks that actually HAD something to report, and
// the failure was swallowed by a catch that only console.error'd.
//
//   1. runMondayGapDetection declared `_correlationId` but its post line referenced
//      `correlationId` -> ReferenceError thrown while evaluating the call arguments,
//      so executeChannelPost was never even entered.
//   2. executeChannelPost called say() unguarded. On cron paths say is null, so the
//      channel post SUCCEEDED and then the confirmation threw a TypeError — and the
//      catch called say() again, throwing a second time.
//
// Extracted from index.js rather than copied (index.js boots the Slack app on
// require), same approach as make-watchdog.test.js.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('async function executeChannelPost'),
  SRC.indexOf('// ─── EMAIL PROXY: tool handlers'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

function build({ channels = [{ id: 'C1', name: 'ng-fullfillment-ops' }], postThrows = false } = {}) {
  const posted = [], said = [], logged = [];
  const slackStub = { client: { chat: { postMessage: async (a) => {
    if (postThrows) throw new Error('slack exploded');
    posted.push(a);
  } } } };
  const factory = new Function(
    'slack', 'getCachedChannelList', 'logActivity', 'console',
    `${block}; return executeChannelPost;`
  );
  const fn = factory(
    slackStub,
    async () => channels,
    (row) => { logged.push(row); },
    { log: (m) => said.push(`[log] ${m}`), warn: () => {}, error: () => {} },
  );
  return { fn, posted, said, logged };
}

(async () => {
  // ── the cron path: say === null ──────────────────────────────────────────
  {
    const t = build();
    let threw = null;
    try { await t.fn('#ng-fullfillment-ops', 'gap report body', null, 'cid-1'); }
    catch (e) { threw = e.message; }
    check('1a cron path (say=null) does not throw', threw, null);
    check('1b and the message actually reaches the channel', t.posted.map(p => p.text), ['gap report body']);
    check('1c confirmation is logged instead of said', t.said.some(s => s.includes('Posted to')), true);
    check('1d the post is recorded in agent_activity', t.logged.filter(r => r.action === 'outbound').length, 2);
  }
  {
    // Unknown channel must not throw either — the old code called say() here too.
    const t = build({ channels: [] });
    let threw = null;
    try { await t.fn('#does-not-exist', 'body', null, 'cid-2'); }
    catch (e) { threw = e.message; }
    check('2a unknown channel with say=null does not throw', threw, null);
    check('2b and nothing is posted', t.posted.length, 0);
  }
  {
    // The double-fault: post throws, catch used to call say() and throw again.
    const t = build({ postThrows: true });
    let threw = null;
    try { await t.fn('#ng-fullfillment-ops', 'body', null, 'cid-3'); }
    catch (e) { threw = e.message; }
    check('3  a failing post with say=null is caught, not re-thrown', threw, null);
  }

  // ── the interactive path is unchanged: say is a real function ─────────────
  {
    const t = build();
    const said = [];
    await t.fn('#ng-fullfillment-ops', 'approved draft', async (m) => { said.push(m); }, 'cid-4');
    check('4a real say still receives the confirmation', said, ['Posted to #ng-fullfillment-ops.']);
    check('4b and the message still posts', t.posted.map(p => p.text), ['approved draft']);
  }
  {
    const t = build({ channels: [] });
    const said = [];
    await t.fn('#nope', 'x', async (m) => { said.push(m); }, 'cid-5');
    check('5  real say still receives the not-found message', said, ['Could not find channel #nope.']);
  }

  // ── the caller that started it all ───────────────────────────────────────
  {
    const sig = /async function runMondayGapDetection\(([^)]*)\)/.exec(SRC);
    check('6a runMondayGapDetection takes a USED (non-underscored) correlationId', sig && sig[1], 'correlationId');
    const body = SRC.slice(SRC.indexOf('async function runMondayGapDetection'), SRC.indexOf('// ─── NIGHTLY LEARNING'));
    check('6b and no stray _correlationId reference survives in its body', /\b_correlationId\b/.test(body), false);
  }

  console.log(failures ? `\n${failures} failing` : '\nall passing');
  process.exit(failures ? 1 : 0);
})();
