// Rules test for the Make [PROD] scenario watchdog.  Run:  node test/make-watchdog.test.js
//
// Extracts the real checkMakeScenarioHealth block straight out of index.js rather
// than copying it, so the test can never drift from shipped behaviour (same
// approach as strike-rules.test.js — index.js boots the Slack app on require).
//
// Fixtures are shaped from a live Make API response for team 432699 captured
// 2026-08-04, including [PROD] Auto Strike Mover, which is deactivated on purpose.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const MAKE_API_TOKEN'),
  SRC.indexOf('if (MAKE_API_TOKEN) {'),
);
// The divergence checker reuses postMakeHealthAlert from the block above, so the
// two are concatenated when building it.
const divergenceBlock = block + SRC.slice(
  SRC.indexOf('const bookingDivergenceAlerted'),
  SRC.indexOf('// Every 30 min — the 30-minute grace window'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// Builds a fresh watchdog with stubbed I/O. Returns the checker plus the alert log.
function build({ token = 'tok', pages = [], status = 200 } = {}) {
  const posted = [];
  const env = { MAKE_API_TOKEN: token, MAKE_API_BASE: 'https://make.test/api/v2', MAKE_TEAM_ID: '432699' };
  const urls = [];
  const fetchStub = async (url) => {
    urls.push(url);
    if (status !== 200) return { ok: false, status, text: async () => 'nope' };
    const off = Number(new URL(url).searchParams.get('pg[offset]') || 0);
    // Serve the fixture pages in order; anything past the end is an empty page.
    const idx = pages.findIndex(p => p.offset === off);
    return { ok: true, status: 200, json: async () => ({ scenarios: idx === -1 ? [] : pages[idx].scenarios }) };
  };
  const slackStub = { client: { chat: { postMessage: async ({ text }) => { posted.push(text); } } } };
  const factory = new Function(
    'process', 'fetch', 'slack', 'getCachedChannelList', 'logActivity', 'OPS_CHANNEL', 'RON_SLACK_ID', 'console',
    `${block}; return checkMakeScenarioHealth;`
  );
  const fn = factory(
    { env }, fetchStub, slackStub,
    async () => { throw new Error('no channel cache in test'); }, // forces the post-by-name fallback
    () => {}, '#ng-fullfillment-ops', 'U05HXGX18H3',
    { log: () => {}, warn: () => {}, error: () => {} },
  );
  return { fn, posted, urls };
}

const SCEN = (id, name, over = {}) => ({ id, name, isActive: true, isinvalid: false, ...over });
const page = (offset, scenarios) => ({ offset, scenarios });

// 1. A deactivated [PROD] scenario alerts exactly once, and tags Ron.
(async () => {
  const down = [page(0, [
    SCEN(5148796, '[PROD] GHL Appt Booked → CAPI + Slack Alert', { isActive: false }),
    SCEN(5148801, '[PROD] GHL Opp Won → CAPI Purchase'),
    SCEN(4851355, 'FB Lead Form → GHL Lead ID Sync + Booking Link', { isActive: false }), // non-[PROD]: ignored
  ])];
  const { fn, posted } = build({ pages: down });
  await fn('corr');
  check('1a deactivated [PROD] scenario alerts', posted.length, 1);
  check('1b alert tags Ron', posted[0].includes('<@U05HXGX18H3>'), true);
  check('1c alert names the scenario', posted[0].includes('[PROD] GHL Appt Booked'), true);
  check('1d non-[PROD] scenario is not watched', posted[0].includes('FB Lead Form'), false);

  // 2. Still down on the next run → no repeat alert.
  await fn('corr');
  check('2  no duplicate alert while still down', posted.length, 1);
})().then(() => {

// 3. Recovery: down on run 1, healthy on run 2 → one alert + one all-clear.
  return (async () => {
    let healthy = false;
    const posted = [];
    const factory = new Function(
      'process', 'fetch', 'slack', 'getCachedChannelList', 'logActivity', 'OPS_CHANNEL', 'RON_SLACK_ID', 'console',
      `${block}; return checkMakeScenarioHealth;`
    );
    const fn = factory(
      { env: { MAKE_API_TOKEN: 'tok', MAKE_API_BASE: 'https://make.test/api/v2', MAKE_TEAM_ID: '432699' } },
      async (url) => {
        const off = Number(new URL(url).searchParams.get('pg[offset]') || 0);
        const rows = off === 0
          ? [SCEN(5148796, '[PROD] GHL Appt Booked → CAPI + Slack Alert', { isActive: healthy })]
          : [];
        return { ok: true, status: 200, json: async () => ({ scenarios: rows }) };
      },
      { client: { chat: { postMessage: async ({ text }) => { posted.push(text); } } } },
      async () => { throw new Error('no cache'); }, () => {}, '#ng-fullfillment-ops', 'U05HXGX18H3',
      { log: () => {}, warn: () => {}, error: () => {} },
    );
    await fn('c');            // down
    healthy = true;
    await fn('c');            // recovered
    check('3a recovery posts a second message', posted.length, 2);
    check('3b recovery message is an all-clear', posted[1].startsWith('✅'), true);
    await fn('c');
    check('3c no repeat all-clear once healthy', posted.length, 2);
  })();

}).then(() => {

// 4. Intentionally-retired scenario on the ignore list stays quiet.
  return (async () => {
    const { fn, posted } = build({ pages: [page(0, [
      SCEN(5776020, '[PROD] Auto Strike Mover', { isActive: false }),
    ])] });
    await fn('c');
    check('4  ignore-listed scenario does not alert', posted.length, 0);
  })();

}).then(() => {

// 5. An invalid blueprint alerts before Make deactivates it.
  return (async () => {
    const { fn, posted } = build({ pages: [page(0, [
      SCEN(5822303, '[PROD] GHL Appt Cancelled/Rescheduled → Slack Alert', { isinvalid: true }),
    ])] });
    await fn('c');
    check('5a invalid blueprint alerts', posted.length, 1);
    check('5b invalid alert says INVALID BLUEPRINT', posted[0].includes('INVALID BLUEPRINT'), true);
  })();

}).then(() => {

// 6. Pagination: a broken scenario on page 2 is still caught.
  return (async () => {
    const full = Array.from({ length: 100 }, (_, i) => SCEN(1000 + i, `Filler ${i}`));
    const { fn, posted, urls } = build({ pages: [
      page(0, full),
      page(100, [SCEN(5148801, '[PROD] GHL Opp Won → CAPI Purchase', { isActive: false })]),
    ] });
    await fn('c');
    check('6a walks past the first page', urls.length >= 3, true);
    check('6b catches a down scenario on page 2', posted.length, 1);
    check('6c names the page-2 scenario', posted[0].includes('GHL Opp Won'), true);
  })();

}).then(() => {

// 7. A dead API token makes the watchdog loudly blind, once.
  return (async () => {
    const { fn, posted } = build({ status: 401 });
    await fn('c');
    await fn('c');
    check('7a API failure alerts once', posted.length, 1);
    check('7b API failure names the token', posted[0].includes('MAKE_API_TOKEN'), true);
  })();

}).then(() => {

// 8. No token configured → completely inert, no API calls, no posts.
  return (async () => {
    const { fn, posted, urls } = build({ token: '', pages: [page(0, [
      SCEN(5148796, '[PROD] GHL Appt Booked → CAPI + Slack Alert', { isActive: false }),
    ])] });
    await fn('c');
    check('8a no token → no alert', posted.length, 0);
    check('8b no token → no API call', urls.length, 0);
  })();

}).then(() => {

// 9. A scenario that is active but piling up incomplete executions still alerts —
//    the "has NOT been paused" class isActive never catches — but only once the
//    queue has PERSISTED, so a flapping dlqCount cannot spam the channel.
  return (async () => {
    const { fn, posted } = build({ pages: [page(0, [
      SCEN(5148796, '[PROD] GHL Appt Booked → CAPI + Slack Alert', { dlqCount: 4 }),
    ])] });
    await fn('c');
    check('9a first DLQ poll stays quiet', posted.length, 0);
    await fn('c');
    check('9b second DLQ poll stays quiet', posted.length, 0);
    await fn('c');
    check('9c sustained DLQ alerts on the third poll', posted.length, 1);
    check('9d names the incomplete executions', posted[0].includes('4 incomplete execution(s)'), true);
    check('9e does not call it deactivated', posted[0].includes('DEACTIVATED'), false);
    await fn('c');
    check('9f no repeat while the queue persists', posted.length, 1);
  })();

}).then(() => {

// 9b. THE FLAPPING CASE: dlqCount oscillating never reaches the threshold, so it
//     never alerts. This is the 2026-08-13 05:00/05:10/05:20 pattern.
  return (async () => {
    let dlq = 2;
    const posted = [];
    const factory = new Function(
      'process', 'fetch', 'slack', 'getCachedChannelList', 'logActivity', 'OPS_CHANNEL', 'RON_SLACK_ID', 'console',
      `${block}; return checkMakeScenarioHealth;`
    );
    const fn = factory(
      { env: { MAKE_API_TOKEN: 'tok', MAKE_API_BASE: 'https://make.test/api/v2', MAKE_TEAM_ID: '432699' } },
      async (url) => {
        const off = Number(new URL(url).searchParams.get('pg[offset]') || 0);
        const rows = off === 0 ? [SCEN(5148796, '[PROD] GHL Appt Booked → CAPI + Slack Alert', { dlqCount: dlq })] : [];
        return { ok: true, status: 200, json: async () => ({ scenarios: rows }) };
      },
      { client: { chat: { postMessage: async ({ text }) => { posted.push(text); } } } },
      async () => { throw new Error('no cache'); }, () => {}, '#ng-fullfillment-ops', 'U05HXGX18H3',
      { log: () => {}, warn: () => {}, error: () => {} },
    );
    for (let i = 0; i < 12; i++) { dlq = i % 2 ? 0 : 2; await fn('c'); }
    check('9g a flapping DLQ never alerts', posted.length, 0);

    // …but once it stops flapping and stays queued, it does alert.
    dlq = 2;
    await fn('c'); await fn('c'); await fn('c');
    check('9h a settled DLQ still alerts', posted.length, 1);

    // Recovery is damped too: one clear poll is not enough for an all-clear.
    dlq = 0;
    await fn('c');
    check('9i one clear poll posts no premature all-clear', posted.length, 1);
    await fn('c'); await fn('c');
    check('9j sustained clear posts exactly one all-clear', posted.length, 2);
    check('9k all-clear is the recovery message', posted[1].startsWith('✅'), true);
  })();

}).then(() => {

// ── Booking → alert divergence ──────────────────────────────────────────────
// Chainable Supabase stub: records the filters each query applied so the grace
// window can be asserted, and returns whatever the fixture configures per table.
  function buildDivergence({ appts = [], apptErr = null, alerts = [], alertErr = null } = {}) {
    const posted = [];
    const seen = { appts: {}, alerts: {} };
    const portalStub = {
      from(table) {
        const key = table === 'revops_appointments' ? 'appts' : 'alerts';
        const res = key === 'appts' ? { data: appts, error: apptErr } : { data: alerts, error: alertErr };
        const chain = {
          select: () => chain,
          eq: (col, val) => { seen[key][col] = val; return chain; },
          gte: (col, val) => { seen[key].gte = val; return chain; },
          lte: (col, val) => { seen[key].lte = val; return chain; },
          in:  (col, val) => { seen[key].in  = val; return chain; },
          limit: () => Promise.resolve(res),
          then: (onOk) => Promise.resolve(res).then(onOk), // awaited without .limit()
        };
        return chain;
      },
    };
    const factory = new Function(
      'process', 'fetch', 'slack', 'getCachedChannelList', 'logActivity', 'OPS_CHANNEL', 'RON_SLACK_ID',
      'console', 'portalSupabase', 'getNonFlywheelCallIds', 'filterFlywheelAppts',
      `${divergenceBlock}; return checkBookingAlertDivergence;`
    );
    const fn = factory(
      { env: {} }, async () => { throw new Error('no fetch here'); },
      { client: { chat: { postMessage: async ({ text }) => { posted.push(text); } } } },
      async () => { throw new Error('no cache'); }, () => {}, '#ng-fullfillment-ops', 'U05HXGX18H3',
      { log: () => {}, warn: () => {}, error: () => {} },
      portalStub,
      async () => new Set(),
      // the real filterFlywheelAppts, pulled out of index.js rather than reimplemented
      new Function(`${SRC.slice(SRC.indexOf('function filterFlywheelAppts'), SRC.indexOf('async function getSalesIntelligence'))}; return filterFlywheelAppts;`)(),
    );
    return { fn, posted, seen };
  }

  const APPT = (id, over = {}) => ({ ghl_appointment_id: id, iclosed_call_id: null, source: 'ghl', created_at: '2026-08-06T04:00:00Z', ...over });

  return (async () => {
    // 10. Every booking has its alert row → silent.
    const ok = buildDivergence({ appts: [APPT('a1'), APPT('a2')], alerts: [{ appointment_id: 'a1' }, { appointment_id: 'a2' }] });
    await ok.fn('c');
    check('10 fully-alerted window stays silent', ok.posted.length, 0);

    // 11/12. A missing alert row fires once, then stays quiet.
    const gap = buildDivergence({ appts: [APPT('a1'), APPT('a2')], alerts: [{ appointment_id: 'a1' }] });
    await gap.fn('c');
    check('11a missing booked alert fires', gap.posted.length, 1);
    check('11b names the appointment', gap.posted[0].includes('a2'), true);
    check('11c tags Ron', gap.posted[0].includes('<@U05HXGX18H3>'), true);
    await gap.fn('c');
    check('12  standing gap does not re-post', gap.posted.length, 1);

    // 13. THE CRITICAL ONE: alerts table unreadable must NOT read as "everything
    //     diverged" — RLS or a dropped grant would otherwise storm the channel.
    const blind = buildDivergence({ appts: [APPT('a1'), APPT('a2')], alertErr: { message: 'permission denied' } });
    await blind.fn('c');
    check('13 unreadable alerts table fails closed (no alert)', blind.posted.length, 0);

    // 14. Unreadable appointments table is likewise silent, not a gap.
    const noAppts = buildDivergence({ apptErr: { message: 'permission denied' } });
    await noAppts.fn('c');
    check('14 unreadable appointments table stays silent', noAppts.posted.length, 0);

    // 15. Synthetic pre-book disqualification rows are not real calls.
    const synth = buildDivergence({ appts: [APPT('opp:12345')], alerts: [] });
    await synth.fn('c');
    check('15 synthetic opp: row is not a gap', synth.posted.length, 0);

    // 16. The query asks only for settled bookings — the grace window keeps
    //     in-flight ones out, which is what stops this alerting on every booking.
    const win = buildDivergence({ appts: [APPT('a1')], alerts: [{ appointment_id: 'a1' }] });
    await win.fn('c');
    const graceMs = Date.now() - new Date(win.seen.appts.lte).getTime();
    check('16a upper bound is ~30 min in the past', graceMs > 25 * 60_000 && graceMs < 35 * 60_000, true);
    check('16b only GHL-sourced bookings are checked', win.seen.appts.source, 'ghl');
    check('16c only booked-kind alert rows are matched', win.seen.alerts.kind, 'booked');
  })();

}).then(() => {
  console.log(failures ? `\n${failures} failure(s)` : '\nall passing');
  process.exit(failures ? 1 : 0);
});
