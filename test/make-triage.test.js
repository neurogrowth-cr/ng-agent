// Rules test for the Make agentic remediation path (DLQ retry + reactivate + triage
// gating).  Run:  node test/make-triage.test.js
//
// Same extraction trick as make-watchdog.test.js: the real block is sliced out of
// index.js and compiled with new Function, so this can never drift from shipped
// behaviour. The triage/proposal helpers live OUTSIDE the slice, so they are injected
// as stubs — no LLM and no Slack call ever runs here.
//
// What this guards, in one line: nothing reaches Make that a human did not approve,
// and nothing gets retried that would double-count a Meta conversion.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const MAKE_API_TOKEN'),
  SRC.indexOf('if (MAKE_API_TOKEN) {'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const SCEN = (id, name, over = {}) => ({ id, name, isActive: true, isinvalid: false, dlqCount: 0, ...over });
const hoursAgo = (h) => new Date(Date.now() - h * 3600 * 1000).toISOString();
// Both response shapes: checkMakeScenarioHealth uses res.json(), makeApi uses res.text().
const reply = (obj) => ({ ok: true, status: 200, json: async () => obj, text: async () => JSON.stringify(obj) });

// Builds the Make block with stubbed I/O. `calls` records every outbound request so
// tests can assert that a blocked path issued ZERO writes, not merely that it
// returned an error string.
function build({ env = {}, scenarios = [], dlqs = [], failRetry = new Set(),
                 triageResult = null, hasOpen = false } = {}) {
  const calls = [], triaged = [], posted = [], alerts = [];

  const fetchStub = async (url, init = {}) => {
    const u = new URL(url);
    const p = u.pathname.replace('/api/v2', '');
    calls.push({ method: (init.method || 'GET').toUpperCase(), path: p });
    // Serve page 0 only; every later offset is empty so the pagination loop
    // terminates. Returning the same page for every offset would replay the
    // scenario ~1000 times per sweep and silently defeat any per-poll streak logic.
    if (p === '/scenarios') {
      const off = Number(u.searchParams.get('pg[offset]') || 0);
      return reply({ scenarios: off === 0 ? scenarios : [] });
    }
    if (/^\/scenarios\/[^/]+$/.test(p)) {
      return reply({ scenario: scenarios.find(s => String(s.id) === p.split('/')[2]) });
    }
    if (/^\/scenarios\/[^/]+\/start$/.test(p)) return reply({ scenario: { isActive: true } });
    if (p === '/dlqs') return reply({ dlqs });
    if (/^\/dlqs\/[^/]+\/retry$/.test(p)) {
      const id = p.split('/')[2];
      return failRetry.has(id) ? { ok: false, status: 500, text: async () => 'boom' } : reply({});
    }
    if (/^\/dlqs\/[^/]+$/.test(p)) return reply({ id: p.split('/')[2], module: 8 });
    return reply({});
  };

  const factory = new Function(
    'process', 'fetch', 'slack', 'getCachedChannelList', 'logActivity', 'OPS_CHANNEL', 'RON_SLACK_ID', 'console',
    'triageAlert', 'postRemediationProposal', 'hasOpenRemediationProposal',
    `${block}; return { checkMakeScenarioHealth, proposeMakeRemediation, retryMakeDlqs, reactivateMakeScenario, listMakeDlqs };`
  );
  const api = factory(
    { env: { MAKE_API_TOKEN: 'tok', MAKE_API_BASE: 'https://make.test/api/v2', MAKE_TEAM_ID: '432699', ...env } },
    fetchStub,
    { client: { chat: { postMessage: async ({ text }) => { alerts.push(text); } } } },
    async () => { throw new Error('no channel cache in test'); },
    () => {}, '#ng-fullfillment-ops', 'U05HXGX18H3',
    { log: () => {}, warn: () => {}, error: () => {} },
    async (a) => { triaged.push(a); return triageResult; },
    async (p) => { posted.push(p); return { channel: 'C1', ts: '1.1' }; },
    async () => hasOpen,
  );
  return { ...api, calls, triaged, posted, alerts };
}

const writes = (calls, re) => calls.filter(c => c.method === 'POST' && re.test(c.path));
// Triage is fired and NOT awaited inside checkMakeScenarioHealth on purpose (it must
// never block or break the alert loop), so let its promise chain settle before
// asserting — otherwise the "nothing was posted" checks pass vacuously.
const drain = () => new Promise(r => setImmediate(r));

(async () => {
  // ── reactivate: the guards ────────────────────────────────────────────────
  {
    const t = build({ scenarios: [SCEN(1, '[PROD] Broken', { isActive: false, isinvalid: true })] });
    const r = await t.reactivateMakeScenario(1);
    check('1a invalid blueprint refuses reactivate', r.ok, false);
    check('1b names the real reason', /INVALID BLUEPRINT/.test(r.error), true);
    check('1c and issues ZERO start calls', writes(t.calls, /\/start$/).length, 0);
  }
  {
    const t = build({ scenarios: [SCEN(5776020, '[PROD] Auto Strike Mover', { isActive: false })] });
    const r = await t.reactivateMakeScenario(5776020);
    check('2a ignore-listed scenario is blocked', r.ok, false);
    check('2b no start call', writes(t.calls, /\/start$/).length, 0);
  }
  {
    const t = build({ scenarios: [SCEN(9, 'Untitled scenario', { isActive: false })] });
    const r = await t.reactivateMakeScenario(9);
    check('3a non-[PROD] scenario is blocked', r.ok, false);
    check('3b no start call', writes(t.calls, /\/start$/).length, 0);
  }
  {
    const t = build({ scenarios: [SCEN(5148796, '[PROD] GHL Appt Booked', { isActive: false })] });
    const r = await t.reactivateMakeScenario(5148796);
    check('4a healthy deactivated scenario reactivates', r.ok, true);
    check('4b exactly one start call', writes(t.calls, /\/start$/).length, 1);
  }
  {
    const t = build({ scenarios: [SCEN(5148796, '[PROD] GHL Appt Booked')] });
    const r = await t.reactivateMakeScenario(5148796);
    check('5a already-active is a no-op success', [r.ok, r.already_active], [true, true]);
    check('5b no redundant start call', writes(t.calls, /\/start$/).length, 0);
  }

  // ── retry: scope, cap, staleness, hallucination ───────────────────────────
  const S = [SCEN(5148796, '[PROD] GHL Appt Booked', { dlqCount: 20 })];
  {
    const many = Array.from({ length: 15 }, (_, i) => ({ id: `d${i}`, created: hoursAgo(2) }));
    const t = build({ scenarios: S, dlqs: many });
    const r = await t.retryMakeDlqs(5148796, many.map(d => d.id));
    check('6a retry caps at MAKE_RETRY_CAP', r.retried, 10);
    check('6b reports that it capped', r.capped, true);
    check('6c exactly 10 retry POSTs', writes(t.calls, /\/retry$/).length, 10);
  }
  {
    const t = build({ scenarios: S, dlqs: [{ id: 'real', created: hoursAgo(2) }] });
    const r = await t.retryMakeDlqs(5148796, ['real', 'hallucinated']);
    check('7a ids absent from the live queue are dropped', r.gone, ['hallucinated']);
    check('7b only the real one is POSTed', writes(t.calls, /\/retry$/).map(c => c.path), ['/dlqs/real/retry']);
  }
  {
    const t = build({ scenarios: S, dlqs: [{ id: 'stale', created: hoursAgo(60) }] });
    const r = await t.retryMakeDlqs(5148796, ['stale']);
    check('8a item past the CAPI dedup window is refused', r.too_old, ['stale']);
    check('8b and is never POSTed (no double-counted conversion)', writes(t.calls, /\/retry$/).length, 0);
  }
  {
    const t = build({ scenarios: [SCEN(9, 'Untitled', { dlqCount: 3 })] });
    const r = await t.retryMakeDlqs(9, ['a']);
    check('9  retry on a non-[PROD] scenario issues no writes', writes(t.calls, /\/retry$/).length, 0);
  }
  {
    const three = ['a', 'b', 'c'].map(id => ({ id, created: hoursAgo(1) }));
    const t = build({ scenarios: S, dlqs: three, failRetry: new Set(['b']) });
    const r = await t.retryMakeDlqs(5148796, ['a', 'b', 'c']);
    check('10a partial failure still reports success for the rest', [r.ok, r.retried], [true, 2]);
    check('10b and names the failed id', r.failed.map(f => f.id), ['b']);
  }
  {
    const t = build({ scenarios: S, dlqs: [] });
    const r = await t.retryMakeDlqs(5148796, []);
    check('11 empty id list is refused before any call', [r.ok, writes(t.calls, /\/retry$/).length], [false, 0]);
  }

  // ── triage gating: cost + loop safety ─────────────────────────────────────
  const DOWN = [SCEN(5148796, '[PROD] GHL Appt Booked', { isActive: false })];
  {
    const t = build({ scenarios: DOWN });                       // MAKE_TRIAGE_ENABLED unset
    await t.checkMakeScenarioHealth('c1'); await drain();
    check('12a flag off: alert still posts', t.alerts.length, 1);
    check('12b flag off: zero triage', t.triaged.length, 0);
  }
  {
    const t = build({ env: { MAKE_TRIAGE_ENABLED: 'true' }, scenarios: DOWN,
                      triageResult: { recommended_action: 'reactivate_scenario', summary: 's', root_cause: 'r', confidence: 'high' } });
    await t.checkMakeScenarioHealth('c1'); await drain();
    check('13a flag on: triage runs once', t.triaged.length, 1);
    check('13b and a proposal is posted', t.posted.length, 1);
    await t.checkMakeScenarioHealth('c2'); await drain();
    check('13c repeat sweep with unchanged reason does NOT re-triage', t.triaged.length, 1);
  }
  {
    // The interaction between DLQ damping and triage: a queued DLQ must survive
    // DLQ_CONFIRM_POLLS before it counts as a reason at all, so triage must stay
    // silent through the unconfirmed polls and fire exactly once on confirmation.
    // Neither PR covers this on its own — damping is tested without triage, and
    // the triage tests above all use the immediate `inactive` reason.
    const QUEUED = [SCEN(5148796, '[PROD] GHL Appt Booked', { dlqCount: 2 })];
    const t = build({ env: { MAKE_TRIAGE_ENABLED: 'true' }, scenarios: QUEUED,
                      dlqs: [{ id: 'a', created: hoursAgo(1) }, { id: 'b', created: hoursAgo(1) }],
                      triageResult: { recommended_action: 'retry_dlqs', dlq_ids: ['a','b'], summary: 's', root_cause: 'r', confidence: 'high' } });
    await t.checkMakeScenarioHealth('p1'); await drain();
    await t.checkMakeScenarioHealth('p2'); await drain();
    check('13d unconfirmed DLQ does not triage (no LLM burned on a flap)', [t.triaged.length, t.alerts.length], [0, 0]);
    await t.checkMakeScenarioHealth('p3'); await drain();
    check('13e confirmed DLQ triages exactly once', t.triaged.length, 1);
    check('13f and carries the dlq ids through to the proposal', t.posted[0].payload.dlq_ids, ['a','b']);
  }
  {
    const t = build({ env: { MAKE_TRIAGE_ENABLED: 'true' }, scenarios: DOWN, hasOpen: true });
    await t.checkMakeScenarioHealth('c1'); await drain();
    check('14 an open proposal (survives restarts) suppresses re-triage', t.triaged.length, 0);
  }
  {
    const t = build({ env: { MAKE_TRIAGE_ENABLED: 'true' },
                      scenarios: [SCEN(5148796, '[PROD] GHL Appt Booked', { isinvalid: true })] });
    await t.checkMakeScenarioHealth('c1'); await drain();
    check('15 invalid-blueprint alerts are not triaged (no action can fix them)', t.triaged.length, 0);
  }
  {
    const t = build({ env: { MAKE_TRIAGE_ENABLED: 'true' }, scenarios: DOWN,
                      triageResult: { recommended_action: 'no_action', summary: 's', root_cause: 'r', confidence: 'low' } });
    await t.checkMakeScenarioHealth('c1'); await drain();
    check('16 no_action verdict posts no proposal', t.posted.length, 0);
  }
  {
    const t = build({ env: { MAKE_TRIAGE_ENABLED: 'true' }, scenarios: DOWN, triageResult: null });
    await t.checkMakeScenarioHealth('c1'); await drain();
    check('17a a failed triage does not throw or post', t.posted.length, 0);
    check('17b and the plain alert still fired', t.alerts.length, 1);
  }

  // ── read path ─────────────────────────────────────────────────────────────
  {
    const t = build({ scenarios: S, dlqs: [{ id: 'x', created: hoursAgo(1), reason: 'timeout' }] });
    const r = await t.listMakeDlqs(5148796);
    check('18 listMakeDlqs normalises the envelope', [r.count, r.dlqs[0].id, r.dlqs[0].reason], [1, 'x', 'timeout']);
  }

  console.log(failures ? `\n${failures} failing` : '\nall passing');
  process.exit(failures ? 1 : 0);
})();
