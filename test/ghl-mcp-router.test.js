// Tests the GHL native-MCP operation router.  Run:  node test/ghl-mcp-router.test.js
//
// Same extraction trick as the other suites: the router block is sliced out of
// index.js and compiled with new Function, so this can never drift from shipped
// behaviour. ghlFetch is injected as a fake, so the suite is offline and instant.
//
// The load-bearing part is the write gate. Ron added write scopes to the Private
// Integration Token on 2026-08-23, so the token can now mutate objects nothing else
// guards. These tests pin the three layers that stand between an LLM tool call and a
// GHL write: deletes/money movement are refused unconditionally, every other non-read
// needs its exact operationId armed in GHL_MCP_WRITE_ALLOWLIST, and the financial
// domains stay Ron-only. `kind` and `domain` come from describe_operation, never from
// tool input, so the model cannot assert its way past any of it.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const START = '// ── GHL native MCP operation router ─';
const END   = '// ── end GHL native MCP operation router ─';
if (SRC.indexOf(START) === -1 || SRC.indexOf(END) === -1) {
  console.error('FAIL  router block sentinels missing from index.js — did the comment markers change?');
  process.exit(1);
}
const block = SRC.slice(SRC.indexOf(START), SRC.indexOf(END));

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}
function checkIncludes(name, actual, needle) {
  if (typeof actual === 'string' && actual.includes(needle)) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected a string containing ${JSON.stringify(needle)}\n      got      ${JSON.stringify(actual)}`); }
}
async function throws(name, fn, needle) {
  try { await fn(); failures += 1; console.error(`FAIL  ${name}\n      expected a throw, got none`); }
  catch (err) {
    if (String(err.message).includes(needle)) console.log(`  ok  ${name}`);
    else { failures += 1; console.error(`FAIL  ${name}\n      expected message containing ${JSON.stringify(needle)}\n      got      ${JSON.stringify(err.message)}`); }
  }
}

const RON = 'U05HXGX18H3';

// A Response stand-in with just the surface ghlMcpCall touches. `body` is the JSON the
// MCP tool result carries; it gets wrapped in the SSE + JSON-RPC framing the real
// endpoint uses (captured verbatim from a live probe on 2026-08-23).
function sseResponse(body, { status = 200 } = {}) {
  const frame = `event: message\ndata: ${JSON.stringify({
    result: { content: [{ type: 'text', text: JSON.stringify(body) }], isError: false },
    jsonrpc: '2.0', id: 1,
  })}\n\n`;
  return { status, ok: status >= 200 && status < 300, text: async () => frame };
}

// Builds the router over a scripted ghlFetch. `script` maps MCP tool name -> body (or
// a function of the arguments). Records every call so a test can assert that a blocked
// operation never reached execute_operation.
function build(script, env = {}) {
  const calls = [];
  const fakeGhlFetch = async (url, init) => {
    const rpc = JSON.parse(init.body);
    const name = rpc.params.name;
    calls.push({ name, args: rpc.params.arguments, headers: init.headers, url });
    const entry = script[name];
    if (entry === undefined) throw new Error(`test script has no entry for ${name}`);
    return typeof entry === 'function' ? entry(rpc.params.arguments) : sseResponse(entry);
  };
  const fakeProcess = { env: { GHL_API_KEY: 'pit-test', GHL_LOCATION_ID: 'loc-test', ...env } };
  const api = new Function('ghlFetch', 'RON_SLACK_ID', 'process', 'console', `${block}
    return { parseMcpSse, ghlMcpTruncate, ghlMcpWriteAllowlist, ghlMcpGateDecision,
             ghlMcpOperationMeta, ghlMcpFindOperation, ghlMcpDescribeOperation, ghlMcpRunOperation };`
  )(fakeGhlFetch, RON, fakeProcess, { warn() {}, log() {}, error() {} });
  return { ...api, calls };
}

(async () => {
  // ── SSE framing ───────────────────────────────────────────────────────────
  {
    const { parseMcpSse } = build({});
    // Verbatim from a live initialize probe, 2026-08-23.
    const real = 'event: message\ndata: {"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"ghl-mcp","version":"1.0.0"}},"jsonrpc":"2.0","id":1}\n\n';
    check('parseMcpSse reads a real live frame', parseMcpSse(real).result.serverInfo.name, 'ghl-mcp');
    check('parseMcpSse takes the LAST data frame',
      parseMcpSse('data: {"n":1}\ndata: {"n":2}\n').n, 2);
    let threw = false;
    try { parseMcpSse('event: ping\n\n'); } catch (e) { threw = /no data frame/.test(e.message); }
    check('parseMcpSse throws when there is no data frame', threw, true);
  }

  // ── Allowlist parsing ─────────────────────────────────────────────────────
  {
    check('empty allowlist env means read-only',
      [...build({}, {}).ghlMcpWriteAllowlist()], []);
    check('allowlist tolerates whitespace and empty entries',
      [...build({}, { GHL_MCP_WRITE_ALLOWLIST: ' update-contact , ,add-tags ' }).ghlMcpWriteAllowlist()],
      ['update-contact', 'add-tags']);
  }

  // ── Gate policy (pure) ────────────────────────────────────────────────────
  {
    const { ghlMcpGateDecision } = build({});
    const gate = (meta, allow = [], isRon = true) =>
      ghlMcpGateDecision(meta, { allowlist: new Set(allow), operationId: 'op-x', isRon });

    check('read passes with an empty allowlist',
      gate({ kind: 'read', domain: 'contacts' }).allowed, true);
    check('write is refused when not armed',
      gate({ kind: 'write', domain: 'contacts' }).allowed, false);
    check('write passes once its operationId is armed',
      gate({ kind: 'write', domain: 'contacts' }, ['op-x']).allowed, true);
    check('arming a DIFFERENT operation does not arm this one',
      gate({ kind: 'write', domain: 'contacts' }, ['op-other']).allowed, false);

    // The allowlist must never be able to reach these two.
    check('delete is refused even when armed',
      gate({ kind: 'delete', domain: 'contacts' }, ['op-x']).allowed, false);
    check('money_movement is refused even when armed',
      gate({ kind: 'money_movement', domain: 'payments' }, ['op-x']).allowed, false);
    checkIncludes('delete refusal explains itself',
      gate({ kind: 'delete', domain: 'contacts' }, ['op-x']).reason, 'deletes or money movement');

    // Financial confidentiality applies to READS too, not just writes.
    check('payments read is refused for a non-Ron user',
      gate({ kind: 'read', domain: 'payments' }, [], false).allowed, false);
    check('invoices read is refused for a non-Ron user',
      gate({ kind: 'read', domain: 'invoices' }, [], false).allowed, false);
    check('payments read passes for Ron',
      gate({ kind: 'read', domain: 'payments' }, [], true).allowed, true);
    check('non-financial read passes for a non-Ron user',
      gate({ kind: 'read', domain: 'contacts' }, [], false).allowed, true);
  }

  // ── Meta fails closed ─────────────────────────────────────────────────────
  {
    // An operation whose describe payload omits `kind` must be treated as a write,
    // never as a read — otherwise an unrecognised shape becomes a free mutation.
    const r = build({ describe_operation: { operation: { operationId: 'mystery', domain: 'contacts' } } });
    const meta = await r.ghlMcpOperationMeta('mystery');
    check('missing kind defaults to write, not read', meta.kind, 'write');
  }
  {
    const r = build({ describe_operation: { operation: { operationId: 'op-a', kind: 'read', domain: 'contacts' } } });
    await r.ghlMcpOperationMeta('op-a');
    await r.ghlMcpOperationMeta('op-a');
    check('operation meta is cached after the first describe', r.calls.length, 1);
  }

  // ── End to end: a blocked call must never reach execute_operation ─────────
  {
    const r = build({ describe_operation: { operation: { operationId: 'delete-contact', kind: 'delete', domain: 'contacts' } } },
                    { GHL_MCP_WRITE_ALLOWLIST: 'delete-contact' });
    const out = await r.ghlMcpRunOperation({ operationId: 'delete-contact', params: { contactId: 'abc' } }, RON);
    checkIncludes('run refuses an armed delete', out, 'BLOCKED');
    check('run never called execute_operation for the delete',
      r.calls.filter(c => c.name === 'execute_operation').length, 0);
  }
  {
    const r = build({ describe_operation: { operation: { operationId: 'update-contact', kind: 'write', domain: 'contacts' } } });
    const out = await r.ghlMcpRunOperation({ operationId: 'update-contact', params: { contactId: 'abc' } }, RON);
    checkIncludes('unarmed write is refused by name', out, 'GHL_MCP_WRITE_ALLOWLIST');
    check('unarmed write never reached execute_operation',
      r.calls.filter(c => c.name === 'execute_operation').length, 0);
  }
  {
    const r = build({
      describe_operation: { operation: { operationId: 'list-transactions', kind: 'read', domain: 'payments' } },
    });
    const out = await r.ghlMcpRunOperation({ operationId: 'list-transactions' }, 'U_SETTER_123');
    checkIncludes('a setter cannot read the payments domain', out, 'Ron-only');
    check('the setter payments read never reached execute_operation',
      r.calls.filter(c => c.name === 'execute_operation').length, 0);
  }

  // ── End to end: an allowed call goes through, dryRun is forwarded ─────────
  {
    const r = build({
      describe_operation: { operation: { operationId: 'get-pipelines', kind: 'read', domain: 'opportunities' } },
      execute_operation: { success: true, status: 200, data: { pipelines: [{ id: 'KH1IQuaN8aNB1lfRpvP4' }] } },
    });
    const out = await r.ghlMcpRunOperation({ operationId: 'get-pipelines' }, 'U_SETTER_123');
    checkIncludes('an ordinary read executes', out, 'KH1IQuaN8aNB1lfRpvP4');
    const exec = r.calls.find(c => c.name === 'execute_operation');
    check('dryRun is omitted when not requested', 'dryRun' in exec.args, false);
    check('auth header carries the PIT', exec.headers.Authorization, 'Bearer pit-test');
    check('locationId header is sent', exec.headers.locationId, 'loc-test');
  }
  {
    const r = build({
      describe_operation: { operation: { operationId: 'edit-appointment', kind: 'write', domain: 'calendars' } },
      execute_operation: { success: true, dryRun: true, resolvedRequest: { method: 'PUT', path: '/calendars/events/appointments/X' } },
    }, { GHL_MCP_WRITE_ALLOWLIST: 'edit-appointment' });
    const out = await r.ghlMcpRunOperation({ operationId: 'edit-appointment', params: { eventId: 'X' }, dryRun: true }, RON);
    checkIncludes('an armed write dry-runs', out, 'resolvedRequest');
    check('dryRun is forwarded when requested',
      r.calls.find(c => c.name === 'execute_operation').args.dryRun, true);
  }

  // ── Cron context (no userId) runs as Ron ──────────────────────────────────
  {
    const r = build({
      describe_operation: { operation: { operationId: 'list-transactions', kind: 'read', domain: 'payments' } },
      execute_operation: { success: true, data: { transactions: [] } },
    });
    const out = await r.ghlMcpRunOperation({ operationId: 'list-transactions' }, null);
    checkIncludes('a null userId (cron) is treated as Ron', out, 'transactions');
  }

  // ── Search shaping ────────────────────────────────────────────────────────
  {
    const r = build({ search_operations: { results: [
      { operationId: 'invoices.list-invoices', kind: 'read', domain: 'invoices', method: 'GET', summary: 'List invoices', hasRequestBody: false, extraNoise: 'x'.repeat(500) },
    ] } });
    const out = await r.ghlMcpFindOperation({ query: 'list invoices' });
    checkIncludes('search returns the operationId', out, 'invoices.list-invoices');
    checkIncludes('search returns hasRequestBody so reads can skip describe', out, 'hasRequestBody');
    check('search drops fields Max does not need', out.includes('extraNoise'), false);
    check('limit is capped at 25', r.calls[0].args.limit, 8);
  }
  {
    const r = build({ search_operations: { results: [], zeroResultReason: 'domain_filtered' } });
    const out = await r.ghlMcpFindOperation({ query: 'refund a payment' });
    checkIncludes('an empty search explains why', out, 'domain_filtered');
  }
  {
    const r = build({});
    check('an empty query is rejected without a call',
      (await r.ghlMcpFindOperation({ query: '  ' })).startsWith('Provide a query'), true);
    check('the rejected query made no HTTP call', r.calls.length, 0);
  }

  // ── Truncation ────────────────────────────────────────────────────────────
  {
    const { ghlMcpTruncate } = build({});
    check('short payloads pass through untouched', ghlMcpTruncate({ a: 1 }), '{"a":1}');
    const big = ghlMcpTruncate({ a: 'x'.repeat(9000) });
    check('an oversized payload is capped', big.length < 6100, true);
    checkIncludes('truncation is announced, not silent', big, '[truncated');
  }

  // ── Transport errors surface, they do not return junk ─────────────────────
  {
    const r = build({ search_operations: () => ({ status: 502, ok: false, text: async () => '' }) });
    await throws('a non-2xx surfaces as an error', () => r.ghlMcpFindOperation({ query: 'x' }), 'HTTP 502');
  }
  {
    const r = build({ search_operations: () => ({
      status: 200, ok: true,
      text: async () => `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: 1, error: { message: 'bad operationId' } })}\n\n`,
    }) });
    await throws('a JSON-RPC error surfaces', () => r.ghlMcpFindOperation({ query: 'x' }), 'bad operationId');
  }
  {
    const r = build({ search_operations: () => ({ status: 200, ok: true, text: async () => 'event: message\ndata: {"result":{"content":[]}}\n\n' }) });
    await throws('an empty content array surfaces', () => r.ghlMcpFindOperation({ query: 'x' }), 'empty content');
  }
  {
    // A missing token must fail loudly rather than firing an unauthenticated request.
    const r = build({ search_operations: {} }, { GHL_API_KEY: undefined });
    await throws('a missing GHL_API_KEY fails before the call', () => r.ghlMcpFindOperation({ query: 'x' }), 'GHL_API_KEY is not set');
  }

  console.log(failures ? `\n${failures} failure(s)` : '\nAll GHL MCP router tests passed.');
  process.exit(failures ? 1 : 0);
})();
