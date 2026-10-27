// Wiring test for the Max backlog I/O half in index.js.  Run:  node test/max-backlog-io.test.js
//
// index.js boots Slack on require, so the backlog block is sliced out by its
// section markers and run against an in-memory Supabase, a fake Haiku and a
// fake Slack. Proves capture -> dedupe -> digest end to end, and that the
// digest fails closed. Synthetic data only (public repo).
const fs = require('fs');
const path = require('path');
const maxBacklog = require('../lib/maxBacklog');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const START = SRC.indexOf('// ─── MAX SELF-IMPROVEMENT BACKLOG');
const END = SRC.indexOf('// ─── end Max self-improvement backlog');
if (START < 0 || END < 0) { console.error('FAIL backlog block markers not found in index.js'); process.exit(1); }
const BLOCK = SRC.slice(START, END);

// ── In-memory Supabase ──────────────────────────────────────────────────────
function makeDb(tables, { failReads = false } = {}) {
  let nextId = 1;
  function from(name) {
    const rows = tables[name] || (tables[name] = []);
    const filters = []; let op = 'select'; let payload = null; let single = false; let headCount = false;
    const q = {
      select(_cols, opts) { if (opts && opts.head) headCount = true; return q; },
      eq(c, v) { filters.push(r => r[c] === v); return q; },
      in(c, vs) { filters.push(r => vs.includes(r[c])); return q; },
      gte(c, v) { filters.push(r => String(r[c]) >= String(v)); return q; },
      order() { return q; },
      limit() { return q; },
      maybeSingle() { single = true; return q; },
      insert(row) { op = 'insert'; payload = row; return q; },
      update(patch) { op = 'update'; payload = patch; return q; },
      then(resolve, reject) {
        try {
          if (failReads && op === 'select') return resolve({ data: null, error: { message: 'boom' } });
          if (op === 'insert') {
            if (name === 'max_backlog' && rows.some(r => r.slug === payload.slug)) return resolve({ error: { message: 'duplicate key value violates unique constraint' } });
            rows.push({ id: nextId++, ...payload }); return resolve({ error: null });
          }
          const hit = rows.filter(r => filters.every(f => f(r)));
          if (op === 'update') { hit.forEach(r => Object.assign(r, payload)); return resolve({ error: null }); }
          if (headCount) return resolve({ count: hit.length, error: null });
          return resolve({ data: single ? (hit[0] || null) : hit, error: null });
        } catch (e) { reject(e); }
      },
    };
    return q;
  }
  return { from };
}

function load({ tables = {}, portalTables = {}, haiku = '{"kind":"none"}', failReads = false, env = {} } = {}) {
  const posts = []; const activity = []; const llm = [];
  const deps = {
    process: { env },
    supabase: makeDb(tables, { failReads }),
    portalSupabase: makeDb(portalTables),
    maxBacklog,
    containsSensitiveData: (t) => /password|api.?key/i.test(String(t || '')),
    logActivity: (row) => activity.push(row),
    logLlmFromAnthropicResponse: () => {},
    anthropic: { messages: { create: async (req) => { llm.push(req); return { content: [{ type: 'text', text: haiku }] }; } } },
    MODEL_LIGHT: 'haiku', THINKING_OFF: { type: 'disabled' },
    slack: { client: { chat: { getPermalink: async ({ channel, message_ts }) => ({ permalink: `https://slack.test/${channel}/${message_ts}` }) } } },
    postToSlack: async (channel, text) => { posts.push({ channel, text }); return { channel, ts: '1' }; },
    AGENT_CHANNEL: '#ng-pm-agent', RON_SLACK_ID: 'URON',
  };
  const names = Object.keys(deps);
  const api = new Function(...names, `${BLOCK}\nreturn { recordBacklogItem, noteGapFromReply, captureGapFromReply, syncMaxBacklogFromActivity, getMaxBacklogStats, runMaxBacklogDigest, MAX_BACKLOG_ENABLED };`)(...names.map(n => deps[n]));
  return { api, tables, posts, activity, llm };
}

const GAP = JSON.stringify({ kind: 'capability_gap', slug: '', topic: 'eliminar-contacto-ghl', title: 'Eliminar contactos de GHL', summary: 'Pidieron borrar un contacto y Max no tiene herramienta de borrado.' });
const REFUSAL = 'No tengo autorización para eliminar contactos de GHL sin que Ron lo apruebe.';
const FIRST_MONDAY = new Date('2026-11-02T15:00:00Z');

(async () => {
  // ── 1. Capture from a reply ──────────────────────────────────────────────
  {
    const t = load({ haiku: GAP });
    const r1 = await t.api.captureGapFromReply({ replyText: REFUSAL, userText: 'borra a X', userId: 'USEB', channel: 'C1', messageTs: '111.1', correlationId: 'c1' });
    const r2 = await t.api.captureGapFromReply({ replyText: REFUSAL, userText: 'borra a Y', userId: 'USEB', channel: 'C1', messageTs: '222.2', correlationId: 'c2' });
    const row = t.tables.max_backlog[0];
    check('1a first refusal inserts, second identical one updates', [r1, r2], ['inserted', 'updated']);
    check('1b one row, two occurrences, permalink of the first ask',
      [t.tables.max_backlog.length, row.occurrences, row.slug, row.permalink, row.requested_by],
      [1, 2, 'gap:eliminar-contacto-ghl', 'https://slack.test/C1/111.1', 'USEB']);
    check('1c the second Haiku call was shown the open slug', t.llm[1].messages[0].content.includes('- gap:eliminar-contacto-ghl:'), true);
    check('1d Haiku runs with thinking off', t.llm[0].thinking, { type: 'disabled' });
  }
  {
    const t = load({ haiku: GAP });
    const r = await t.api.captureGapFromReply({ replyText: 'Hubo 14 llamadas esta semana.', userText: 'cuántas llamadas?' });
    check('1e a normal answer never calls Haiku', [r, t.llm.length, (t.tables.max_backlog || []).length], [null, 0, 0]);
  }
  {
    const t = load({ haiku: '{"kind":"none"}' });
    const r = await t.api.captureGapFromReply({ replyText: REFUSAL, userText: 'x' });
    check('1f Haiku says none: nothing recorded', [r, (t.tables.max_backlog || []).length], [null, 0]);
  }
  {
    const t = load({ haiku: GAP, env: { MAX_BACKLOG_DISABLED: 'true' } });
    const r = await t.api.recordBacklogItem({ kind: 'tool_error', slug: 'tool_error:x', title: 'x' });
    check('1g kill switch skips writes', [t.api.MAX_BACKLOG_ENABLED, r, (t.tables.max_backlog || []).length], [false, 'skipped', 0]);
  }
  {
    const t = load();
    await t.api.recordBacklogItem({ kind: 'tool_error', slug: 'tool_error:x', title: 'Herramienta x falló', summary: 'invalid api_key sk-zzz', times: ['2026-10-30T00:00:00Z'] });
    check('1h a sensitive summary is dropped, not stored', t.tables.max_backlog[0].summary, null);
  }
  {
    const t = load({ failReads: true });
    const r = await t.api.recordBacklogItem({ kind: 'tool_error', slug: 'tool_error:x', title: 'x' });
    check('1i a read error is logged, never thrown', [r, t.activity.some(a => a.status === 'error' && a.action === 'backlog_record')], ['error', true]);
  }

  // ── 2. Weekly sync from activity and lessons ─────────────────────────────
  {
    const recent = new Date(Date.now() - 2 * 86400000).toISOString();
    const portalTables = { agent_activity: [
      { agent: 'max', event_type: 'cron_run', action: 'runFoo', status: 'error', error_message: 'statement timeout', created_at: recent },
      { agent: 'kai', event_type: 'cron_run', action: 'runKaiThing', status: 'error', created_at: recent },
    ] };
    const tables = { agent_knowledge: [
      { category: 'process', visibility: 'shared', source: 'correction', key: 'correction:closer-email:2026-10-30', value: 'Use the company domain.', user_id: 'U1', updated_at: recent },
    ] };
    const t = load({ tables, portalTables });
    const first = await t.api.syncMaxBacklogFromActivity('c');
    const again = await t.api.syncMaxBacklogFromActivity('c');
    check('2a Max failures and corrections become rows, Kai rows do not',
      t.tables.max_backlog.map(r => r.slug).sort(), ['check_broken:runfoo', 'correction:closer-email']);
    check('2b re-reading the same window counts nothing twice', [first.results, again.results], [{ inserted: 2 }, { unchanged: 2 }]);
    const stats = await t.api.getMaxBacklogStats(new Date(Date.now() - 7 * 86400000).toISOString());
    check('2c brief stats count new and open rows', stats, { newThisWeek: 2, openTotal: 2 });
  }

  // ── 3. Monthly digest ────────────────────────────────────────────────────
  {
    const t = load({ tables: { max_backlog: [
      { slug: 'gap:a', kind: 'capability_gap', title: 'Eliminar contactos de GHL', occurrences: 3, last_seen: '2026-10-30', status: 'open', requested_by: 'USEB', permalink: 'https://slack.test/p' },
      { slug: 'gap:b', kind: 'capability_gap', title: 'Ya resuelto', occurrences: 9, last_seen: '2026-10-30', status: 'done' },
    ] } });
    await t.api.runMaxBacklogDigest('c', { now: new Date('2026-11-09T15:00:00Z') });
    check('3a second Monday: no post', t.posts.length, 0);
    await t.api.runMaxBacklogDigest('c', { now: FIRST_MONDAY });
    check('3b first Monday: one post to #ng-pm-agent', t.posts.map(p => p.channel), ['#ng-pm-agent']);
    check('3c the post opens with the header and lists only open rows',
      [t.posts[0].text.split('\n')[0], t.posts[0].text.includes('Ya resuelto')], ['`MAX BACKLOG: NOVIEMBRE 2026` <@URON>', false]);
  }
  {
    const t = load({ tables: { max_backlog: [] } });
    await t.api.runMaxBacklogDigest('c', { now: FIRST_MONDAY });
    check('3d nothing open: no post, logged as skipped',
      [t.posts.length, t.activity.some(a => a.output && a.output.skipped === 'nothing open')], [0, true]);
  }
  {
    const t = load({ failReads: true });
    let threw = false;
    try { await t.api.runMaxBacklogDigest('c', { now: FIRST_MONDAY }); } catch (_) { threw = true; }
    check('3e a read error posts CHECK BROKEN and throws (wrapCronJob logs it)',
      [threw, t.posts.length, /CHECK BROKEN/.test(t.posts[0] && t.posts[0].text)], [true, 1, true]);
  }

  // ── 4. index.js wiring (source checks) ───────────────────────────────────
  check('4a the digest cron is declared to the liveness audit', /runMaxBacklogDigest:\s+'0 9 \* \* 1'/.test(SRC), true);
  check('4b the digest cron is scheduled and wrapped', /wrapCronJob\('runMaxBacklogDigest'/.test(SRC), true);
  check('4c all three reply paths note gaps', (SRC.match(/noteGapFromReply\(\{ replyText: reply/g) || []).length, 3);
  check('4d the weekly brief carries the section', SRC.includes("'`MAX SELF-IMPROVEMENT`', backlogBlock"), true);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall max-backlog io checks passed');
})().catch(err => { console.error('FAIL unexpected error', err); process.exit(1); });
