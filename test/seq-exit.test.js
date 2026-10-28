// Wiring test for "exit the sales sequences on a reply" in index.js.
// Run:  node test/seq-exit.test.js
//
// index.js boots Slack on require, so the block is sliced out by its section
// markers and run against a fake GHL and a fake Supabase. Synthetic ids only
// (public repo); the workflow ids are the real ones, they are not secrets.
const fs = require('fs');
const path = require('path');
const sequenceReplies = require('../lib/sequenceReplies');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const START = SRC.indexOf('// ─── EXIT THE SALES SEQUENCES ON A REPLY');
const END = SRC.indexOf('// ─── end exit the sales sequences on a reply');
if (START < 0 || END < 0) { console.error('FAIL exit block markers not found'); process.exit(1); }
const BLOCK = SRC.slice(START, END);
const POSTCALL = 'bcad7c86-2f26-4b16-8c93-fc8acd2f0b4d';
const H = 3600e3, D = 24 * H;
const NOW = Date.parse('2026-10-22T14:00:00Z');

// GHL startTime format: "YYYY-MM-DD HH:mm:ss" in UTC, no offset.
const ghlTime = (ms) => new Date(ms).toISOString().slice(0, 19).replace('T', ' ');

function load({ events = [], apptOk = true, deleteOk = () => true, env = {}, rows = [] } = {}) {
  const deletes = []; const upserts = []; const activity = []; const reads = [];
  const supabase = {
    from: () => ({
      upsert: async (row, opts) => { upserts.push({ row, opts }); return { error: null }; },
      select: () => {
        const q = { eq: () => q, like: () => q, gte: async (col, v) => { reads.push(v); return { data: rows, error: null }; } };
        return q;
      },
    }),
  };
  const deps = {
    process: { env: { GHL_API_KEY: 'k', ...env } },
    sequenceReplies, supabase, RON_SLACK_ID: 'URON',
    logActivity: (r) => activity.push(r),
    parseGhlApptTime: (s) => { const t = Date.parse(String(s || '').replace(' ', 'T') + 'Z'); return Number.isNaN(t) ? 0 : t; },
    ghlFetch: async (url, init = {}) => {
      if (init.method === 'DELETE') {
        const m = url.match(/contacts\/([^/]+)\/workflow\/([^/]+)$/);
        const ok = deleteOk(m[2]);
        deletes.push({ cid: m[1], wid: m[2], ok });
        return { ok, status: ok ? 200 : 400, text: async () => 'nope' };
      }
      return { ok: apptOk, status: apptOk ? 200 : 500, json: async () => ({ events }) };
    },
  };
  const names = Object.keys(deps);
  const api = new Function(...names, `${BLOCK}\nreturn { seqExitOnReply, runSequenceExitSweep, SEQ_EXIT_WORKFLOWS };`)(...names.map(n => deps[n]));
  return { api, deletes, upserts, activity, reads };
}

(async () => {
  {
    const t = load({ events: [{ startTime: ghlTime(NOW - 23 * H), appointmentStatus: 'showed' }] });
    const r = await t.api.seqExitOnReply({ contactId: 'C1', replyAt: NOW - 5 * H, now: NOW });
    check('1a a human reply removes the contact from all four sales sequences', [r, t.deletes.length, t.deletes.every(d => d.cid === 'C1')], ['exited', 4, true]);
    check('1b the post-call workflow is in the list', t.deletes.some(d => d.wid === POSTCALL), true);
    check('1c the exit is recorded for the sweep with the reply time', [t.upserts.length, JSON.parse(t.upserts[0].row.value).replyAt, t.upserts[0].row.key], [1, NOW - 5 * H, 'seq-exit:C1']);
    check('1d the record upserts on category,key (newest reply wins)', t.upserts[0].opts, { onConflict: 'category,key' });
  }
  {
    const t = load({ events: [{ startTime: ghlTime(NOW + 1 * D), appointmentStatus: 'confirmed' }] });
    const r = await t.api.seqExitOnReply({ contactId: 'C1', replyAt: NOW - 1 * H, now: NOW });
    check('2a a reply before a booked call touches nothing', [r, t.deletes.length, t.upserts.length], ['call', 0, 0]);
  }
  {
    const t = load({ apptOk: false });
    const r = await t.api.seqExitOnReply({ contactId: 'C1', replyAt: NOW - 1 * H, now: NOW });
    check('2b appointments unreadable: exits anyway', [r, t.deletes.length], ['exited', 4]);
  }
  {
    const t = load({ deleteOk: (wid) => wid !== POSTCALL });
    const r = await t.api.seqExitOnReply({ contactId: 'C1', replyAt: NOW - 1 * H, now: NOW });
    check('2c one removal fails: partial, still recorded, error logged', [r, t.upserts.length, t.activity[0].status], ['partial', 1, 'error']);
  }
  {
    const t = load({ deleteOk: () => false });
    const r = await t.api.seqExitOnReply({ contactId: 'C1', replyAt: NOW - 1 * H, now: NOW });
    check('2d every removal fails: failed, nothing recorded, never throws', [r, t.upserts.length], ['failed', 0]);
  }
  {
    const t = load({ env: { SEQ_EXIT_DISABLED: 'true' } });
    const r = await t.api.seqExitOnReply({ contactId: 'C1', replyAt: NOW - 1 * H, now: NOW });
    check('2e kill switch: nothing happens', [r, t.deletes.length], ['off', 0]);
  }
  // Sweep
  const rec = (cid, replyAt) => ({ key: `seq-exit:${cid}`, value: JSON.stringify({ cid, replyAt, at: replyAt }) });
  {
    const t = load({ rows: [rec('C1', NOW - 19 * H)], events: [{ startTime: ghlTime(NOW - 42 * H), appointmentStatus: 'showed' }] });
    const r = await t.api.runSequenceExitSweep('corr', { now: NOW });
    check('3a the day after a reply: removed from the post-call workflow only', [t.deletes.length, t.deletes[0].wid, r.stats.held, r.stats.removed], [1, POSTCALL, 1, 1]);
    check('3b reads only records from the last 14 days', t.reads[0], new Date(NOW - 14 * D).toISOString());
  }
  {
    const t = load({ rows: [rec('C1', NOW - 5 * D)], events: [{ startTime: ghlTime(NOW - 2 * D), appointmentStatus: 'showed' }] });
    const r = await t.api.runSequenceExitSweep('corr', { now: NOW });
    check('3c a call held after the reply: left alone', [t.deletes.length, r.stats.new_call], [0, 1]);
  }
  {
    const t = load({ rows: [rec('C1', NOW - 2 * D)], apptOk: false });
    const r = await t.api.runSequenceExitSweep('corr', { now: NOW });
    check('3d appointments unreadable in the sweep: left alone this hour', [t.deletes.length, r.stats.errors, r.ok], [0, 1, false]);
  }
  {
    const t = load({ rows: [{ key: 'seq-exit:bad', value: 'not json' }, rec('C2', NOW - 1 * D)] });
    const r = await t.api.runSequenceExitSweep('corr', { now: NOW });
    check('3e a bad record is counted and skipped, the rest still run', [r.stats.errors, r.stats.removed], [1, 1]);
  }
  {
    const t = load({ env: { SEQ_EXIT_DISABLED: 'true' }, rows: [rec('C1', NOW - 1 * D)] });
    const r = await t.api.runSequenceExitSweep('corr', { now: NOW });
    check('3f kill switch stops the sweep too', [r.disabled, t.deletes.length], [true, 0]);
  }
  check('4a pre-call and webinar reminder workflows are never in the exit list',
    ['3b5f1520-1233-422b-aa4e-c479ce6d3e11', 'e2c1ad4c-d4ef-4f1e-a8c2-f320e9d1871b', 'cf96c723-fa04-4f2e-9d39-699392e81194'].some(w => load().api.SEQ_EXIT_WORKFLOWS.includes(w)), false);
  check('4b no em dash in the block', /\u2014/.test(BLOCK), false);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall sequence exit checks passed');
})().catch(err => { console.error('FAIL unexpected error', err); process.exit(1); });
