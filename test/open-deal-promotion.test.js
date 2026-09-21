// Behaviour test for the open-deal promotion write and the card retry rule.
//   Run:  node test/open-deal-promotion.test.js
//
// Built 2026-09-20 after Ron tapped Lost on an open-deal card and got
// "bind message supplies 4 parameters, but prepared statement requires 5".
// Three defects sat behind that one tap:
//   1. promoteOpenDealOutcome never passed `source`, so every promotion failed
//      before reaching the database.
//   2. The failure stamped the card with a warning, and the tap handler read
//      that stamp as "already actioned", so the card ignored every later tap.
//   3. With 1 fixed, the write reached Postgres and was refused: "permission
//      denied for table revops_sales_outcomes". The max_outcome_writer role can
//      INSERT outcomes but deliberately cannot UPDATE them. The promotion now
//      goes through the portal RPC ng_promote_open_deal_outcome.
//
// A fake client cannot see database permissions, which is how the fix for 1
// shipped as "verified" and failed on the next tap. So section 5 pins the rule
// statically: Max never sends a raw UPDATE or DELETE at the outcomes table.
//
// Same extraction trick as the other tests: functions are sliced out of
// index.js and compiled with new Function, so this cannot drift from shipped
// behaviour.
const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
function slice(startMarker, endMarker) {
  const s = SRC.indexOf(startMarker);
  const e = SRC.indexOf(endMarker, s);
  if (s < 0 || e < 0) { console.error(`FAIL: could not extract ${startMarker}`); process.exit(1); }
  return SRC.slice(s, e);
}

// ── promoteOpenDealOutcome against a fake pg client ──────────────────────────
const promoSrc = slice('async function promoteOpenDealOutcome', '// One snooze: push snoozeUntil out');

function promoHarness({ updateRowCount = 1, throwOnUpdate = null } = {}) {
  const queries = [];
  let released = false;
  const client = {
    query: async (sql, params) => {
      queries.push({ sql: String(sql), params: params || [] });
      if (/ng_promote_open_deal_outcome\(/.test(sql)) {
        if (throwOnUpdate) throw new Error(throwOnUpdate);
        // The RPC always returns one row: the outcome id, or NULL when the row
        // is no longer follow_up.
        return { rowCount: 1, rows: [{ id: updateRowCount ? 'outcome-1' : null }] };
      }
      if (/^\s*SELECT outcome, source/.test(sql)) return { rows: [{ outcome: 'lost', source: 'ghl' }] };
      if (/^\s*SELECT p\.id/.test(sql)) return { rows: [{ id: 'prospect-1', status: 'nurture', ghl_opportunity_id: 'opp-1', ghl_contact_id: 'c-1' }] };
      return { rowCount: 0, rows: [] };
    },
    release: () => { released = true; },
  };
  const portalWriterPg = { connect: async () => client };
  const fn = new Function(
    'portalWriterPg', 'evaluateOutcomePromotion', 'nextProspectStatusForOutcome',
    'ghlFindSalesOpportunityByContact', 'ghlMoveOpportunityForOutcome',
    `${promoSrc}; return promoteOpenDealOutcome;`
  )(
    portalWriterPg,
    () => ({ ok: true }),
    (outcome) => outcome,
    async () => 'opp-1',
    async () => ({ ok: true, label: 'Lost' }),
  );
  return { fn, queries, wasReleased: () => released };
}

// ── handleOpenDealFollowupReaction against stamped cards ─────────────────────
const handlerSrc = slice('async function handleOpenDealFollowupReaction', '// A typed reply in an open-deal card');
const BOT = 'UBOT';

function handlerHarness() {
  const calls = [];
  const fn = new Function(
    'CLOSER_SLACK', 'RON_SLACK_ID', 'process', 'OPEN_DEAL_SNOOZE_EMOJI', 'EMOJI_TO_OUTCOME',
    'OPEN_DEAL_PROMOTABLE', 'OUTCOME_EMOJI', 'OPEN_DEAL_SNOOZE_DAYS', 'applyOpenDealSnooze',
    'promoteOpenDealOutcome', 'reportOpenDealCardResult', 'slack', 'console',
    `${handlerSrc}; return handleOpenDealFollowupReaction;`
  )(
    { 'jose@x.io': 'UJOSE' }, 'URON', { env: { SLACK_BOT_USER_ID: BOT } }, 'zzz',
    { '-1': 'lost' }, new Set(['lost', 'disqualified', 'won']), { lost: { char: '👎' } }, 7,
    async () => { calls.push('snooze'); },
    async (args) => { calls.push(`promote:${args.outcome}:${args.source}`); return { ok: true }; },
    async () => { calls.push('report'); },
    { client: { chat: { postMessage: async () => { calls.push('explain'); } } } },
    { log() {}, error() {} },
  );
  return { fn, calls };
}
const tap = { user: 'URON', item: { channel: 'D1', ts: '1.1' } };
const payload = { appointment_id: 'appt-1', closer_email: 'jose@x.io', prospect_name: 'Kyokushin Kenbukai Costa Rica' };
const stamped = (name, user) => ({ reactions: [{ name, users: [user] }] });

(async () => {
  console.log('promoteOpenDealOutcome: the RPC call binds what it declares');
  let h = promoHarness();
  let res = await h.fn({ appointmentId: 'appt-1', outcome: 'lost', source: 'closer', notes: 'tapped' });
  // If the RPC call is missing, say so plainly and let the dependent checks
  // fail on an empty query rather than crash with a stack trace.
  const found = h.queries.find(q => /ng_promote_open_deal_outcome\(/.test(q.sql));
  check('1 the promotion calls the portal RPC, not a raw write', !!found, true);
  const upd = found || { sql: '', params: [] };
  const maxPh = Math.max(0, ...[...upd.sql.matchAll(/\$(\d+)/g)].map(x => Number(x[1])));
  check('1a as many values as placeholders (this was 4 against 5)', upd.params.length, maxPh);
  check('1b values arrive in placeholder order, source in $3', upd.params, ['appt-1', 'lost', 'closer', 'tapped', null]);
  check('1c a tap with no revenue leaves closed_revenue alone (null into COALESCE)', upd.params[4], null);
  check('1d the promotion reports ok with the GHL move', [res.ok, res.outcomeId, res.stageMove && res.stageMove.label], [true, 'outcome-1', 'Lost']);
  check('1e the write commits', h.queries.some(q => q.sql === 'COMMIT'), true);
  check('1f the connection goes back to the pool', h.wasReleased(), true);

  h = promoHarness();
  await h.fn({ appointmentId: 'appt-1', outcome: 'won', source: 'closer', notes: 'typed', closedRevenue: '3500' });
  check('1g a typed win carries its revenue as a number in $5', (h.queries.find(q => /ng_promote_open_deal_outcome\(/.test(q.sql)) || { params: [] }).params[4], 3500);

  console.log('promoteOpenDealOutcome: the safety story still holds');
  check('2a the call matches the RPC signature (uuid, text, text, text, numeric)', upd.sql.replace(/\s+/g, ' '), 'SELECT ng_promote_open_deal_outcome($1::uuid, $2::text, $3::text, $4::text, $5::numeric) AS id');
  h = promoHarness({ updateRowCount: 0 });
  res = await h.fn({ appointmentId: 'appt-1', outcome: 'lost', source: 'closer', notes: 'tapped' });
  check('2b a NULL from the RPC (someone else resolved it) is not_promotable, never an error', [res.ok, res.reason, res.existing], [false, 'not_promotable', { outcome: 'lost', source: 'ghl' }]);
  check('2c and that path rolls back', h.queries.some(q => q.sql === 'ROLLBACK'), true);

  h = promoHarness({ throwOnUpdate: 'bind message supplies 4 parameters, but prepared statement "" requires 5' });
  res = await h.fn({ appointmentId: 'appt-1', outcome: 'lost', source: 'closer', notes: 'tapped' });
  check('2d a database error surfaces as a message, rolls back, releases', [res.ok, res.reason, /bind message/.test(res.message), h.queries.some(q => q.sql === 'ROLLBACK'), h.wasReleased()], [false, 'error', true, true, true]);

  console.log('handleOpenDealFollowupReaction: a failed card stays answerable');
  let t = handlerHarness();
  await t.fn(tap, '-1', stamped('warning', BOT), payload);
  check('3a a card stamped with a warning still accepts the tap', t.calls, ['promote:lost:closer', 'report']);

  t = handlerHarness();
  await t.fn(tap, '-1', stamped('white_check_mark', BOT), payload);
  check('3b a card stamped with a check mark is closed', t.calls, []);

  t = handlerHarness();
  await t.fn(tap, '-1', stamped('white_check_mark', 'UJOSE'), payload);
  check('3c a check mark from a human is not Max\'s stamp', t.calls, ['promote:lost:closer', 'report']);

  t = handlerHarness();
  await t.fn({ ...tap, user: 'USTRANGER' }, '-1', { reactions: [] }, payload);
  check('3d a tap from someone who is not the closer or Ron is ignored', t.calls, []);

  console.log('stamps: success clears a stale warning, and the proposal card follows the same rule');
  const reporter = slice('async function reportOpenDealCardResult', '// A reaction on an open-deal card.');
  check('4a a resolved open-deal card clears its warning (both success branches)', (reporter.match(/await clearFailedWriteStamp\(channel, ts\);/g) || []).length, 2);
  const proposalHandler = slice('async function handleOutcomeProposalReaction', 'const tappedOutcome =');
  check('4b the outcome-proposal card no longer treats a warning as actioned', /\['white_check_mark', 'no_entry'\]\.includes/.test(proposalHandler), true);
  check('4c and a dismissed proposal card still stays dismissed', /'no_entry'/.test(proposalHandler), true);

  console.log('permissions: Max never sends a write its database role cannot run');
  // max_outcome_writer holds INSERT + SELECT on revops_sales_outcomes and
  // nothing else. Any raw UPDATE or DELETE is refused at runtime, and no test
  // with a fake client will ever notice. Changing an existing outcome row goes
  // through a guarded portal RPC, or it does not happen.
  const rawWrites = [...SRC.matchAll(/\b(UPDATE\s+(?:public\.)?revops_sales_outcomes|DELETE\s+FROM\s+(?:public\.)?revops_sales_outcomes)\b/gi)]
    .map(m => `line ${SRC.slice(0, m.index).split('\n').length}: ${m[1].replace(/\s+/g, ' ')}`)
    .filter(hit => {
      // Comments may name the statement (this file's own history does).
      const lineNo = Number(hit.match(/^line (\d+)/)[1]);
      return !/^\s*\/\//.test(SRC.split('\n')[lineNo - 1]);
    });
  check('5a no raw UPDATE or DELETE against revops_sales_outcomes anywhere in index.js', rawWrites, []);
  check('5b the promotion goes through the guarded RPC', /ng_promote_open_deal_outcome\(\$1::uuid/.test(promoSrc), true);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nAll open-deal-promotion tests passed.');
})();
