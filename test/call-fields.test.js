// Call fields form (lib/callFields.js) + its wiring in index.js.
// Run: node test/call-fields.test.js
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const cf = require('../lib/callFields');

let n = 0;
function t(name, fn) { fn(); n++; console.log(`ok - ${name}`); }

const sel = (v) => ({ v: { selected_option: { value: v } } });
const num = (v) => ({ v: { value: v } });
const date = (v) => ({ v: { selected_date: v } });

t('only follow_up with the flag on offers the form', () => {
  assert.strictEqual(cf.shouldOfferCallFields('follow_up', 'on'), true);
  assert.strictEqual(cf.shouldOfferCallFields('follow_up', ' ON '), true);
  assert.strictEqual(cf.shouldOfferCallFields('follow_up', undefined), false);
  assert.strictEqual(cf.shouldOfferCallFields('follow_up', 'off'), false);
  for (const o of ['won', 'lost', 'disqualified', 'no_show']) assert.strictEqual(cf.shouldOfferCallFields(o, 'on'), false);
});

t('full submission parses', () => {
  const r = cf.parseCallFieldsSubmission({
    industry: sel('asesor'), objection: sel('liquidez'), ticket: num('3000'), clients: num('2'), next_step: date('2026-10-20'),
  });
  assert.deepStrictEqual(r, { ok: true, fields: { industry: 'asesor', objection: 'liquidez', ticket: 3000, clientsMissing: 2, nextStepDate: '2026-10-20' } });
});

t('numbers and date are optional; zero clients is a real answer', () => {
  const r = cf.parseCallFieldsSubmission({ industry: sel('tech'), objection: sel('socio'), ticket: { v: { value: null } }, clients: num('0'), next_step: { v: {} } });
  assert.deepStrictEqual(r.fields, { industry: 'tech', objection: 'socio', ticket: null, clientsMissing: 0, nextStepDate: null });
});

t('bad input returns Slack errors keyed by block_id', () => {
  const r = cf.parseCallFieldsSubmission({ industry: sel('bakery'), objection: {}, ticket: num('0'), clients: num('2.5') });
  assert.strictEqual(r.ok, false);
  assert.deepStrictEqual(Object.keys(r.errors).sort(), ['clients', 'industry', 'objection', 'ticket']);
});

t('tags match the email workflow names', () => {
  assert.deepStrictEqual(cf.callFieldTags({ industry: 'real_estate', objection: 'timing' }), ['industria:real_estate', 'objecion:timing']);
});

t('resubmission removes only the replaced industria/objecion tags', () => {
  const stale = cf.staleCallFieldTags(['Industria:Coach', 'objecion:timing', 'objecion:liquidez', 'personal', 'follow-up'], { industry: 'asesor', objection: 'liquidez' });
  assert.deepStrictEqual(stale, ['industria:coach', 'objecion:timing']);
  assert.deepStrictEqual(cf.staleCallFieldTags(undefined, { industry: 'asesor', objection: 'liquidez' }), []);
});

t('GHL values are email-ready: money, 2x ticket, inaction cost, Spanish date', () => {
  const v = cf.ghlCallFieldValues({ industry: 'coach', objection: 'prueba', ticket: 3000, clientsMissing: 3, nextStepDate: '2026-10-20' });
  assert.strictEqual(v['Llamada Ticket'], '$3,000');
  assert.strictEqual(v['Llamada Valor Mensual'], '$6,000');
  assert.strictEqual(v['Llamada Costo Inaccion'], '$9,000');
  assert.strictEqual(v['Llamada Clientes Faltantes'], '3');
  assert.strictEqual(v['Llamada Siguiente Paso'], 'martes 20 de octubre');
  assert.strictEqual(v['Llamada Industria'], 'coach');
  assert.strictEqual(v['Llamada Objecion'], 'prueba');
});

t('blank answers clear the fields instead of leaving stale numbers', () => {
  const v = cf.ghlCallFieldValues({ industry: 'otro', objection: 'otro', ticket: null, clientsMissing: null, nextStepDate: null });
  for (const k of ['Llamada Ticket', 'Llamada Valor Mensual', 'Llamada Costo Inaccion', 'Llamada Clientes Faltantes', 'Llamada Siguiente Paso']) assert.strictEqual(v[k], '');
  const noTicket = cf.ghlCallFieldValues({ industry: 'otro', objection: 'otro', ticket: null, clientsMissing: 4, nextStepDate: null });
  assert.strictEqual(noTicket['Llamada Costo Inaccion'], '');
  assert.strictEqual(noTicket['Llamada Clientes Faltantes'], '4');
});

t('field ids resolve by name, case-insensitive; missing names reported', () => {
  const { resolved, missing } = cf.resolveGhlFieldIds({ 'Llamada Ticket': '$1', 'Llamada Objecion': 'socio' }, [{ id: 'A', name: 'llamada ticket' }, { id: 'B', name: 'Other' }]);
  assert.deepStrictEqual(resolved, [{ id: 'A', field_value: '$1' }]);
  assert.deepStrictEqual(missing, ['Llamada Objecion']);
});

t('modal round-trips meta and prefills a previous answer', () => {
  const meta = { appointment_id: 'appt-1', prospect_name: 'Ana', closer_email: 'c@x.io', channel: 'D1', ts: '1.2' };
  const view = cf.buildCallFieldsModal({ meta, prospectName: 'Ana', prefill: { industry: 'tech', objection: 'socio', ticket: 2500, clientsMissing: 0, nextStepDate: '2026-11-02' } });
  assert.strictEqual(view.callback_id, cf.SUBMIT_CALLBACK_ID);
  assert.deepStrictEqual(cf.parseMeta(view.private_metadata), meta);
  const byId = Object.fromEntries(view.blocks.filter((b) => b.block_id).map((b) => [b.block_id, b.element]));
  assert.strictEqual(byId.industry.initial_option.value, 'tech');
  assert.strictEqual(byId.objection.initial_option.value, 'socio');
  assert.strictEqual(byId.ticket.initial_value, '2500');
  assert.strictEqual(byId.clients.initial_value, '0');
  assert.strictEqual(byId.next_step.initial_date, '2026-11-02');
  const fresh = cf.buildCallFieldsModal({ meta, prospectName: 'Ana' });
  assert.ok(fresh.blocks.every((b) => !b.element || (!b.element.initial_option && !b.element.initial_value && !b.element.initial_date)));
});

t('button message carries meta under the Slack value limit', () => {
  const meta = { appointment_id: '00000000-0000-0000-0000-000000000000', prospect_name: 'x'.repeat(80), closer_email: 'someone@neurogrowth.io', channel: 'D0123456789', ts: '1760000000.123456' };
  const msg = cf.buildCallFieldsButtonMessage({ prospectName: meta.prospect_name, meta });
  const btn = msg.blocks[1].elements[0];
  assert.strictEqual(btn.action_id, cf.OPEN_ACTION_ID);
  assert.ok(btn.value.length < 2000);
  assert.deepStrictEqual(cf.parseMeta(btn.value), meta);
  assert.strictEqual(cf.parseMeta('not json'), null);
  assert.strictEqual(cf.parseMeta('{}'), null);
});

t('option lists stay in sync with the migration CHECK constraints', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'migrations', '022_call_fields.sql'), 'utf8');
  const listIn = (col) => sql.match(new RegExp(`${col}\\s+IN \\(([^)]+)\\)`))[1].split(',').map((s) => s.trim().replace(/'/g, ''));
  assert.deepStrictEqual(listIn('industry'), cf.INDUSTRIES.map((o) => o.value));
  assert.deepStrictEqual(listIn('objection'), cf.OBJECTIONS.map((o) => o.value));
});

t('index.js offers the form after every card-logged outcome and registers both handlers', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  const report = src.slice(src.indexOf('async function reportOutcomeCardResult'), src.indexOf('async function handleOutcomeProposalReaction'));
  assert.ok(/offerCallFieldsForm\(\{ channel, ts, payload, outcome \}\)/.test(report), 'reaction/✅ path offers the form');
  const typed = src.slice(src.indexOf('async function handleOutcomeCardThreadReply'), src.indexOf('async function reportOutcomeCardResult'));
  assert.ok(/offerCallFieldsForm\(\{ channel: message\.channel, ts: message\.thread_ts, payload, outcome: parsed\.outcome \}\)/.test(typed), 'typed path offers the form');
  assert.ok(src.includes('slack.action(callFields.OPEN_ACTION_ID'));
  assert.ok(src.includes('slack.view(callFields.SUBMIT_CALLBACK_ID'));
  // The writer role has no UPDATE on revops_sales_outcomes: the RPC is the only way in.
  const block = src.slice(src.indexOf('// ─── CALL FIELDS FORM'), src.indexOf('// Shared tail of every reaction-driven outcome log'));
  assert.ok(block.includes('ng_set_outcome_call_fields('));
  assert.ok(!/UPDATE\s+revops_sales_outcomes/i.test(block));
});

t('every objection maps onto the dash picklist the portal RPC enforces', () => {
  const DASH = ['price', 'timing', 'authority', 'need_fit', 'trust', 'competitor', 'implementation', 'none', 'other'];
  for (const o of cf.OBJECTIONS) assert.ok(DASH.includes(cf.PORTAL_OBJECTION[o.value]), o.value);
  const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  assert.ok(src.includes('callFields.PORTAL_OBJECTION[fields.objection]'), 'portal write maps the objection');
});

console.log(`\n${n} passed`);
