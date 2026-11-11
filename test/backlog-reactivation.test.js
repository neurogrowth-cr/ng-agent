// Rules test for the backlog reactivation (WhatsApp templates).  Run:  node test/backlog-reactivation.test.js
// Spec: ~/automations/ops/recipes/backlog-reactivation-whatsapp.md. Every name,
// phone and email is made up (public repo).
const br = require('../lib/backlogReactivation');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const DAY = 864e5;
const NOW = Date.parse('2026-11-06T15:30:00.000Z');
const S = br.CONFIG.stages;
let n = 0;
const card = (over = {}, ct = {}) => ({
  id: `opp${++n}`, contactId: over.contactId || `ct${n}`, pipelineId: br.CONFIG.pipelineId, status: 'open',
  pipelineStageId: S.STRIKE_1, lastStageChangeAt: new Date(NOW - 40 * DAY).toISOString(), ...over,
  contact: { phone: '+50600000000', email: `p${n}@example.test`, tags: [], ...ct },
});

// Selection
{
  const r = br.selectBacklog([
    card({ lastStageChangeAt: new Date(NOW - 90 * DAY).toISOString(), contactId: 'old' }),
    card({ lastStageChangeAt: new Date(NOW - 20 * DAY).toISOString(), contactId: 'fresh' }),
    card({ lastStageChangeAt: new Date(NOW - 50 * DAY).toISOString(), contactId: 'mid' }),
  ], { now: NOW, limit: 2 });
  check('newest silence first, capped at the daily limit', r.picked.map((o) => o.contactId), ['fresh', 'mid']);
  check('eligible counts everything that could go', r.eligible, 3);
}
{
  const clients = new Set(['client@example.test']);
  const r = br.selectBacklog([
    card({}, { phone: '' }),
    card({ lastStageChangeAt: new Date(NOW - 5 * DAY).toISOString() }),
    card({ pipelineStageId: '93de6a09-78a4-4253-bea4-c1528ed6f2b3' }), // New Lead
    card({ pipelineStageId: '8c01f7d1-bfe4-432d-8791-b85e24372ff0' }), // Hot Strike
    card({}, { tags: ['reactivacion-backlog'] }),
    card({}, { tags: ['Reactivacion-Cerrada'] }),
    card({}, { tags: ['personal'] }),
    card({}, { tags: ['guardian-off'] }),
    card({}, { email: 'client@example.test' }),
    card({ status: 'lost' }),
    card({ contactId: 'dup' }), card({ contactId: 'dup' }),
  ], { now: NOW, limit: 100, clientEmails: clients });
  check('only the one valid duplicate goes', r.picked.length, 1);
  check('skip reasons are counted', r.skips, {
    no_phone: 1, recent: 1, stage_out_of_scope: 2, already_enrolled: 2, personal: 1, blocked_tag: 1, client: 1, not_open: 1, duplicate_or_empty: 1,
  });
}
check('all four stages are in scope', [S.INITIAL_CONTACT, S.STRIKE_1, S.STRIKE_2, S.STRIKE_3].map((s) => br.selectBacklog([card({ pipelineStageId: s })], { now: NOW, limit: 5 }).picked.length), [1, 1, 1, 1]);

// Health
{
  const at = NOW - 30 * 60 * 60 * 1000; // tagged 30 h ago
  const ids = Array.from({ length: 25 }, (_, i) => `h${i}`);
  const enrolled = ids.map((contactId) => ({ contactId, at }));
  const allSent = new Map(ids.map((id) => [id, { at: at + 3600e3, status: 'read' }]));
  check('all delivered is green', br.evaluateHealth(enrolled, allSent, NOW).level, 'green');

  const someFailed = new Map(allSent); ['h0', 'h1', 'h2'].forEach((id) => someFailed.set(id, { at, status: 'failed' }));
  check('12% failed is yellow', br.evaluateHealth(enrolled, someFailed, NOW).level, 'yellow');

  const manyFailed = new Map(allSent); ids.slice(0, 6).forEach((id) => manyFailed.set(id, { at, status: 'failed' }));
  check('24% failed is red', br.evaluateHealth(enrolled, manyFailed, NOW).level, 'red');

  const missing = new Map(allSent); ['h0', 'h1'].forEach((id) => missing.delete(id));
  check('8% with no strike 1 after a day is red', br.evaluateHealth(enrolled, missing, NOW).level, 'red');

  check('a contact tagged 2 h ago is not judged yet', br.evaluateHealth([{ contactId: 'x', at: NOW - 2 * 3600e3 }], new Map(), NOW).checked, 0);
  check('below warmup, a few failures stay green', br.evaluateHealth([{ contactId: 'a', at }, { contactId: 'b', at }], new Map([['a', { at, status: 'failed' }], ['b', { at, status: 'read' }]]), NOW).level, 'green');
  check('below warmup, nothing sent at all is red', br.evaluateHealth([{ contactId: 'a', at }, { contactId: 'b', at }], new Map(), NOW).level, 'red');
}

// Verdict line
{
  const green = br.verdictLine({ mode: 'live', picked: 100, eligible: 1770, health: { level: 'green', checked: 0 }, paused: null });
  check('green line starts with the verdict and has the real numbers', /^✅ ALL GREEN · .*100 etiquetados hoy, 1670 pendientes/.test(green), true);
  const pausedLine = br.verdictLine({ mode: 'live', picked: 0, eligible: 0, health: { level: 'red', checked: 25, delivered: 19, failed: 6, missing: 0, reasons: ['24% de plantillas fallidas'] }, paused: '24% de plantillas fallidas' });
  check('a pause says nobody was tagged', /^⚠️ ISSUES · .*en pausa.*No se etiquetó a nadie hoy\./.test(pausedLine), true);
  check('no em dash in any line', /—/.test(green + pausedLine), false);
}

// Situation pool (Ron, 2026-11-10): the thread decides the message.
{
  const msg = (direction, body, i, type = 'TYPE_WHATSAPP') => ({ direction, body, messageType: type, dateAdded: new Date(NOW - (100 - i) * 3600e3).toISOString() });
  const auto = (b) => /gracias por comunicarte/i.test(b);
  check('no reply from the lead is sin_respuesta', br.ruleSituation([msg('outbound', 'Hola', 1)]), 'sin_respuesta');
  check('only a business auto-reply is still sin_respuesta', br.ruleSituation([msg('outbound', 'Hola', 1), msg('inbound', 'Gracias por comunicarte con Ferretería Ana', 2)], { isAutomatedBody: auto }), 'sin_respuesta');
  check('a price question is pregunto_precio', br.ruleSituation([msg('inbound', 'Info', 1), msg('outbound', 'Claro', 2), msg('inbound', 'Y cuánto cuesta?', 3)]), 'pregunto_precio');
  check('anything else goes to the model', br.ruleSituation([msg('inbound', 'Tengo una agencia de seguros en Heredia', 1)]), null);
  check('activity rows never count as the lead', br.ruleSituation([{ direction: 'inbound', body: 'Opportunity created', messageType: 'TYPE_ACTIVITY_OPPORTUNITY', dateAdded: new Date().toISOString() }]), 'sin_respuesta');
  check('model answer parsed', br.parseSituation('conto_negocio'), 'conto_negocio');
  check('model answer with noise parsed', br.parseSituation(' Etiqueta: respondio_poco.'), 'respondio_poco');
  check('unknown model answer is null, never a guess', br.parseSituation('no sé'), null);
  const prompt = br.buildSituationPrompt([msg('outbound', 'Hola, a qué se dedica?', 1), msg('inbound', 'Vendo seguros', 2)]);
  check('prompt carries the thread with roles', /NOSOTROS: Hola, a qué se dedica\?\nPROSPECTO: Vendo seguros/.test(prompt), true);
  check('prompt has no em dash', /—/.test(prompt), false);
  const dry = br.verdictLine({ mode: 'dry_run', picked: 100, eligible: 1583, health: { level: 'green', checked: 0 }, paused: null, situations: { sin_respuesta: 25, respondio_poco: 19, conto_negocio: 20, pregunto_precio: 36 }, closed: 3 });
  check('dry run says what it would do, not what it did', /se etiquetarían 100 hoy, 1483 pendientes\. Situación: sin_respuesta 25, respondio_poco 19, conto_negocio 20, pregunto_precio 36\. Se moverían a Lost: 3\./.test(dry), true);
  check('live run says what it did', /100 etiquetados hoy/.test(br.verdictLine({ mode: 'live', picked: 100, eligible: 200, health: { level: 'green', checked: 0 }, paused: null })), true);
}

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
