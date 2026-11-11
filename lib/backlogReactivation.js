// Backlog reactivation over WhatsApp templates (plan-of-record UPDATE 2026-10-29,
// scope and channel ruled by Ron 2026-11-04/05). Pure: no I/O, so the rules are
// tested without GHL. index.js reads GHL, calls these, and writes the tags.
//
// Max picks up to N silent Appointment Setting cards a weekday and tags the
// contact `reactivacion-backlog`; the GHL workflow "Reactivación backlog ·
// WhatsApp (3 strikes)" sends the three approved templates and tags
// `reactivacion-cerrada` at the end. A reply stops the workflow (Stop on
// response) and the guardian takes the lead. Spec:
// ~/automations/ops/recipes/backlog-reactivation-whatsapp.md

const DAY = 24 * 60 * 60 * 1000;

const CONFIG = {
  enrolTag: 'reactivacion-backlog',
  closedTag: 'reactivacion-cerrada',
  minSilenceDays: 14,
  // Only these stages (Ron, 2026-11-05): Initial Contact, Strike 1, 2, 3.
  stages: {
    INITIAL_CONTACT: '4b936528-794e-40ab-812d-144b9d5e8128',
    STRIKE_1: '92245916-0622-4f46-aabc-6091b8af5fc0',
    STRIKE_2: '6274c958-8252-4698-acc6-c6818d43a99f',
    STRIKE_3: 'e639662d-6b1b-42b5-a89d-7ebd70ca97e3',
  },
  pipelineId: 'KH1IQuaN8aNB1lfRpvP4',
  lostStageId: '8eff3fbb-3cc0-474f-bbe7-df4704e0a668',
  // GHL contact field "Reactivación · situación" (single option). The workflow
  // branches on it to pick the template; Max writes it before tagging.
  situationFieldId: 'gV0AMCwTirvQ4WqUGIwz',
  // Contacts that never enter, besides clients and the personal tag.
  blockTags: ['guardian-off', 'cliente', 'won-deal', 'no-fit'],
  // Health (spec 4b). A strike 1 that never shows up in the thread a full day
  // after the tag is a missing send; a template Meta did not deliver is failed.
  sendGraceMs: 26 * 60 * 60 * 1000,
  warmup: 20,
  failedYellow: 0.10,
  failedRed: 0.20,
  missingRed: 0.05,
};
const STAGE_IDS = new Set(Object.values(CONFIG.stages));

const lowerTags = (tags) => (Array.isArray(tags) ? tags : []).map((t) => String(t || '').toLowerCase().trim());
const touched = (o) => Date.parse(o.lastStageChangeAt || o.updatedAt || o.createdAt || '') || 0;

// Cards to tag today. Newest silence first: a lead quiet for 3 weeks is warmer
// than one quiet for 5 months. One card per contact.
function selectBacklog(cards, { now, limit, clientEmails = null, personalTag = 'personal' }) {
  const skips = {};
  const skip = (r) => { skips[r] = (skips[r] || 0) + 1; };
  const seen = new Set();
  const eligible = [];
  for (const o of cards || []) {
    if (!o || !o.contactId || seen.has(o.contactId)) { skip('duplicate_or_empty'); continue; }
    seen.add(o.contactId);
    if (o.pipelineId && o.pipelineId !== CONFIG.pipelineId) { skip('other_pipeline'); continue; }
    if (String(o.status || 'open').toLowerCase() !== 'open') { skip('not_open'); continue; }
    if (!STAGE_IDS.has(o.pipelineStageId)) { skip('stage_out_of_scope'); continue; }
    const ct = o.contact || {};
    const tags = lowerTags(ct.tags);
    if (tags.includes(CONFIG.enrolTag) || tags.includes(CONFIG.closedTag)) { skip('already_enrolled'); continue; }
    if (tags.includes(String(personalTag).toLowerCase())) { skip('personal'); continue; }
    if (CONFIG.blockTags.some((t) => tags.includes(t))) { skip('blocked_tag'); continue; }
    if (!String(ct.phone || '').trim()) { skip('no_phone'); continue; }
    const email = String(ct.email || '').toLowerCase().trim();
    if (email && clientEmails && clientEmails.has(email)) { skip('client'); continue; }
    if (now - touched(o) < CONFIG.minSilenceDays * DAY) { skip('recent'); continue; }
    eligible.push(o);
  }
  eligible.sort((a, b) => touched(b) - touched(a));
  return { picked: eligible.slice(0, limit), eligible: eligible.length, skips };
}

// enrolled: [{ contactId, at }] from earlier runs. sends: Map contactId → { at, status }
// of the first strike-1 template found after the tag (null when none).
function evaluateHealth(enrolled, sends, now) {
  const due = (enrolled || []).filter((e) => now - e.at >= CONFIG.sendGraceMs);
  let delivered = 0, failed = 0, missing = 0;
  for (const e of due) {
    const s = sends.get(e.contactId);
    if (!s) missing++;
    else if (String(s.status || '').toLowerCase() === 'failed') failed++;
    else delivered++;
  }
  const n = due.length;
  const failedShare = n ? failed / n : 0;
  const missingShare = n ? missing / n : 0;
  let level = 'green';
  const reasons = [];
  if (n >= CONFIG.warmup) {
    if (failedShare > CONFIG.failedRed) { level = 'red'; reasons.push(`${Math.round(failedShare * 100)}% de plantillas fallidas`); }
    else if (failedShare > CONFIG.failedYellow) { level = 'yellow'; reasons.push(`${Math.round(failedShare * 100)}% de plantillas fallidas`); }
    if (missingShare > CONFIG.missingRed) { level = 'red'; reasons.push(`${missing} contactos sin strike 1 un día después`); }
  } else if (n && missing === n) {
    // Below warmup a run where nothing at all went out is still a broken workflow.
    level = 'red'; reasons.push(`ninguno de ${n} contactos recibió el strike 1`);
  }
  return { level, checked: n, delivered, failed, missing, failedShare, missingShare, reasons };
}

// ── Situation (Ron, 2026-11-10): Max reads the whole thread and picks the
// message that fits; only the strike order is fixed. Four situations, each with
// its own approved strike-1 template; strike 2 is "llamada" for leads we already
// know something about, "calificar" for the rest; strike 3 is the same for all.
const SITUATIONS = ['sin_respuesta', 'respondio_poco', 'conto_negocio', 'pregunto_precio'];
const REAL_TYPES = new Set(['TYPE_WHATSAPP', 'TYPE_INSTAGRAM', 'TYPE_FACEBOOK', 'TYPE_SMS', 'TYPE_EMAIL']);
const norm = (s) => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
const PRICE_RE = /\b(precio|precios|cuanto cuesta|cuanto vale|cuanto cobran|costo|costos|inversion|tarifa|cuanto seria)\b/;

// The lead's own words, oldest first: real channels, inbound, not an automated
// reply (their business auto-reply, or ours echoed back).
function leadWords(messages, { isAutomatedBody = () => false } = {}) {
  return (messages || [])
    .filter((m) => m && REAL_TYPES.has(m.messageType) && m.direction === 'inbound' && String(m.body || '').trim() && !isAutomatedBody(m.body))
    .sort((a, b) => (Date.parse(a.dateAdded || 0) || 0) - (Date.parse(b.dateAdded || 0) || 0))
    .map((m) => String(m.body).trim());
}

// Decided without a model when the thread is clear; null means ask Haiku.
function ruleSituation(messages, opts = {}) {
  const said = leadWords(messages, opts);
  if (!said.length) return 'sin_respuesta';
  if (said.some((s) => PRICE_RE.test(norm(s)))) return 'pregunto_precio';
  return null;
}

function buildSituationPrompt(messages, opts = {}) {
  const lines = (messages || [])
    .filter((m) => m && REAL_TYPES.has(m.messageType) && String(m.body || '').trim())
    .sort((a, b) => (Date.parse(a.dateAdded || 0) || 0) - (Date.parse(b.dateAdded || 0) || 0))
    .slice(-30)
    .map((m) => `${m.direction === 'inbound' ? 'PROSPECTO' : 'NOSOTROS'}: ${String(m.body).replace(/\s+/g, ' ').slice(0, 300)}`);
  return [
    'Lee esta conversación entre NeuroGrowth (NOSOTROS) y un prospecto que dejó de responder. NeuroGrowth vende un sistema para conseguir clientes por LinkedIn sin pagar anuncios.',
    'Clasifica al prospecto en UNA de estas etiquetas:',
    '- conto_negocio: el prospecto nos dijo a qué se dedica, qué vende o a quién le vende.',
    '- pregunto_precio: el prospecto preguntó por el precio, la inversión o el costo.',
    '- respondio_poco: el prospecto respondió algo (un saludo, "info", "gracias", una pregunta general) pero no contó de su negocio ni preguntó precio.',
    'Las respuestas automáticas de un negocio ("Gracias por comunicarte...") no cuentan como respuesta del prospecto.',
    'Responde SOLO con la etiqueta, sin nada más.',
    '',
    ...lines,
  ].join('\n');
}

function parseSituation(text) {
  const t = norm(text);
  return ['conto_negocio', 'pregunto_precio', 'respondio_poco'].find((s) => t.includes(s)) || null;
}

function verdictLine({ mode, picked, eligible, health, paused, situations = null, closed = 0 }) {
  const h = health && health.checked
    ? ` Revisión de días anteriores: ${health.delivered} entregados, ${health.failed} fallidos, ${health.missing} sin enviar.`
    : '';
  const dry = mode === 'dry_run';
  const tagged = dry ? `se etiquetarían ${picked} hoy` : `${picked} etiquetados hoy`;
  const sit = situations && Object.keys(situations).length
    ? ` Situación: ${SITUATIONS.map((s) => `${s} ${situations[s] || 0}`).join(', ')}${situations.sin_clasificar ? `, sin clasificar ${situations.sin_clasificar}` : ''}.`
    : '';
  const lost = closed ? (dry ? ` Se moverían a Lost: ${closed}.` : ` Movidos a Lost: ${closed}.`) : '';
  if (paused) return `⚠️ ISSUES · Reactivación backlog en pausa: ${paused}.${h} No se etiquetó a nadie hoy.`;
  if (health && health.level !== 'green') return `⚠️ ISSUES · Reactivación backlog (${mode}): ${health.reasons.join('; ')}.${h} Hoy ${tagged}. Pendientes: ${eligible - picked}.${sit}${lost}`;
  return `✅ ALL GREEN · Reactivación backlog (${mode}): ${tagged}, ${eligible - picked} pendientes.${sit}${lost}${h}`;
}

module.exports = { CONFIG, STAGE_IDS, SITUATIONS, selectBacklog, evaluateHealth, verdictLine, leadWords, ruleSituation, buildSituationPrompt, parseSituation };
