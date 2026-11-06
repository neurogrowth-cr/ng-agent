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

function verdictLine({ mode, picked, eligible, health, paused }) {
  const h = health && health.checked
    ? ` Revisión de días anteriores: ${health.delivered} entregados, ${health.failed} fallidos, ${health.missing} sin enviar.`
    : '';
  if (paused) return `⚠️ ISSUES · Reactivación backlog en pausa: ${paused}.${h} No se etiquetó a nadie hoy.`;
  if (health && health.level !== 'green') return `⚠️ ISSUES · Reactivación backlog (${mode}): ${health.reasons.join('; ')}.${h} Etiquetados hoy: ${picked}. Pendientes: ${eligible - picked}.`;
  return `✅ ALL GREEN · Reactivación backlog (${mode}): ${picked} etiquetados hoy, ${eligible - picked} pendientes.${h}`;
}

module.exports = { CONFIG, STAGE_IDS, selectBacklog, evaluateHealth, verdictLine };
