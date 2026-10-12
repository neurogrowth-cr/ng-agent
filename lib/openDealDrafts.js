'use strict';
// Open-deal WhatsApp drafts: the pure half of the day-5 / day-30 follow-up.
//
// Max cards the owning closer twice after a call ends in `follow_up`: at day 5
// (the honest close-or-pause) and at day 30 (the planned return). Each card
// carries a WhatsApp message drafted from REVI's read of that call, so the
// closer taps ✅ instead of composing. Everything here is deterministic and
// I/O-free: archetype choice, the prompt, the validator the LLM output must
// pass, and the card block. index.js owns the REVI read, the Claude call and
// the GHL send.
//
// Why two touches and not three (Persuasion Playbook, Play 6 and Appendix D):
// wins close inside 5 days or after day 30, almost never between. A nudge on
// day 7 or 14 lands in the dead zone and reads as chasing. The email sequence
// (postcall-emails-v2) is silent on days 9 to 30 for the same reason, so the
// WhatsApp cadence must not contradict it.
//
// The six archetypes are Ron's (2026-10-11), adapted to the playbook: the
// "honest close" loses its "no he tenido noticias" opener (the phrase Gong
// measured as the one that cuts meetings booked), nothing carries a price or an
// invented deadline, and every line is usted.

const ARCHETYPES = {
  one_question: {
    name: 'Una sola pregunta',
    touch: 'd5',
    shape: 'Una pregunta directa y binaria: ¿al final se decidió por alguien o esto sigue en pie? Nada más.',
    endsWithQuestion: true,
  },
  goal_reminder: {
    name: 'Recordatorio de objetivo',
    touch: 'd5',
    shape: 'Recordar, con sus propias palabras, la meta o el plazo que el prospecto dijo en la llamada, señalar que el plazo avanza, y preguntar si quiere ayuda para lograrlo o no.',
    endsWithQuestion: true,
  },
  new_reason: {
    name: 'Nuevo motivo',
    touch: 'd30',
    shape: 'Una razón nueva y real para escribir (lo que cambió desde la llamada, o un cliente en una situación parecida), dicha en una línea, y "me acordé de usted". Sin pregunta de cierre.',
    endsWithQuestion: false,
  },
  honest_close: {
    name: 'Cierre honesto',
    touch: 'd30',
    shape: 'Asumir con calma que el momento no era el adecuado, decir que todo está bien, y dejar la puerta abierta: ya sabe dónde encontrarnos. Nunca "no he tenido noticias suyas".',
    endsWithQuestion: false,
  },
  retreat: {
    name: 'La retirada',
    touch: 'd30',
    shape: 'Decir con honestidad que puede que esto no sea lo adecuado para su negocio y que está completamente bien; preferimos decirlo antes que seguir escribiendo.',
    endsWithQuestion: false,
  },
  value_delivery: {
    name: 'Entrega de valor',
    touch: null, // v1 never picks it: needs a real piece to attach
    shape: 'Compartir una pieza concreta que resuelve el punto que mencionó en la llamada. Sin pedir nada.',
    endsWithQuestion: false,
  },
};

const TOUCH_LABEL = { d5: 'día 5', d30: 'día 30' };

// Default "what is new" for the day-30 return. Ron updates it per quarter.
const WHATS_NEW_DEFAULT = 'desde octubre las licencias van incluidas (Sales Navigator, Kai y la IA corren por cuenta de NeuroGrowth), y hay un caso nuevo: Smartrade, B2B de servicios, $71,000 cobrados en 42 días desde su primera campaña';

function isMeaningful(v) {
  const s = String(v == null ? '' : v).trim().toLowerCase();
  return !!s && !/^(unknown|none|null|n\/a|no|ninguno|ninguna|desconocido|sin dato|not stated|no mencion)/.test(s);
}

// Which of Ron's six fits this touch, from REVI's signals. Rules, not vibes,
// so the choice can be asserted in a table.
function pickOpenDealArchetype({ touch, signals = {}, dealStatus = null } = {}) {
  const sig = signals || {};
  if (touch === 'd5') {
    return isMeaningful(sig.stated_timeline) ? 'goal_reminder' : 'one_question';
  }
  const strength = String(sig.buying_signal_strength || '').toLowerCase();
  const strong = /\b(alta|alto|high|strong|fuerte|media|medium|moderad)/.test(strength);
  const weak = /\b(baja|bajo|low|weak|débil|debil|none|ninguna)/.test(strength);
  if (strong) return 'new_reason';
  if (weak || String(dealStatus || '').toLowerCase() === 'dead') return 'retreat';
  return 'honest_close';
}

// The prompt. Only closer-safe REVI fields may be passed in `revi` (deal
// recovery plan, general read, signals, company); never teardown_text.
function buildOpenDealDraftPrompt({
  touch, archetype, prospectFirstName, closerFirstName, language = 'es',
  callDateStr = '', ageDays = null, revi = {}, whatsNew = WHATS_NEW_DEFAULT, feedback = null,
} = {}) {
  const a = ARCHETYPES[archetype] || ARCHETYPES.one_question;
  const es = language !== 'en';
  const lines = [];
  lines.push(`You are drafting ONE WhatsApp message that ${closerFirstName || 'the closer'} (closer at NeuroGrowth) will send to a prospect who had a sales call${callDateStr ? ` on ${callDateStr}` : ''}${ageDays != null ? ` (${ageDays} days ago)` : ''} and left the deal open. This is the ${TOUCH_LABEL[touch] || touch} touch.`);
  lines.push('');
  lines.push(`Archetype to follow: "${a.name}". ${a.shape}`);
  lines.push('');
  lines.push('What REVI (our call-review system) captured on that call, use it and nothing else as context:');
  lines.push(`- Prospect: ${prospectFirstName || '(unknown first name)'}${revi.company ? `, ${revi.company}` : ''}`);
  if (revi.recovery) lines.push(`- Next-step / recovery plan from the call: ${revi.recovery}`);
  if (revi.generalRead) lines.push(`- General read: ${revi.generalRead}`);
  if (isMeaningful(revi.objectionType)) lines.push(`- Main objection: ${revi.objectionType}`);
  if (isMeaningful(revi.statedTimeline)) lines.push(`- Timeline or goal the prospect stated: ${revi.statedTimeline}`);
  if (isMeaningful(revi.decisionMaker)) lines.push(`- Decision maker status: ${revi.decisionMaker}`);
  if (isMeaningful(revi.buyingSignal)) lines.push(`- Buying signal strength: ${revi.buyingSignal}`);
  if (!revi.recovery && !revi.generalRead) lines.push('- (No REVI read for this call. Write the generic version of the archetype; invent nothing about the call.)');
  if (archetype === 'new_reason') lines.push(`- What is new since the call (true, approved): ${whatsNew}`);
  lines.push('');
  lines.push('Rules:');
  lines.push(es
    ? '- Spanish, formal usted throughout. Never tutear (no "tú", "te", "tienes", "quieres").'
    : '- English, warm and formal. No slang.');
  lines.push(`- Start with the prospect's first name and a comma. Sign off with just "${closerFirstName || 'the closer'}" on its own last line.`);
  lines.push('- 60 words maximum, counting the name and the sign-off. Short lines are fine.');
  lines.push('- Never "no he tenido noticias", "no supe más de usted", "le escribo para dar seguimiento", "espero que esté bien", "hola, quiero ofrecerte".');
  lines.push('- No price, no discount, no invented deadline or scarcity, no promise beyond the contract (10 or more meetings a month or we keep working free).');
  lines.push('- No emoji, no markdown, no quotation marks around the message, no links.');
  lines.push(a.endsWithQuestion ? '- End with exactly one question.' : '- Do not end with a question; the message asks for nothing.');
  lines.push('- Sound like a person who ran the call and remembers it, not a template.');
  if (feedback) { lines.push(''); lines.push(`Your previous draft was rejected: ${feedback}. Fix exactly that.`); }
  lines.push('');
  lines.push('Output ONLY the message text.');
  return lines.join('\n');
}

const BANNED = [
  'no he tenido noticias', 'no supe más', 'no supe mas', 'para dar seguimiento', 'dar seguimiento',
  'espero que esté bien', 'espero que este bien', 'hola, quiero ofrecerte', 'descuento', 'promoción', 'promocion',
  'oferta especial', 'últimos cupos', 'ultimos cupos', 'el precio sube',
];
const APPROVED_FIGURES = ['$71,000', '$71K', '$71k'];
const TUTEO = /\b(tú|tu|tus|te|tienes|quieres|puedes|podrías|podrias|decidiste|contigo|ti|hablamos contigo)\b/i;
const EMOJI = /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u;

function wordCount(text) {
  return String(text || '').trim().split(/\s+/).filter(Boolean).length;
}

// Validates an LLM draft against the rules the prompt states. Returns
// { ok, problems: [] } so the caller can retry once with the problems as
// feedback, then give up (a card with no draft is better than a bad one).
function validateOpenDealDraft(text, { language = 'es', archetype = 'one_question' } = {}) {
  const problems = [];
  const t = String(text || '').trim();
  const a = ARCHETYPES[archetype] || ARCHETYPES.one_question;
  if (!t) return { ok: false, problems: ['empty'] };
  const words = wordCount(t);
  if (words > 60) problems.push(`too long (${words} words, max 60)`);
  if (words < 6) problems.push('too short');
  const low = t.toLowerCase();
  for (const b of BANNED) if (low.includes(b)) problems.push(`banned phrase: "${b}"`);
  let scrubbed = t;
  for (const f of APPROVED_FIGURES) scrubbed = scrubbed.split(f).join('');
  if (/\$\s?\d/.test(scrubbed) || /\b\d[\d.,]*\s?(usd|dólares|dolares)\b/i.test(scrubbed)) problems.push('contains a price');
  if (language !== 'en' && TUTEO.test(t)) problems.push('tuteo (must be usted)');
  if (EMOJI.test(t)) problems.push('emoji');
  if (/[*_`#]|https?:\/\//.test(t)) problems.push('markdown or link');
  if (/^["“]|["”]$/.test(t)) problems.push('wrapped in quotes');
  // The sign-off ("Jose") sits on its own last line; the question rule is
  // about the line before it.
  const body = t.replace(/\n\s*[^\n?]{1,30}\s*$/, '').trim();
  const endsQ = /\?\s*$/.test(body);
  if (a.endsWithQuestion && !endsQ) problems.push('must end with a question');
  if (!a.endsWithQuestion && endsQ) problems.push('must not end with a question');
  return { ok: problems.length === 0, problems };
}

// Picks the closer-safe fields out of a normalized REVI recording (the shape
// fetchReviRecordings returns) plus the raw coaching object when available.
function reviContextForDraft(rec, coaching = null) {
  if (!rec && !coaching) return {};
  const sig = (rec && rec.signals) || {};
  const c = coaching || {};
  return {
    company: c.company || null,
    recovery: (rec && rec.dealRecovery) || (c.deal && c.deal.recovery) || null,
    generalRead: c.general_read || null,
    objectionType: sig.objection_type || null,
    statedTimeline: sig.stated_timeline || null,
    decisionMaker: sig.decision_maker_status || null,
    buyingSignal: sig.buying_signal_strength || null,
    language: c.language ? (String(c.language).toLowerCase().startsWith('en') ? 'en' : 'es') : 'es',
  };
}

// The block appended to the closer's card. Pure so the exact copy is tested.
function renderOpenDealDraftBlock({ touch, archetype, text, hasContact, source }) {
  const a = ARCHETYPES[archetype] || {};
  const head = `💬 *WhatsApp draft (${TOUCH_LABEL[touch] || touch}, ${a.name || archetype}${source === 'generic' ? ', no REVI read' : ''}):*`;
  const quoted = String(text || '').split('\n').map(l => `> ${l}`).join('\n');
  const how = hasContact
    ? '→ ✅ *sends this by WhatsApp from GHL* · reply `revise: <your text>` in the thread to send your own version instead'
    : '→ ⚠️ No GHL contact on file: copy it and send from your phone, then 💤 or close the card.';
  return [head, quoted, how].join('\n');
}

module.exports = {
  ARCHETYPES, TOUCH_LABEL, WHATS_NEW_DEFAULT,
  pickOpenDealArchetype, buildOpenDealDraftPrompt, validateOpenDealDraft,
  reviContextForDraft, renderOpenDealDraftBlock, wordCount,
};
