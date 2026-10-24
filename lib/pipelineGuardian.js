'use strict';
// Pipeline guardian, phase A (draft and approve). Spec: tasks/specs/pipeline-
// guardian-phase-a.md; decision: strategy repo plan-of-record UPDATE 2026-10-24
// "the pipeline guardian replaces the second setter seat". Oscar leaves
// 2026-10-31; shadow mode starts 2026-11-03, draft mode 2026-11-06; Sebastián
// approves from day one and Ron reads samples.
//
// Max reads every inbound message and every silence on the Appointment Setting
// pipeline, drafts the next reply or strike in usted, and hands Sebastián a
// one-tap approval. Leads that need a person (call request, question outside
// the FAQ, hot verdict, objection outside the playbook) become a signal card
// with a suggestion, never a send. Stops (unsubscribe, angry, legal, sensitive,
// already a client) tag the contact `guardian-off`.
//
// Pure: no Slack, no DB, no GHL, no Anthropic. index.js owns the I/O.
// Tested in test/pipeline-guardian.test.js.

const hotReplies = require('./hotReplies');

const MIN = 60 * 1000;
const H = 60 * MIN;
const DAY = 24 * H;
// Costa Rica is UTC-6 with no DST (same assumption as lib/hotReplies.js).
const CR_OFFSET_MS = 6 * H;

const CONFIG = {
  cadenceDays: [1, 2, 3, 4, 7],   // gap since our last message before strike 1..5
  renurtureDays: [60, 75],         // days after strike 5 for re-nurture 1 and 2
  windowMs: 24 * H,                // Meta: free-text reply only within 24 h of the lead's last message
  settleMs: 2 * MIN,               // let a burst of lead messages finish before drafting
  holdMs: 12 * H,                  // hot reply alerts do not poke the setter while a fresh card waits
  pokeFromHour: 8,                 // cards go out 08:00 to 20:00 CR
  pokeToHour: 20,
  maxDraftsPerRun: 8,
  maxDraftChars: 700,
  maxDraftParagraphs: 3,
  quoteMaxChars: 220,
  signalExpiryMs: 15 * MIN,        // "Sebastián, within 15 min"
  noWindowExpiryMs: 24 * H,        // email cards: the setter has a day
  inboundLookbackMs: 48 * H,       // same as hot replies: a Saturday message is in scope Monday
};

const SALES_PIPELINE_ID = 'KH1IQuaN8aNB1lfRpvP4'; // Appointment Setting
const STAGES = {
  NEW_LEAD:        '93de6a09-78a4-4253-bea4-c1528ed6f2b3',
  INITIAL_CONTACT: '4b936528-794e-40ab-812d-144b9d5e8128',
  STRIKE_1:        '92245916-0622-4f46-aabc-6091b8af5fc0',
  STRIKE_2:        '6274c958-8252-4698-acc6-c6818d43a99f',
  STRIKE_3:        'e639662d-6b1b-42b5-a89d-7ebd70ca97e3',
  HOT_STRIKE:      '8c01f7d1-bfe4-432d-8791-b85e24372ff0',
  CALL_BOOKED:     'dc1fba03-abeb-4b47-9d29-6c308002b6c1',
};
const STAGE_NAME = {
  [STAGES.NEW_LEAD]: 'New Lead', [STAGES.INITIAL_CONTACT]: 'Initial Contact',
  [STAGES.STRIKE_1]: 'Strike 1', [STAGES.STRIKE_2]: 'Strike 2', [STAGES.STRIKE_3]: 'Strike 3',
  [STAGES.HOT_STRIKE]: 'Hot Strike', [STAGES.CALL_BOOKED]: 'Call Booked',
};
// The population: open cards in these stages. Call Booked and later belong to
// the open-deal follow-up, never to the guardian.
const GUARDIAN_STAGE_IDS = [STAGES.NEW_LEAD, STAGES.INITIAL_CONTACT, STAGES.STRIKE_1, STAGES.STRIKE_2, STAGES.STRIKE_3, STAGES.HOT_STRIKE];

// Kai's intent classes (ng-automation/lib/intent-classifier.js, copied not imported).
const INTENT_CLASSES = [
  'interested', 'wants_info', 'objection_timing', 'objection_budget', 'objection_relevance', 'objection_existing',
  'referral', 'question', 'positive_not_ready', 'unsubscribe', 'angry', 'legal_risk', 'competitor_mention',
  'sensitive', 'already_client', 'unclear',
];
const INTENT_LABEL = {
  interested: 'interesado', wants_info: 'pide info', objection_timing: 'objeción de tiempo', objection_budget: 'objeción de presupuesto',
  objection_relevance: 'objeción de relevancia', objection_existing: 'ya tiene algo', referral: 'referido', question: 'pregunta',
  positive_not_ready: 'no por ahora', unsubscribe: 'baja', angry: 'molesto', legal_risk: 'riesgo legal', competitor_mention: 'menciona competidor',
  sensitive: 'tema sensible', already_client: 'ya es cliente', unclear: 'poco claro', silence: 'silencio',
};
const REPLY_INTENTS = new Set(['interested', 'wants_info', 'referral', 'positive_not_ready']);
const STOP_INTENTS = new Set(['unsubscribe', 'angry', 'legal_risk', 'sensitive', 'already_client']);
const RON_STOP_INTENTS = new Set(['legal_risk', 'sensitive']);
const SIGNAL_INTENTS = new Set(['objection_timing', 'objection_budget', 'objection_relevance', 'objection_existing', 'competitor_mention']);

const APPROVED_URLS = {
  booking: 'https://api.leadconnectorhq.com/widget/bookings/linkedin-flywheel-appointment',
  guide: 'https://neurogrowth.io/recursos/plantillas',
};

// GHL message type ↔ guardian channel key ↔ GHL send type.
const CHANNEL_KEY = { TYPE_WHATSAPP: 'whatsapp', TYPE_INSTAGRAM: 'instagram', TYPE_FACEBOOK: 'facebook', TYPE_EMAIL: 'email' };
const CHANNEL_LABEL = { whatsapp: 'WhatsApp', instagram: 'Instagram', facebook: 'Messenger', email: 'Email' };
const CHANNEL_SEND_TYPE = { whatsapp: 'WhatsApp', instagram: 'IG', facebook: 'FB', email: 'Email' };
const META_CHANNELS = new Set(['whatsapp', 'instagram', 'facebook']);
const REAL_TYPES = new Set(['TYPE_WHATSAPP', 'TYPE_INSTAGRAM', 'TYPE_FACEBOOK', 'TYPE_SMS', 'TYPE_EMAIL', 'TYPE_CALL']);
const AUTOMATED_SOURCES = new Set(['workflow', 'bot', 'campaign', 'bulk_actions', 'conversation_ai']);

const msgTime = hotReplies.msgTime;
const msgBody = (m) => String((m && (m.body || m.message)) || '').trim();
const lowerTags = (tags) => (Array.isArray(tags) ? tags : []).map((t) => String(t || '').trim().toLowerCase());
const channelKey = (type) => CHANNEL_KEY[type] || (META_CHANNELS.has(type) || type === 'email' ? type : null);

// ── Screen (step 2) ─────────────────────────────────────────────────────────
// Layered on hotReplies.screenConversation. That screen is written for a lead
// who spoke last on a chat channel inside 48 h; a silent lead has the team
// speaking last, often days ago, so its contact-level rules (personal and
// skip tags, webinar registrants without a sales card, lost cards, clients)
// are run on a copy with the message-level fields neutralised, and the
// guardian adds its own: the `guardian-off` tag, an open card in a guardian
// stage, Call Booked or later, and leads Ron holds in ig_guide_drafts.
function guardianScreen(convo, { now, clientEmails = null, igHeld = null, personalTag = 'personal', offTag = 'guardian-off' } = {}) {
  if (!convo) return { keep: false, reason: 'empty' };
  const tags = lowerTags(convo.tags);
  if (tags.includes(String(offTag).toLowerCase())) return { keep: false, reason: 'guardian_off' };
  const contactLevel = hotReplies.screenConversation(
    { ...convo, lastMessageDirection: 'inbound', lastMessageType: 'TYPE_WHATSAPP', lastMessageDate: now },
    { now, personalTag, clientEmails },
  );
  if (!contactLevel.keep) return contactLevel;
  const opps = Array.isArray(convo.opportunities) ? convo.opportunities : [];
  const open = opps.filter((o) => String(o.status || '').toLowerCase() === 'open' && o.pipelineId === SALES_PIPELINE_ID);
  if (!open.length) return { keep: false, reason: 'no_sales_card' };
  if (open.some((o) => !GUARDIAN_STAGE_IDS.includes(o.pipelineStageId))) return { keep: false, reason: 'call_booked_or_later' };
  if (igHeld && igHeld.has(convo.id)) return { keep: false, reason: 'ig_guide_held' };
  if (convo.lastMessageDirection === 'inbound' && !channelKey(convo.lastMessageType)) return { keep: false, reason: 'other_channel' };
  return { keep: true, reason: null };
}

// ── Thread state (step 3) ───────────────────────────────────────────────────
// Who spoke last, when, and how many strikes we already sent since the lead
// last wrote. A strike is a human (or guardian) send on a real channel after
// the lead's last message, beyond the first one, which is the answer itself
// (or the first contact, for a lead who never wrote). Workflow sends count as
// "our last message" for timing but never as a strike: the automated first
// message starts the clock, the guardian does the chasing.
function threadState(messages, { isAutomatedBody = () => false } = {}) {
  const sorted = (messages || []).filter((m) => m && REAL_TYPES.has(m.messageType)).slice().sort((a, b) => msgTime(a) - msgTime(b));
  let lastInIdx = -1;
  sorted.forEach((m, i) => { if (m.direction === 'inbound') lastInIdx = i; });
  const lastIn = lastInIdx >= 0 ? sorted[lastInIdx] : null;
  const after = sorted.slice(lastInIdx + 1).filter((m) => m.direction === 'outbound');
  const human = after.filter((m) => !AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase()) && !isAutomatedBody(msgBody(m)));
  const lastOut = after.length ? after[after.length - 1] : null;
  const newestOut = sorted.slice().reverse().find((m) => m.direction === 'outbound') || null;
  const channelType = (lastIn && lastIn.messageType) || (newestOut && newestOut.messageType) || null;
  return {
    hasMessages: sorted.length > 0,
    leadSpokeLast: !!lastIn && after.length === 0,
    lastInboundAt: lastIn ? msgTime(lastIn) : null,
    lastInboundId: lastIn ? lastIn.id : null,
    lastInboundText: lastIn ? msgBody(lastIn) : '',
    lastOutboundAt: lastOut ? msgTime(lastOut) : (newestOut ? msgTime(newestOut) : null),
    outboundSinceInbound: after.length,
    strikesSent: Math.max(0, human.length - 1),
    channel: channelKey(channelType),
  };
}

// ── Window (step 3) ─────────────────────────────────────────────────────────
// Meta channels: free text only while now < lastInbound + 24 h (an ad-started
// chat may carry 7 days; 24 h is the limit until Meta's documentation is
// confirmed). WhatsApp outside the window needs an approved template, which
// phase A does not send. Email is always open. A lead who never wrote has no
// window on any Meta channel.
function windowState({ channel, lastInboundAt }, now) {
  const key = channelKey(channel) || channel;
  if (key === 'email') return { channel: 'email', open: true, closesAt: null, remainingMs: null };
  const at = lastInboundAt == null ? null : (typeof lastInboundAt === 'number' ? lastInboundAt : Date.parse(lastInboundAt));
  if (!at) return { channel: key, open: false, closesAt: null, remainingMs: null, reason: 'no_inbound' };
  const closesAt = at + CONFIG.windowMs;
  return { channel: key, open: now < closesAt, closesAt, remainingMs: closesAt - now };
}

// ── Cadence (3.1, rows "silent N days" and "re-nurture") ────────────────────
// Strike N is due when N-1 strikes went out and our last message is at least
// cadenceDays[N-1] days old. After strike 5, re-nurture 1 at 60 days and
// re-nurture 2 at 75 days (so 15 after re-nurture 1). Null when nothing is due
// or the sequence is over.
function strikeStep(lastOutboundAt, strikesSent, now) {
  if (!lastOutboundAt) return null;
  const at = typeof lastOutboundAt === 'number' ? lastOutboundAt : Date.parse(lastOutboundAt);
  if (!at) return null;
  const n = Number(strikesSent) || 0;
  const gapDays = (now - at) / DAY;
  if (n < CONFIG.cadenceDays.length) {
    return gapDays >= CONFIG.cadenceDays[n] ? `strike_${n + 1}` : null;
  }
  const r = n - CONFIG.cadenceDays.length; // re-nurtures already sent
  if (r >= CONFIG.renurtureDays.length) return null;
  const need = r === 0 ? CONFIG.renurtureDays[0] : CONFIG.renurtureDays[r] - CONFIG.renurtureDays[r - 1];
  return gapDays >= need ? `renurture_${r + 1}` : null;
}

// A lead asking for a call, naming a time, or leaving a number: a person takes
// it (3.1 row 2). Cheap and deterministic; the classifier's call_request flag
// is the second opinion.
const CALL_RE = /\b(ll[aá]me(me|nme)?|llamar(me|nos)?|una llamada|me llama[ns]?|hablar por tel[eé]fono|agendar|agende(mos)?|le parece (el|a las|ma[ñn]ana)|ma[ñn]ana a las|hoy a las|a las \d{1,2}(:\d{2})?\s*(am|pm|h)?)\b/i;
const PHONE_RE = /(\+?\d[\d\s().-]{7,}\d)/;
function detectCallRequest(text) {
  const t = String(text || '');
  return CALL_RE.test(t) || PHONE_RE.test(t);
}

// ── Decision (3.1) ──────────────────────────────────────────────────────────
//   analysis: hotReplies.analyzeThread result (lead waiting) or null (silent)
//   intent, verdict, signals {callRequest, faqCovered}: classifier output (reply path only)
//   thread: threadState result; contact: { email, phone }; stageId
// Returns { kind: 'reply'|'signal'|'stop'|'strike'|'skip', step, reason, channel, askContact, ron }.
function decideAction({ analysis, intent, verdict, signals = {}, thread, contact = {}, stageId, now }) {
  if (stageId && !GUARDIAN_STAGE_IDS.includes(stageId)) return { kind: 'skip', step: null, reason: 'call_booked_or_later', channel: null };
  const hasEmail = !!String(contact.email || '').trim();
  const hasPhone = !!String(contact.phone || '').trim();
  const reroute = (channel, lastInboundAt, kind) => {
    const w = windowState({ channel, lastInboundAt }, now);
    if (w.open) return { channel: w.channel, closesAt: w.closesAt };
    if (hasEmail) return { channel: 'email', closesAt: null, moved: true };
    return { skip: { kind: 'skip', step: kind, reason: 'window_closed', channel: w.channel } };
  };

  if (analysis) {
    // The lead wrote and nobody answered.
    if (now - analysis.newestAt < CONFIG.settleMs) return { kind: 'skip', step: 'reply', reason: 'settling', channel: channelKey(analysis.channel) };
    const chan = channelKey(analysis.channel);
    if (STOP_INTENTS.has(intent)) return { kind: 'stop', step: 'stop', reason: intent, channel: chan, ron: RON_STOP_INTENTS.has(intent) };
    const wantsCall = signals.callRequest === true || detectCallRequest(analysis.quote);
    if (wantsCall) return { kind: 'signal', step: 'signal', reason: 'call_request', channel: chan };
    if (verdict === 'hot') return { kind: 'signal', step: 'signal', reason: 'hot', channel: chan };
    if (SIGNAL_INTENTS.has(intent)) return { kind: 'signal', step: 'signal', reason: 'objection', channel: chan };
    if (intent === 'question' && signals.faqCovered !== true) return { kind: 'signal', step: 'signal', reason: 'outside_faq', channel: chan };
    const askContact = (chan === 'instagram' || chan === 'facebook') && !hasPhone && !hasEmail;
    const r = reroute(chan, analysis.newestAt, 'reply');
    if (r.skip) return r.skip;
    if (REPLY_INTENTS.has(intent) || intent === 'question') return { kind: 'reply', step: 'reply', reason: intent, channel: r.channel, closesAt: r.closesAt, askContact, movedToEmail: !!r.moved };
    if (intent === 'unclear' || !intent) return { kind: 'reply', step: 'clarify', reason: 'unclear', channel: r.channel, closesAt: r.closesAt, askContact, movedToEmail: !!r.moved };
    return { kind: 'signal', step: 'signal', reason: `intent_${intent}`, channel: chan };
  }

  // Silent lead: ours is the newest message.
  if (!thread || !thread.hasMessages) return { kind: 'skip', step: null, reason: 'no_messages', channel: null };
  if (thread.leadSpokeLast) return { kind: 'skip', step: null, reason: 'lead_waiting_unanalyzed', channel: thread.channel };
  const step = strikeStep(thread.lastOutboundAt, thread.strikesSent, now);
  if (!step) return { kind: 'skip', step: null, reason: 'not_due', channel: thread.channel };
  const r = reroute(thread.channel || 'whatsapp', thread.lastInboundAt, step);
  if (r.skip) return r.skip;
  const askContact = (thread.channel === 'instagram' || thread.channel === 'facebook') && !hasPhone && !hasEmail;
  return { kind: 'strike', step, reason: 'silence', channel: r.channel, closesAt: r.closesAt, askContact, movedToEmail: !!r.moved };
}

// ── Prompts (steps 4 and 6) ─────────────────────────────────────────────────
// Persuasion Playbook, condensed to the rules a chat message can honour.
const PLAYBOOK_RULES = [
  'Postura: evaluamos si podemos ayudar. No rogamos, no perseguimos, no convencemos.',
  'Una idea por mensaje. Primero entender su negocio, después ofrecer la llamada.',
  'Qué averiguar, una cosa a la vez: qué vende y a quién, cuánto cobra por cliente, cómo le llegan clientes hoy, qué le cuesta más, quién decide.',
  'La llamada se ofrece en dos pasos: primero el sí ("le parece?"), después el calendario.',
  'Objeción de tiempo: valide y pregunte cuándo conviene retomar; nunca insista dos veces.',
  'Objeción de presupuesto o precio: el precio se ve en la llamada según su caso; no se dice un número por chat.',
  'Objeción "ya tengo algo" o "ya me quemó una agencia": no somos agencia, el sistema queda suyo; pregunte qué resultado le dio lo que tiene.',
  'Referido: agradezca corto y pregunte el nombre y cómo contactarlo, o pida que lo presente.',
  'Nunca prometa resultados, clientes ni plazos. Nunca ofrezca descuentos.',
  'Strike 1: recordatorio corto de que seguimos aquí, una pregunta fácil de contestar.',
  'Strike 2: una pregunta de calificación (qué vende y a quién, o cómo le llegan clientes hoy).',
  'Strike 3: una prueba concreta (un caso real, sin cifras inventadas) y una pregunta.',
  'Strike 4: cierre suave: ofrezca la llamada o la salida, sin presión.',
  'Strike 5: un recurso útil (la guía) y la puerta abierta; sin pregunta.',
  'Re-nurture (60 y 75 días): un mensaje humano, corto, retomando sin reclamar el silencio.',
];

// What the guardian may answer by itself. GUARDIAN_FAQ on Railway replaces it.
const DEFAULT_FAQ = [
  'Qué hacemos: instalamos en el LinkedIn del cliente un sistema que le trae reuniones con quien decide, sin publicidad y sin que el cliente prospecte. El sistema queda suyo.',
  'Para quién es: empresas y profesionales que venden servicios de 1,500 dólares o más por cliente a otras empresas.',
  'Cómo sigue: una llamada corta de diagnóstico con el equipo; ahí se ve si tiene sentido y cuánto costaría en su caso.',
  'Precio: se define en la llamada según el caso; por chat no se dan cifras.',
  'Quién escribe: el equipo de NeuroGrowth (Ron Duarte, fundador), desde Costa Rica, para Costa Rica y México.',
].join('\n');

function buildClassifierPrompt({ contactName, tail, faq }) {
  const lines = (tail || []).map((m) => {
    const who = m.direction === 'inbound' ? 'LEAD' : 'EQUIPO';
    const body = msgBody(m) || (m.attachments && m.attachments.length ? '[adjunto]' : '[sin texto]');
    return `${who}: ${body.replace(/\s+/g, ' ').slice(0, 500)}`;
  });
  return [
    'You triage WhatsApp, Instagram and Messenger chats for a sales team that sells a LinkedIn client-acquisition service in Costa Rica and Mexico.',
    'Classify the LEAD\'s latest unanswered messages. Output ONLY one JSON object, no prose:',
    `{"verdict":"hot|normal|noise","intent":"${INTENT_CLASSES.join('|')}","call_request":true|false,"faq_covered":true|false,"reason_es":"<one short Spanish sentence>","summary_es":"<what the lead wants, max 12 words, Spanish>","ficha":{"empresa":"<text or null>","vende_a":"<text or null>","dolor":"<text or null>","urgencia":"<text or null>"}}`,
    '',
    'verdict: hot = a buying signal a setter must answer now (asks the price or how it works, wants a call or proposes a time, says yes to a next step, sends business info after being asked, asks a direct question about the service). noise = nothing to answer (business auto-reply, thanks or emoji only, opt-out, "ya contratamos a otro"). normal = everything else.',
    'intent: exactly one class. question = asks something concrete; wants_info = asks for material or an explanation in general; objection_* = pushes back on timing, budget, relevance or because they already have something; legal_risk = threatens, mentions lawyers, data protection or spam complaints; sensitive = health, grief, personal crisis, politics or religion; already_client = says they are a current client; unclear = cannot tell.',
    'call_request: true when the lead asks for a call, proposes a day or time, writes "llámeme", or shares a phone number.',
    'faq_covered: true only when the FAQ below answers the lead\'s question completely, without a price, a promise or a guess.',
    'ficha: what the lead has said about their company, who they sell to, their pain and their urgency; null when not said.',
    '',
    'FAQ:',
    faq || DEFAULT_FAQ,
    '',
    `Contact: ${contactName || 'unknown'}`,
    'Conversation (oldest first):',
    ...lines,
  ].join('\n');
}

// Never throws. Anything unreadable is normal / unclear, so a bad model answer
// becomes a clarifying question for the setter to approve, never a send.
function parseClassification(text) {
  const fallback = { verdict: 'normal', intent: 'unclear', callRequest: false, faqCovered: false, reason_es: '', summary_es: '', ficha: {}, parsed: false };
  const m = String(text || '').match(/\{[\s\S]*\}/);
  if (!m) return fallback;
  try {
    const j = JSON.parse(m[0]);
    const v = String(j.verdict || '').toLowerCase().trim();
    const intent = INTENT_CLASSES.includes(j.intent) ? j.intent : 'unclear';
    const ficha = {};
    for (const f of ['empresa', 'vende_a', 'dolor', 'urgencia']) {
      const x = j.ficha && j.ficha[f];
      ficha[f] = x == null || String(x).trim() === '' || String(x).trim().toLowerCase() === 'null' ? null : String(x).trim().slice(0, 120);
    }
    return {
      verdict: ['hot', 'normal', 'noise'].includes(v) ? v : 'normal', intent,
      callRequest: j.call_request === true, faqCovered: j.faq_covered === true,
      reason_es: String(j.reason_es || '').slice(0, 200), summary_es: String(j.summary_es || '').slice(0, 200), ficha, parsed: true,
    };
  } catch (_) { return fallback; }
}

/** Thread as the drafter reads it: oldest first, last 12, roles in Spanish. */
function threadForPrompt(messages) {
  return (messages || []).filter((m) => m && REAL_TYPES.has(m.messageType)).slice().sort((a, b) => msgTime(a) - msgTime(b)).slice(-12)
    .map((m) => {
      const who = m.direction === 'inbound' ? 'PROSPECTO'
        : AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase()) ? 'NOSOTROS (automático)' : 'NOSOTROS';
      const body = msgBody(m).replace(/\s+/g, ' ').slice(0, 600);
      return body ? `${who}: ${body}` : null;
    }).filter(Boolean).join('\n');
}

const STEP_INSTRUCTION = {
  reply: 'Responda lo que el prospecto dijo, en una idea, y cierre con la siguiente pregunta de calificación que falte.',
  clarify: 'El mensaje del prospecto no se entiende. Haga UNA pregunta corta para aclarar qué necesita.',
  strike_1: 'Strike 1 de 5: recordatorio corto de que seguimos aquí y una pregunta fácil de contestar.',
  strike_2: 'Strike 2 de 5: una pregunta de calificación (qué vende y a quién, o cómo le llegan clientes hoy).',
  strike_3: 'Strike 3 de 5: una prueba concreta sin cifras inventadas (por ejemplo: clientes de consultoría y software en Costa Rica y México ya agendan reuniones con quien decide desde su LinkedIn) y una pregunta.',
  strike_4: 'Strike 4 de 5: cierre suave: ofrezca la llamada corta o la salida, sin presión, pidiendo un sí o un no.',
  strike_5: `Strike 5 de 5: deje un recurso útil, la guía ${APPROVED_URLS.guide}, y la puerta abierta. Sin pregunta.`,
  renurture_1: 'Re-nurture 1 (60 días): un mensaje humano y corto retomando el contacto sin reclamar el silencio, con una pregunta abierta sobre cómo le va con clientes.',
  renurture_2: 'Re-nurture 2 (75 días): último mensaje: una frase, la puerta abierta, sin pregunta.',
  signal: 'Esto NO se envía: es una SUGERENCIA para el setter humano que tomará la conversación. Escriba cómo respondería usted, en usted, siguiendo el playbook.',
};

/**
 * Static system prompt (cacheable) plus the per-lead user prompt. The drafter
 * writes as the NeuroGrowth team in usted; Sebastián approves before anything
 * is sent. Output is one JSON object: {"draft": "...", "reason_es": "..."}.
 */
function buildDraftPrompt({ action, thread, contactName, intentLabel, stageName, faq, channel, bookingUrl = APPROVED_URLS.booking, guideUrl = APPROVED_URLS.guide }) {
  const system = `Usted escribe por ${CHANNEL_LABEL[channel] || 'chat'} como el equipo comercial de NeuroGrowth (Costa Rica). Un setter humano revisa y aprueba cada mensaje antes de enviarlo.

VOZ
- Español de Costa Rica, siempre de usted. Nunca tú, vos, tienes, puedes, quieres.
- Corto y humano, como un mensaje entre profesionales que se respetan: 1 a 3 párrafos cortos, máximo 700 caracteres. Por correo igual de corto.
- Sin halagos ni relleno ("Gracias por escribir", "Excelente", "Estimado"). Sin emojis. Sin guion largo. Sin signos de exclamación.
- Nunca use: leads, agencia (para nosotros), ticket, ICP, setter, closer, triage, funnel, embudo. Diga: lo que cobra, su cliente ideal, reuniones con quien decide, un sistema en su LinkedIn.

PLAYBOOK
${PLAYBOOK_RULES.map((r) => `- ${r}`).join('\n')}

LO QUE PUEDE AFIRMAR (FAQ)
${faq || DEFAULT_FAQ}

REGLAS FIJAS
- Nunca diga precios, planes, descuentos, garantías ni promesas de resultados o de clientes. El precio se ve en la llamada.
- Solo dos enlaces existen: el calendario del equipo (${bookingUrl}), únicamente cuando el prospecto ya dijo sí a la llamada, y la guía (${guideUrl}), únicamente en el strike 5. Ningún otro enlace.
- No invente datos del prospecto ni de NeuroGrowth. Si no sabe, pregunte.
- Si el prospecto está en Instagram o Messenger y no tenemos su WhatsApp ni su correo, el mensaje termina pidiendo uno de los dos para seguir por ahí.
- Nada de texto fuera del JSON. Sin corchetes ni llaves dentro del borrador.

FORMATO DE SALIDA: solo un objeto JSON, sin texto alrededor:
{"draft": "el mensaje a enviar", "reason_es": "una frase de por qué este mensaje"}`;

  const user = [
    `Prospecto: ${contactName || 'sin nombre'}`,
    `Etapa en el pipeline: ${stageName || 'desconocida'}`,
    `Intención detectada: ${intentLabel || 'silencio'}`,
    `Paso: ${action.step}. ${STEP_INSTRUCTION[action.step] || STEP_INSTRUCTION.reply}`,
    action.askContact ? 'Este prospecto está en Instagram o Messenger sin WhatsApp ni correo conocidos: termine pidiendo su WhatsApp o su correo.' : null,
    action.movedToEmail ? 'La ventana de Meta cerró: este mensaje va por correo. Empiece con una línea que explique por qué le escribe por aquí.' : null,
    '',
    'Conversación (más antigua primero):',
    thread || '(sin mensajes)',
  ].filter((l) => l !== null).join('\n');
  return { system, user };
}

/** Parse the drafter's JSON. Never throws; a broken reply becomes { ok: false }. */
function parseDraft(text) {
  const raw = String(text || '');
  const start = raw.indexOf('{');
  const end = raw.lastIndexOf('}');
  if (start < 0 || end <= start) return { ok: false, error: 'no JSON object' };
  try {
    const j = JSON.parse(raw.slice(start, end + 1));
    return { ok: true, draft: String(j.draft || '').trim(), reason_es: String(j.reason_es || '').trim().slice(0, 200) };
  } catch (e) { return { ok: false, error: `bad JSON: ${e.message}` }; }
}

// ── Criteria (4a) ───────────────────────────────────────────────────────────
const TU_RE = /(^|[^a-záéíóúñü])(tú|tienes|puedes|quieres)([^a-záéíóúñü]|$)/i;
const BANNED_WORDS = ['leads', 'lead', 'ticket', 'icp', 'setter', 'closer', 'triage', 'funnel', 'embudo', 'garantizo', 'garantizamos', 'garantizado', 'garantía', 'descuento'];
const PLACEHOLDER_RE = /\[|\{\{|\bundefined\b|\bnull\b|\[object Object\]/;
const PRICE_RE = /\$\s?\d|\d\s?\$|\d\s?usd\b|\busd\s?\d/i;

/** validateDraft(text, { allowedUrls }) → { ok, problems }. */
function validateDraft(text, { allowedUrls = Object.values(APPROVED_URLS) } = {}) {
  const problems = [];
  const t = String(text || '');
  if (!t.trim()) return { ok: false, problems: ['empty draft'] };
  if (t.length > CONFIG.maxDraftChars) problems.push(`too long (${t.length} chars)`);
  const paragraphs = t.split(/\n\s*\n/).map((p) => p.trim()).filter(Boolean);
  if (paragraphs.length > CONFIG.maxDraftParagraphs) problems.push(`${paragraphs.length} paragraphs`);
  if (/\u2014/.test(t)) problems.push('em dash');
  if (PLACEHOLDER_RE.test(t)) problems.push('placeholder');
  if (TU_RE.test(t)) problems.push('uses tú');
  if (PRICE_RE.test(t)) problems.push('mentions a price');
  if (/[!¡]/.test(t)) problems.push('exclamation mark');
  const lower = t.toLowerCase();
  for (const w of BANNED_WORDS) if (new RegExp(`(^|[^a-záéíóúñ])${w}([^a-záéíóúñ]|$)`, 'i').test(lower)) problems.push(`banned word "${w}"`);
  for (const raw of t.match(/https?:\/\/\S+/g) || []) {
    const u = raw.replace(/[).,]+$/, '');
    if (!allowedUrls.includes(u)) problems.push(`unexpected link ${u}`);
  }
  if (/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u.test(t)) problems.push('emoji');
  return { ok: problems.length === 0, problems };
}

// ── Card (3.2) ──────────────────────────────────────────────────────────────
const INVALID_DRAFT_MARK = '⚠️ BORRADOR INVÁLIDO';
const GHL_LINK_PREFIX = 'https://app.gohighlevel.com/';

function crClock(ms) {
  const d = new Date(ms - CR_OFFSET_MS);
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
}
function crDay(ms) {
  return new Date(ms - CR_OFFSET_MS).toISOString().slice(0, 10);
}
function ago(ms) {
  const m = Math.max(0, Math.round(ms / MIN));
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  return h < 24 ? `${h} h ${m % 60} min` : `${Math.floor(h / 24)} d ${h % 24} h`;
}
function shortQuote(q, max = CONFIG.quoteMaxChars) {
  const s = String(q || '').replace(/\s+/g, ' ').trim();
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}
function stepLabel(step) {
  if (step === 'reply') return 'respuesta';
  if (step === 'clarify') return 'respuesta (aclarar)';
  if (step === 'signal') return 'señal';
  if (step === 'stop') return 'alto';
  const s = /^strike_(\d)$/.exec(step);
  if (s) return `strike ${s[1]} de ${CONFIG.cadenceDays.length}`;
  const r = /^renurture_(\d)$/.exec(step);
  if (r) return `re-nurture ${r[1]}`;
  return step;
}
// When the setter must act by: Meta window end, 15 min for a signal, a day otherwise.
function expiryFor(action, now) {
  if (action.kind === 'signal') return now + CONFIG.signalExpiryMs;
  if (action.closesAt) return action.closesAt;
  return now + CONFIG.noWindowExpiryMs;
}

/**
 * action: { kind, step, contactName, channel, intent, leadQuote, leadAt, stageName,
 *           draft, problems, closesAt, ficha, contactLink, bookingUrl }
 */
function formatCard(action, { now }) {
  const chan = CHANNEL_LABEL[action.channel] || 'chat';
  const intentLabel = INTENT_LABEL[action.intent] || (action.kind === 'strike' ? INTENT_LABEL.silence : (action.intent || 'sin clasificar'));
  const quote = action.leadQuote ? `"${shortQuote(action.leadQuote)}"` : '"(sin mensajes del prospecto)"';
  const leadAgo = action.leadAt ? ago(now - action.leadAt) : 'nunca';
  const lines = [
    `🛡️ Guardián · ${action.contactName || 'Prospecto'} · ${chan} · ${intentLabel}`,
    `Lead (hace ${leadAgo}): ${quote}`,
    `Etapa: ${action.stageName || 'sin etapa'} · Paso: ${stepLabel(action.step)}`,
  ];
  const label = action.kind === 'signal' ? 'Sugerencia:' : 'Borrador:';
  const body = String(action.draft || '').trim();
  if (body && !(action.problems && action.problems.length)) {
    lines.push(label, ...body.split('\n'));
  } else {
    const why = (action.problems && action.problems.length) ? ` (${action.problems.join('; ')})` : '';
    lines.push(`${label} ${INVALID_DRAFT_MARK}${why}${action.kind === 'signal' ? '' : ', responda en este hilo con su texto'}`);
  }
  if (action.kind === 'signal') {
    const f = action.ficha || {};
    const known = [['empresa', 'empresa'], ['vende_a', 'a quién vende'], ['dolor', 'dolor'], ['urgencia', 'urgencia']]
      .filter(([k]) => f[k]).map(([k, l]) => `${l}: ${f[k]}`);
    lines.push(`Ficha: ${known.length ? known.join(' · ') : 'sin datos todavía'}`);
    lines.push(`Agendar: ${action.bookingUrl || APPROVED_URLS.booking}`);
  }
  const exp = expiryFor(action, now);
  const windowNote = action.closesAt ? `ventana de Meta cierra ${crClock(action.closesAt)}` : 'sin ventana';
  lines.push(`Vence: ${crClock(exp)} CR (${windowNote})`);
  lines.push(action.kind === 'signal' ? '✅ atendido · ❌ descartar' : '✅ enviar · ❌ descartar · responda en este hilo para enviar su texto');
  lines.push(`🔗 ${action.contactLink}`);
  return lines.join('\n');
}

/** Structural contract (4a) on a rendered card. */
function validateCard(text) {
  const problems = [];
  const t = String(text || '');
  const lines = t.split('\n');
  const has = (prefix) => lines.some((l) => l.startsWith(prefix) && l.slice(prefix.length).trim().length > 0);
  if (!has('🛡️ Guardián ·')) problems.push('missing header');
  if (!has('Lead (hace')) problems.push('missing lead line');
  if (!has('Etapa:')) problems.push('missing stage line');
  const bodyIdx = lines.findIndex((l) => l === 'Borrador:' || l === 'Sugerencia:' || l.startsWith('Borrador: ') || l.startsWith('Sugerencia: '));
  if (bodyIdx < 0) problems.push('missing Borrador/Sugerencia');
  else {
    const inline = lines[bodyIdx].replace(/^(Borrador|Sugerencia):\s*/, '').trim();
    const next = (lines[bodyIdx + 1] || '').trim();
    if (!inline && !next) problems.push('empty draft');
  }
  if (!has('Vence:')) problems.push('missing expiry');
  if (!lines.some((l) => l.startsWith(`🔗 ${GHL_LINK_PREFIX}`))) problems.push('missing GHL link');
  if (lines.some((l) => l.startsWith('Sugerencia:')) && !lines.some((l) => l.startsWith('Agendar: ') && l.includes(APPROVED_URLS.booking))) problems.push('signal without Agendar');
  if (/\u2014/.test(t)) problems.push('em dash');
  if (/\bundefined\b|\bnull\b|\[object Object\]|\{\{/.test(t)) problems.push('placeholder');
  return { ok: problems.length === 0, problems };
}

// ── Digest (3, step 11; verdict line in 7) ──────────────────────────────────
// The spec's exact strings carry an em dash; Ron's writing rule forbids it in
// anything a human reads, so the separator is a middle dot. Same words.
function verdictLine(stats) {
  const s = stats || {};
  if (s.issues && s.issues.length) return `⚠️ ISSUES · ${s.issues.join(' · ')}`;
  const median = s.medianMinutes == null ? 'n/d' : `${Math.round(s.medianMinutes)}`;
  const approval = s.approvalRate == null ? 'n/d' : `${Math.round(s.approvalRate * 100)}`;
  return `✅ ALL GREEN · guardián: ${s.leadsRead || 0} leads leídos, ${s.drafts || 0} borradores, ${s.signals || 0} señales a Sebastián, ${s.sent || 0} enviados, ${s.expired || 0} vencidos, mediana ${median} min, aprobación ${approval}%`;
}

// rows: waiting guardian_actions (status drafted or signal) with contact_name,
// channel, step, window_closes_at, created_at. Returns '' when nothing waits
// and the day had no activity (a no-change day is no post).
function formatDigest(rows, { now, stats = {} }) {
  const waiting = (rows || []).filter((r) => r && (r.status === 'drafted' || r.status === 'signal'));
  const dayHadActivity = (stats.drafts || 0) + (stats.signals || 0) + (stats.sent || 0) + (stats.expired || 0) > 0;
  if (!waiting.length && !dayHadActivity) return '';
  const groups = { expired: [], today: [], tomorrow: [], none: [] };
  for (const r of waiting) {
    const closes = r.window_closes_at ? Date.parse(r.window_closes_at) : null;
    if (!closes) groups.none.push(r);
    else if (closes <= now) groups.expired.push(r);
    else if (crDay(closes) === crDay(now)) groups.today.push(r);
    else groups.tomorrow.push(r);
  }
  const item = (r) => `• ${r.contact_name || 'Sin nombre'} (${CHANNEL_LABEL[r.channel] || r.channel}, ${stepLabel(r.step)}${r.window_closes_at ? `, ventana ${crClock(Date.parse(r.window_closes_at))}` : ''})`;
  const out = [`🛡️ *Guardián · cierre del día* · ${waiting.length} tarjeta(s) esperando su ✅`];
  if (groups.expired.length) out.push('', `*Ventana cerrada* (${groups.expired.length}): ya no se puede enviar desde la tarjeta`, ...groups.expired.map(item));
  if (groups.today.length) out.push('', `*Vencen hoy* (${groups.today.length})`, ...groups.today.map(item));
  if (groups.tomorrow.length) out.push('', `*Vencen mañana* (${groups.tomorrow.length})`, ...groups.tomorrow.map(item));
  if (groups.none.length) out.push('', `*Sin ventana* (${groups.none.length}): correo, cuando pueda`, ...groups.none.map(item));
  out.push('', verdictLine(stats));
  return out.join('\n');
}

// ── Watchdog (4b) ───────────────────────────────────────────────────────────
// m: { runsToday, capStreak, cardsToday, isWeekday, inboundToday, approvals,
//      rejections, medianMinutes, expiredShare, sentInShadowOrOff,
//      sentWithoutApproval, divergence, daysActive, maxDraftsPerRun }
// Returns [{ metric, level, text }], one per metric; warmup rows (n < 7 days)
// come back green with the text "warmup".
function evaluateWatchdog(m) {
  const out = [];
  const warm = (Number(m.daysActive) || 0) >= 7;
  const push = (metric, level, text) => out.push({ metric, level, text });
  const gated = (metric, level, text) => (warm ? push(metric, level, text) : push(metric, 'green', `${metric}: warmup (day ${m.daysActive || 0} of 7)`));

  const runs = Number(m.runsToday) || 0;
  gated('runs', runs >= 52 ? 'green' : runs >= 48 ? 'yellow' : 'red', `runs completed today: ${runs} (green 52, red below 48)`);
  gated('drafts_cap', (Number(m.capStreak) || 0) >= 3 ? 'red' : 'green', `drafts-per-run cap (${m.maxDraftsPerRun || CONFIG.maxDraftsPerRun}) hit ${m.capStreak || 0} run(s) in a row`);
  const cards = Number(m.cardsToday) || 0;
  let cardsLevel = 'green';
  if (cards > 60 || (cards === 0 && m.isWeekday && (Number(m.inboundToday) || 0) > 0)) cardsLevel = 'red';
  else if (cards > 40) cardsLevel = 'yellow';
  gated('cards', cardsLevel, `cards to Sebastián today: ${cards} (green 5 to 40, yellow 41 to 60)`);
  const decided = (Number(m.approvals) || 0) + (Number(m.rejections) || 0);
  const rate = decided ? (Number(m.approvals) || 0) / decided : null;
  gated('approval', rate == null || rate >= 0.7 ? 'green' : rate >= 0.5 ? 'yellow' : 'red', `approval rate 7d: ${rate == null ? 'n/d' : `${Math.round(rate * 100)}%`} (green 70%+)`);
  const med = m.medianMinutes;
  gated('median', med == null || med <= 15 ? 'green' : med <= 30 ? 'yellow' : 'red', `median lead message to card: ${med == null ? 'n/d' : `${Math.round(med)} min`} (green 15)`);
  const exp = m.expiredShare;
  gated('expired', exp == null || exp <= 0.10 ? 'green' : exp <= 0.25 ? 'yellow' : 'red', `cards expired unanswered 7d: ${exp == null ? 'n/d' : `${Math.round(exp * 100)}%`} (green 10%)`);
  // Zero tolerance, no warmup.
  push('sent_in_shadow', (Number(m.sentInShadowOrOff) || 0) > 0 ? 'red' : 'green', `messages sent to leads in shadow or off: ${m.sentInShadowOrOff || 0}`);
  push('sent_without_approval', (Number(m.sentWithoutApproval) || 0) > 0 ? 'red' : 'green', `messages sent without an approved row: ${m.sentWithoutApproval || 0}`);
  const div = Number(m.divergence) || 0;
  push('divergence', div >= 2 ? 'red' : div === 1 ? 'yellow' : 'green', `approved rows with no sent_at after 10 min: ${div}`);
  return out;
}

function median(values) {
  const v = (values || []).filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  if (!v.length) return null;
  const mid = Math.floor(v.length / 2);
  return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

/** The text to send: the approver's latest thread reply wins over the draft. */
function pickSendText({ draft, threadReplies, approverIds }) {
  const ids = new Set(approverIds || []);
  const replies = (threadReplies || []).filter((m) => m && ids.has(m.user) && String(m.text || '').trim());
  if (replies.length) {
    const last = replies.sort((a, b) => Number(a.ts) - Number(b.ts))[replies.length - 1];
    return { text: unSlack(last.text), edited: true };
  }
  return { text: String(draft || '').trim(), edited: false };
}

/** Slack mrkdwn back to plain text: links, entities, quotes. */
function unSlack(text) {
  return String(text || '')
    .replace(/<(https?:\/\/[^|>]+)\|[^>]*>/g, '$1')
    .replace(/<(https?:\/\/[^>]+)>/g, '$1')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/^>\s?/gm, '')
    .trim();
}

module.exports = {
  CONFIG, SALES_PIPELINE_ID, STAGES, STAGE_NAME, GUARDIAN_STAGE_IDS, INTENT_CLASSES, INTENT_LABEL,
  REPLY_INTENTS, STOP_INTENTS, RON_STOP_INTENTS, SIGNAL_INTENTS, APPROVED_URLS,
  CHANNEL_KEY, CHANNEL_LABEL, CHANNEL_SEND_TYPE, META_CHANNELS, DEFAULT_FAQ, PLAYBOOK_RULES, INVALID_DRAFT_MARK,
  guardianScreen, threadState, windowState, strikeStep, detectCallRequest, decideAction,
  buildClassifierPrompt, parseClassification, threadForPrompt, buildDraftPrompt, parseDraft,
  validateDraft, formatCard, validateCard, expiryFor, stepLabel, crClock, crDay, ago,
  verdictLine, formatDigest, evaluateWatchdog, median, pickSendText, unSlack, channelKey,
};
