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
  // Ron, 2026-11-17: a strike Sebastián does not touch goes out on its own
  // (week 1: 24 of 31 strikes expired unsent). Replies stay human.
  autoStrikeAfterMs: 2 * H,
  autoStrikesPerDay: 30,
  autoStrikesPerRun: 5,
  autoStrikeWindowMarginMs: 10 * MIN, // never send into a Meta window about to close
  autoReplyAfterMs: 15 * MIN,       // simple replies Max sends himself (off by default)
  autoRepliesPerDay: 40,
  // Reply and signal cards: a nudge in the card's thread when nobody answered.
  nudgeAtMin: [15, 60],
  runEveryMin: 15,
  unansweredAfterMs: 2 * H,        // watchdog: a lead who wrote and waited longer
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
// The lead's own WhatsApp Business greeting answering our message (2026-11-12:
// "Gracias por comunicarte con ..., ¿Cómo podemos ayudarte?" 8 s after a backlog
// message became a reply card the classifier called "unclear"). Deterministic,
// so it never depends on the model: only one waiting message, auto-reply wording,
// and either it came within 2 min of our message or it says it is automatic.
const AUTO_REPLY_RE = /gracias por (comunicarte|comunicarse|contactarnos|contactarme|escribirnos|escribirme|tu mensaje|su mensaje)|(te|le) (responderemos|contestaremos|atenderemos)|nos pondremos en contacto|en breve (te|le|nos)|mensaje autom[aá]tico|respuesta autom[aá]tica|fuera (de la oficina|del horario)|horario de atenci[oó]n|thanks? (you )?for (contacting|reaching out|your message)|we will (get back|reply)|auto.?reply/i;
const AUTO_REPLY_GAP_MS = 2 * MIN;
function isBusinessAutoReply(analysis) {
  if (!analysis || analysis.waitingCount !== 1) return false;
  const text = String(analysis.quote || '');
  if (!AUTO_REPLY_RE.test(text)) return false;
  if (/autom[aá]tic/i.test(text)) return true;
  const tail = (analysis.tail || []).slice().sort((a, b) => msgTime(a) - msgTime(b));
  const prevOut = tail.filter((m) => m.direction === 'outbound' && msgTime(m) <= analysis.anchorAt).pop();
  return !!prevOut && analysis.anchorAt - msgTime(prevOut) <= AUTO_REPLY_GAP_MS;
}

// The lead's waiting run holds a voice note, image or file (Ron, 2026-11-18:
// Max cannot hear or see it, so it goes to Sebastián as soon as it arrives).
function waitingHasMedia(analysis) {
  if (!analysis) return false;
  const since = analysis.lastAnswerAt || 0;
  return (analysis.tail || []).some((m) => m && m.direction === 'inbound' && msgTime(m) > since
    && ((Array.isArray(m.attachments) && m.attachments.length > 0) || !String(msgBody(m) || '').trim()));
}

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
    const chan = channelKey(analysis.channel);
    // Media skips the settle wait: no draft is written, Sebastián gets it now.
    if (waitingHasMedia(analysis)) return { kind: 'signal', step: 'signal', reason: 'media', channel: chan };
    if (now - analysis.newestAt < CONFIG.settleMs) return { kind: 'skip', step: 'reply', reason: 'settling', channel: chan };
    if (STOP_INTENTS.has(intent)) return { kind: 'stop', step: 'stop', reason: intent, channel: chan, ron: RON_STOP_INTENTS.has(intent) };
    const wantsCall = signals.callRequest === true || detectCallRequest(analysis.quote);
    if (wantsCall) return { kind: 'signal', step: 'signal', reason: 'call_request', channel: chan };
    // Noise (a business auto-reply, thanks or emoji only) never reaches the setter
    // (Ron, 2026-11-11: pass him the signal, drop the noise).
    if (verdict === 'noise') return { kind: 'skip', step: 'reply', reason: 'noise', channel: chan };
    if (isBusinessAutoReply(analysis)) return { kind: 'skip', step: 'reply', reason: 'auto_reply', channel: chan };
    // A hot question the FAQ answers gets a ready draft instead of a blank signal
    // (2026-11-10, "Cómo funciona paso a paso?" reached Sebastián with no draft).
    const faqAnswerable = (intent === 'question' || intent === 'wants_info') && signals.faqCovered === true;
    if (verdict === 'hot' && !faqAnswerable) return { kind: 'signal', step: 'signal', reason: 'hot', channel: chan };
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
  // Ron's setter structure for new leads (2026-11-18): type of business first, then the client.
  'FLUJO para quien escribe, un paso por mensaje y solo el que falte: (1) saludo si todavía no lo saludamos en esta conversación; (2) el tipo de negocio: "Pregunta, su negocio es de coaching o servicios?"; (3) cuando ya contestó algo, el mensaje de calificación de la plantilla (abajo), quitando lo que ya dijo; (4) con esa información, la llamada. Use lo que el prospecto ya dijo: nunca pregunte algo que ya contestó, y salte el paso cuyo dato ya tiene (si dijo "bienes raíces", no pregunte coaching o servicios).',
  // Blind test 2026-11-19: mid-conversation Max restarted the flow from step 2.
  'A mitad de una conversación, responda primero a lo ÚLTIMO que dijo el prospecto. Si ya le preguntamos algo y no lo contestó, no repita la misma pregunta: retome desde lo que sí dijo o pregúntelo con otras palabras. Si habla de otra cosa que no es su negocio (un lugar, algo personal, una cita), conteste eso corto y no meta la calificación.',
  'Cuando ya sabe qué vende y a quién, proponga revisar su caso en una llamada corta, con DOS horarios concretos de la agenda (la lista HORARIOS LIBRES del mensaje). Nunca invente un horario. Si no hay horarios en el mensaje, pregunte si prefiere mañana en la mañana o en la tarde.',
  'Si el prospecto elige un horario, confirme corto que lo agenda y nada más: el equipo lo agenda.',
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

/**
 * Thread as the drafter and the reviewer read it: the whole chat, oldest
 * first, roles in Spanish, each line dated. Blind test 2026-11-18: with only
 * the last 12 messages Max asked Gabriel what he had already answered (Ron,
 * 2026-11-19: "this cannot happen"). Capped at THREAD_MAX_MESSAGES so one
 * huge thread cannot blow the prompt.
 */
const THREAD_MAX_MESSAGES = 80;
function threadForPrompt(messages) {
  return (messages || []).filter((m) => m && REAL_TYPES.has(m.messageType)).slice().sort((a, b) => msgTime(a) - msgTime(b)).slice(-THREAD_MAX_MESSAGES)
    .map((m) => {
      const who = m.direction === 'inbound' ? 'PROSPECTO'
        : AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase()) ? 'NOSOTROS (automático)' : 'NOSOTROS';
      const body = msgBody(m).replace(/\s+/g, ' ').slice(0, 600);
      const t = msgTime(m);
      const date = Number.isFinite(t) && t > 0 ? `[${crDay(t).slice(5).split('-').reverse().join('/')}] ` : '';
      return body ? `${date}${who}: ${body}` : null;
    }).filter(Boolean).join('\n');
}

// First reply = nobody on our side has written by hand yet (workflow and
// template sends do not count). Ron, 2026-11-18: only these may go out on
// their own after 15 min; a conversation Sebastián already carries stays his.
function isFirstReply(messages, { isAutomatedBody = () => false } = {}) {
  return !(messages || []).some((m) => m && m.direction === 'outbound' && REAL_TYPES.has(m.messageType)
    && !AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase()) && !isAutomatedBody(msgBody(m)));
}

// ── Second read (Ron, 2026-11-19) ───────────────────────────────────────────
// A separate model call reads the whole chat and the draft and answers: does
// the draft ask something already answered, misread the lead, miss an odd
// word (a typo or dictation slip like "te abuso" for "te aviso"), or ignore
// where the conversation is? Any yes, or confidence below high, and the draft
// never goes out on its own: it reaches Sebastián marked with the doubt.
// Stored in guardian_actions.last_error while the card is open (no migration).
const REVIEW_OK = 'revisado';
const REVIEW_OK_FIRST = 'revisado:primera';
const DOUBT_PREFIX = 'duda: ';

function buildReviewPrompt({ thread, draft, contactName, channel }) {
  const system = `Usted revisa un mensaje que el equipo comercial de NeuroGrowth está por enviar por ${CHANNEL_LABEL[channel] || 'chat'} a un prospecto. Lea TODA la conversación con cuidado antes de juzgar. Su trabajo es atrapar errores que una persona nunca cometería.

REGLAS DE LA CASA (son correctas, nunca las marque como error)
- Siempre de usted, aunque el prospecto tutee o escriba informal.
- El flujo es: saludo, después "Pregunta, su negocio es de coaching o servicios?" (es la pregunta oficial, no una suposición), después la lista de calificación (nicho, servicio, precio por venta, cliente ideal), después la llamada con dos horarios de la agenda. Un paso cuyo dato ya existe SE SALTA: si el prospecto ya dijo su rubro, no hace falta la pregunta oficial. Nunca exija un paso que ya está contestado.
- Ejemplo correcto: PROSPECTO "Ok te abuso" (quiso decir "te aviso"), borrador "Perfecto, quedo pendiente!": responde al sentido, todo false.
- Mensajes cortos, sin signo de apertura (sin ¿ ni ¡). El estilo no es su trabajo: hay otro control para eso.

Responda cuatro preguntas, solo con lo que está en la conversación:
1. repite: el borrador pregunta algo que el prospecto YA CONTESTÓ (tipo de negocio, qué vende, a quién, precio, si quiere la llamada, un horario)? Volver a preguntar algo que nunca contestó NO es repetir.
2. malinterpreta: el borrador entiende mal lo que el prospecto dijo o quiso decir?
3. palabra_rara: el prospecto usó una palabra que no tiene sentido en contexto (error de dedo o de dictado, por ejemplo "te abuso" por "te aviso") y el borrador la tomó literal o respondió a la palabra equivocada? Si el borrador responde al sentido correcto, es false.
4. contexto: el borrador ignora dónde va la conversación (vuelve a saludar a mitad de la charla, retrocede un paso que ya pasó, no responde a lo último que dijo, o trata como prospecto a alguien que claramente habla de otra cosa)?

Después liste en ya_sabemos lo que el prospecto ya dijo (corto), y dé su confianza en que una persona del equipo, siguiendo las reglas de la casa, enviaría este borrador tal cual: alta, media o baja. Alta si las cuatro respuestas son false y el mensaje sirve; no baje la confianza por detalles de estilo.

FORMATO: solo un objeto JSON, sin texto alrededor:
{"ya_sabemos": "...", "repite": false, "malinterpreta": false, "palabra_rara": false, "contexto": false, "confianza": "alta", "nota_es": "si algo falla, qué falla en una frase; si no, vacío"}`;
  const user = [
    `Prospecto: ${contactName || 'sin nombre'}`,
    '',
    'Conversación completa (más antigua primero):',
    thread || '(sin mensajes)',
    '',
    'Borrador a revisar:',
    String(draft || ''),
  ].join('\n');
  return { system, user };
}

/** Parse the reviewer's JSON. Never throws; anything unreadable is a doubt. */
function parseReview(text) {
  const raw = String(text || '');
  const start = raw.indexOf('{');
  const end = raw.lastIndexOf('}');
  const fail = (why) => ({ parsed: false, pass: false, note_es: why, known_es: '' });
  if (start < 0 || end <= start) return fail('no se pudo revisar');
  try {
    const j = JSON.parse(raw.slice(start, end + 1));
    const flag = (k) => j[k] === true || String(j[k]).toLowerCase() === 'true';
    const flags = { repeats: flag('repite'), misreads: flag('malinterpreta'), oddWord: flag('palabra_rara'), lostContext: flag('contexto') };
    const confidence = ['alta', 'media', 'baja'].includes(String(j.confianza || '').toLowerCase()) ? String(j.confianza).toLowerCase() : 'baja';
    const pass = !flags.repeats && !flags.misreads && !flags.oddWord && !flags.lostContext && confidence === 'alta';
    let note = String(j.nota_es || '').replace(/\s+/g, ' ').trim().slice(0, 200);
    if (!pass && !note) {
      note = flags.repeats ? 'pregunta algo que ya contestó' : flags.misreads ? 'entiende mal lo que dijo'
        : flags.oddWord ? 'hay una palabra rara sin resolver' : flags.lostContext ? 'no sigue la conversación' : `confianza ${confidence}`;
    }
    return { parsed: true, pass, ...flags, confidence, note_es: pass ? '' : note, known_es: String(j.ya_sabemos || '').replace(/\s+/g, ' ').trim().slice(0, 300) };
  } catch (_) { return fail('no se pudo revisar'); }
}

/** The last_error marker an open card carries after the second read. */
function reviewMark({ pass, note_es }, { firstReply = false } = {}) {
  if (!pass) return `${DOUBT_PREFIX}${note_es || 'sin detalle'}`;
  return firstReply ? REVIEW_OK_FIRST : REVIEW_OK;
}
const reviewPassed = (row) => !!row && typeof row.last_error === 'string' && row.last_error.startsWith(REVIEW_OK);
const reviewDoubt = (row) => (row && typeof row.last_error === 'string' && row.last_error.startsWith(DOUBT_PREFIX)) ? row.last_error.slice(DOUBT_PREFIX.length) : null;

const STEP_INSTRUCTION = {
  reply: 'Siga el FLUJO: reaccione corto a lo que dijo y dé el siguiente paso que falte, uno solo.',
  clarify: 'No se entiende qué necesita (puede ser un saludo o algo suelto). Salude si todavía no hubo saludo y pregunte por el tipo de negocio, corto y cálido.',
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
function buildDraftPrompt({ action, thread, contactName, intentLabel, stageName, faq, channel, bookingUrl = APPROVED_URLS.booking, guideUrl = APPROVED_URLS.guide, slots = [], greeted = null }) {
  const system = `Usted escribe por ${CHANNEL_LABEL[channel] || 'chat'} como el equipo comercial de NeuroGrowth (Costa Rica). Un setter humano revisa y aprueba cada mensaje antes de enviarlo.

VOZ (Ron, 2026-11-18: tiene que leerse como escrito por una persona)
- Español de Costa Rica, siempre de usted. Nunca tú, vos, tienes, puedes, quieres.
- Cálido y corto, como un setter que escribe desde el teléfono: una o dos frases, máximo 300 caracteres. La única excepción es el mensaje de calificación de la plantilla. Por correo igual de corto.
- Salude por el nombre la primera vez ("Hola Marco, cómo le va?" o "Hola Marco, todo bien!"). Si no hay nombre, solo "Hola". No vuelva a saludar si ya hubo saludo.
- Reaccione a lo que dijo con una o dos palabras antes de preguntar ("Excelente.", "Perfecto.", "Entiendo."), y siga desde su respuesta.
- Puede cerrar UNA frase afirmativa con signo de exclamación ("Excelente!", "todo bien!"). Una pregunta termina en "?", nunca en "!" (nunca "cómo le va!"). Signos (regla de Ron, 2026-11-10): nunca ponga ¿ ni ¡ al inicio; solo el signo del final. Ejemplo: "A qué se dedica su negocio?"
- Nunca suene a plantilla o a máquina: nada de "Gracias por escribir", "Con gusto le cuento más", "Para darle información que le sirva", "no algo genérico", "Por cierto", "Estimado", "Quedo atento a sus comentarios". Nada de explicar el servicio si no lo preguntó.
- Sin emojis. Sin guion largo.
- Close frame: usted evalúa si el prospecto califica. No ruega, no persigue, no vende por chat.
- Nunca use: leads, agencia (para nosotros), ticket, ICP, setter, closer, triage, funnel, embudo. Diga: lo que cobra, su cliente ideal, reuniones con quien decide, un sistema en su LinkedIn.

PLANTILLA DE CALIFICACIÓN (paso 3 del flujo; quite las líneas que el prospecto ya contestó)
Para entender mejor su situación y ayudarle de la mejor forma posible, me podría compartir esta información:
• nicho del servicio
• el servicio que usted vende
• precio por venta del servicio
• perfil de su cliente ideal
Con esta información puedo revisar su caso y ver si realmente le podemos ayudar con nuestro método.

EJEMPLOS (así escribe el equipo; imite el tono, no copie los datos)
PROSPECTO: Buenas, podrían darme más información?
NOSOTROS: Hola Marco, cómo le va? Pregunta, su negocio es de coaching o servicios?
PROSPECTO: Servicios, bienes raíces
NOSOTROS: Excelente! Para entender mejor su situación y ayudarle de la mejor forma posible, me podría compartir esta información: (la plantilla)
PROSPECTO: Marketing y eventos, le vendo desde pymes hasta empresas grandes
NOSOTROS: Excelente. Y en este momento se está enfocando más en alguna de las dos?
PROSPECTO: Cobro unos 2,500 por proyecto, los clientes me llegan por referidos
NOSOTROS: Perfecto, su caso sí vale la pena revisarlo. Le sirve mañana jueves a las 10:00 am o el viernes a las 3:00 pm, hora Costa Rica?
PROSPECTO: no recuerdo haberles escrito
NOSOTROS: Sí, fue hace un tiempo, por lo de conseguir clientes por LinkedIn. Sigue buscando más clientes para su negocio?
PROSPECTO: Te aviso mañana
NOSOTROS: Perfecto, quedo pendiente mañana!

PLAYBOOK
${PLAYBOOK_RULES.map((r) => `- ${r}`).join('\n')}

LO QUE PUEDE AFIRMAR (FAQ)
${faq || DEFAULT_FAQ}

REGLAS FIJAS
- Nunca diga precios, planes, descuentos, garantías ni promesas de resultados o de clientes. El precio se ve en la llamada.
- Solo dos enlaces existen: el calendario del equipo (${bookingUrl}), únicamente cuando el prospecto ya dijo sí a la llamada, y la guía (${guideUrl}), únicamente en el strike 5. Ningún otro enlace.
- No invente datos del prospecto ni de NeuroGrowth. Si no sabe, pregunte.
- Nunca pida WhatsApp ni correo si el prospecto ya escribe por WhatsApp. Por Instagram o Messenger, pida su WhatsApp solo cuando proponga la llamada, para confirmarle la hora.
- Nada de texto fuera del JSON. Sin corchetes ni llaves dentro del borrador.

ANTES DE ESCRIBIR (Ron, 2026-11-19: Max no puede preguntar lo que ya le dijeron)
- Lea la conversación COMPLETA, desde el primer mensaje, no solo el último.
- Llene ya_sabemos con lo que el prospecto ya dijo: tipo de negocio, qué vende, a quién, precio, si ya le ofrecimos horarios, si aceptó la llamada. El borrador nunca pregunta nada de esa lista: avanza al paso siguiente.
- Si una palabra del prospecto no tiene sentido, casi siempre es un error de dedo o de dictado ("te abuso" es "te aviso", "orita" es "ahorita"): léala por el sentido, nunca la tome literal ni la corrija.

FORMATO DE SALIDA: solo un objeto JSON, sin texto alrededor, en este orden:
{"ya_sabemos": "lo que el prospecto ya dijo, corto", "draft": "el mensaje a enviar", "reason_es": "una frase de por qué este mensaje"}`;

  const fname = firstName(contactName);
  const user = [
    `Prospecto: ${contactName || 'sin nombre'} (llámelo solo "${fname || 'sin nombre'}"; si no hay nombre, no use ninguno)`,
    greeted === true ? 'Ya lo saludamos en esta conversación: NO vuelva a saludar. Siga directo desde lo que dijo.' : null,
    greeted === false ? `Todavía no lo saludamos: empiece con "Hola${fname ? ` ${fname}` : ''}, cómo le va?" o "Hola${fname ? ` ${fname}` : ''}, todo bien!".` : null,
    `Etapa en el pipeline: ${stageName || 'desconocida'}`,
    `Intención detectada: ${intentLabel || 'silencio'}`,
    `Paso: ${action.step}. ${STEP_INSTRUCTION[action.step] || STEP_INSTRUCTION.reply}`,
    action.askContact ? 'Este prospecto está en Instagram o Messenger sin WhatsApp conocido: si propone la llamada, pídale su WhatsApp para confirmarle la hora. Si todavía no, no lo pida.' : null,
    slots && slots.length ? `HORARIOS LIBRES de la agenda (hora Costa Rica), úselos solo si propone la llamada: ${slots.join(' · ')}` : 'HORARIOS LIBRES: no disponibles ahora. Si propone la llamada, pregunte si prefiere mañana en la mañana o en la tarde.',
    action.movedToEmail ? 'La ventana de Meta cerró: este mensaje va por correo. Empiece con una línea que explique por qué le escribe por aquí.' : null,
    '',
    'Conversación (más antigua primero):',
    thread || '(sin mensajes)',
  ].filter((l) => l !== null).join('\n');
  return { system, user };
}

// Ron, 2026-11-10: no opening ¿ or ¡ in any message, only the closing sign.
// Stripped here so a model slip never costs Sebastián a card; the validator
// still checks, in case a draft reaches it some other way.
function stripOpeningMarks(text) {
  return String(text || '').replace(/[¿¡]/g, '');
}

/** Parse the drafter's JSON. Never throws; a broken reply becomes { ok: false }. */
function parseDraft(text) {
  const raw = String(text || '');
  const start = raw.indexOf('{');
  const end = raw.lastIndexOf('}');
  if (start < 0 || end <= start) return { ok: false, error: 'no JSON object' };
  try {
    const j = JSON.parse(raw.slice(start, end + 1));
    return { ok: true, draft: stripOpeningMarks(String(j.draft || '')).trim(), reason_es: String(j.reason_es || '').trim().slice(0, 200), known_es: String(j.ya_sabemos || '').replace(/\s+/g, ' ').trim().slice(0, 300) };
  } catch (e) { return { ok: false, error: `bad JSON: ${e.message}` }; }
}

// ── Criteria (4a) ───────────────────────────────────────────────────────────
const TU_RE = /(^|[^a-záéíóúñü])(tú|tienes|puedes|quieres)([^a-záéíóúñü]|$)/i;
const BANNED_WORDS = ['leads', 'lead', 'ticket', 'icp', 'setter', 'closer', 'triage', 'funnel', 'embudo', 'garantizo', 'garantizamos', 'garantizado', 'garantía', 'descuento'];
const PLACEHOLDER_RE = /\[|\{\{|\bundefined\b|\bnull\b|\[object Object\]/;
const PRICE_RE = /\$\s?\d|\d\s?\$|\d\s?usd\b|\busd\s?\d/i;

// Phrases that make a chat read like a template or a bot (Ron, 2026-11-18:
// "they have to appear very human made"). Taken from week-1 drafts.
const ROBOTIC_PHRASES = ['gracias por escribir', 'con gusto le cuento', 'información que le sirva', 'informacion que le sirva', 'algo genérico', 'algo generico', 'información genérica', 'por cierto', 'estimado', 'quedo atento a sus comentarios', 'para darle información'];
const CHAT_CHANNELS = new Set(['whatsapp', 'instagram', 'facebook']);
const CHAT_MAX_CHARS = 300;

/** validateDraft(text, { allowedUrls, channel }) → { ok, problems }. */
function validateDraft(text, { allowedUrls = Object.values(APPROVED_URLS), channel = null } = {}) {
  const problems = [];
  const t = String(text || '');
  if (!t.trim()) return { ok: false, problems: ['empty draft'] };
  if (t.length > CONFIG.maxDraftChars) problems.push(`too long (${t.length} chars)`);
  // Chat stays short; only the qualification template (a bullet list) may run longer.
  else if (CHAT_CHANNELS.has(channel) && !t.includes('•') && t.length > CHAT_MAX_CHARS) problems.push(`too long for chat (${t.length} chars, max ${CHAT_MAX_CHARS})`);
  const paragraphs = t.split(/\n\s*\n/).map((p) => p.trim()).filter(Boolean);
  if (paragraphs.length > CONFIG.maxDraftParagraphs) problems.push(`${paragraphs.length} paragraphs`);
  if (/\u2014/.test(t)) problems.push('em dash');
  if (PLACEHOLDER_RE.test(t)) problems.push('placeholder');
  if (TU_RE.test(t)) problems.push('uses tú');
  if (PRICE_RE.test(t)) problems.push('mentions a price');
  // Ron, 2026-11-18: a closing "!" is fine ("Excelente!"), never the opening one.
  if (/¡/.test(t)) problems.push('opening exclamation mark');
  if ((t.match(/!/g) || []).length > 1) problems.push('more than one exclamation mark');
  if (/(c[oó]mo (le|te) va|c[oó]mo est[aá]|qu[eé] tal)\s*!/i.test(t)) problems.push('exclamation on a question');
  if (/¿/.test(t)) problems.push('opening question mark');
  const lower = t.toLowerCase();
  for (const p of ROBOTIC_PHRASES) if (lower.includes(p)) problems.push(`sounds like a template ("${p}")`);
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
// Open cards nobody closes (2026-11-12): a card stays 'drafted' or 'signal' when
// Sebastián answers the lead from his phone or GHL instead of tapping it, or
// never acts. An open card also blocks any new card for that lead (one open card
// per conversation), so the run closes them. Pure.
//   row          guardian_actions row (status, kind, mode, created_at, window_closes_at)
//   answeredAtMs first human answer in the thread after the card, or null
// Returns { status, reason, approvedAt? } or null (leave it open).
//   draft mode, answered      -> handled (his reply counts like a ✅)
//   draft mode, past deadline -> expired (signal: 72 h; draft: Meta window, or 24 h without one)
//   shadow mode, past deadline -> skipped (nobody was meant to act on it)
const SIGNAL_CLOSE_MS = 72 * H;
function closeDecision(row, now, answeredAtMs = null) {
  if (!row || !['drafted', 'signal'].includes(row.status)) return null;
  const created = Date.parse(row.created_at);
  if (!Number.isFinite(created)) return null;
  const draftMode = row.mode === 'draft';
  if (draftMode && Number.isFinite(answeredAtMs) && answeredAtMs > created) {
    return { status: 'handled', approvedAt: answeredAtMs, reason: 'contestado directo en GHL o el teléfono' };
  }
  const deadline = row.kind === 'signal' ? created + SIGNAL_CLOSE_MS
    : (row.window_closes_at ? Date.parse(row.window_closes_at) : created + CONFIG.noWindowExpiryMs);
  if (!Number.isFinite(deadline) || now < deadline) return null;
  if (!draftMode) return { status: 'skipped', reason: 'tarjeta de sombra cerrada sin acción' };
  return { status: 'expired', reason: row.kind === 'signal' ? 'señal sin respuesta en 72 h' : 'nadie la aprobó antes de que cerrara la ventana' };
}

// Greeting state (blind test 2026-11-18: Max greeted again mid-conversation).
// Greeted = one of our messages in the last 14 days opens with a greeting
// (Sebastián, a strike template or the backlog template all greet by name).
const GREETING_RE = /^\s*(hola|buen[oa]s|buen d[ií]a|qu[eé] tal)\b/i;
// Blind test 2026-11-19: Max still re-greeted mid-conversation when our earlier
// messages did not open with "Hola". Any message we wrote by hand in the last
// 14 days means the conversation is running: no new greeting.
function alreadyGreeted(messages, now = Date.now()) {
  return (messages || []).some((m) => m && m.direction === 'outbound' && REAL_TYPES.has(m.messageType)
    && now - msgTime(m) <= 14 * 24 * H
    && (GREETING_RE.test(msgBody(m)) || !AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase())));
}
// "Marco-vinicio Pérez" -> "Marco". The first name only, as a person writes it.
function firstName(name) {
  const f = String(name || '').trim().split(/[\s\-_.]+/)[0] || '';
  return f ? f.charAt(0).toUpperCase() + f.slice(1).toLowerCase() : '';
}

// ── Calendar (Ron, 2026-11-18: "Max should query our calendar and provide the
// availability"). slotsByDay: GHL /calendars/{id}/free-slots body, keyed by
// YYYY-MM-DD with { slots: [iso] }. Picks two real times at least 3 h out:
// the first free one, then one on a later day in the other half of the day
// when it exists (a morning and an afternoon read like a person offering).
const DIAS = ['domingo', 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado'];
function pickTwoSlots(slotsByDay, now, { minLeadMs = 3 * H } = {}) {
  const all = [];
  for (const [k, v] of Object.entries(slotsByDay || {})) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(k) || !v || !Array.isArray(v.slots)) continue;
    for (const s of v.slots) { const ms = Date.parse(s); if (Number.isFinite(ms) && ms - now >= minLeadMs) all.push(ms); }
  }
  all.sort((a, b) => a - b);
  if (!all.length) return [];
  const first = all[0];
  const hourCr = (ms) => new Date(ms - CR_OFFSET_MS).getUTCHours();
  const pm = (ms) => hourCr(ms) >= 12;
  const later = all.filter((ms) => crDay(ms) !== crDay(first));
  const second = later.find((ms) => pm(ms) !== pm(first)) || later[0] || all.find((ms) => ms - first >= 2 * H);
  return second ? [first, second] : [first];
}
function slotLabel(ms, now) {
  const d = new Date(ms - CR_OFFSET_MS);
  const h24 = d.getUTCHours(), min = d.getUTCMinutes();
  const h12 = ((h24 + 11) % 12) + 1;
  const time = `${h12}:${String(min).padStart(2, '0')} ${h24 < 12 ? 'am' : 'pm'}`;
  const dayDiff = Math.round((Date.parse(`${crDay(ms)}T00:00:00Z`) - Date.parse(`${crDay(now)}T00:00:00Z`)) / (24 * H));
  const day = dayDiff === 0 ? 'hoy' : dayDiff === 1 ? `mañana ${DIAS[d.getUTCDay()]}` : `el ${DIAS[d.getUTCDay()]}`;
  return `${day} a las ${time}`;
}

// Reply card Max may answer himself after 15 min (Ron, 2026-11-18). Built but
// OFF until Ron approves the blind test (GUARDIAN_AUTO_REPLIES=on). Only "asked
// for information" and "unclear" in text; hot leads, objections, price, call
// requests and media stay with Sebastián.
const AUTO_REPLY_INTENTS = new Set(['wants_info', 'unclear']);
function autoReplyDue(row, now) {
  if (!row || row.kind !== 'reply' || row.status !== 'drafted' || row.mode !== 'draft') return false;
  if (!AUTO_REPLY_INTENTS.has(row.intent) || row.problems) return false;
  // Only a first reply the second read passed with high confidence (Ron, 2026-11-19).
  if (row.last_error !== REVIEW_OK_FIRST) return false;
  const created = Date.parse(row.created_at);
  if (!Number.isFinite(created) || now - created < CONFIG.autoReplyAfterMs) return false;
  if (row.window_closes_at && Date.parse(row.window_closes_at) - now <= CONFIG.autoStrikeWindowMarginMs) return false;
  return true;
}

// Strike card due to go out on its own (Ron, 2026-11-17). The caller still
// checks the live thread (guardianSendApproved) and the daily cap.
function autoSendDue(row, now) {
  if (!row || row.kind !== 'strike' || row.status !== 'drafted' || row.mode !== 'draft') return false;
  if (row.problems || !reviewPassed(row)) return false;
  const created = Date.parse(row.created_at);
  if (!Number.isFinite(created) || now - created < CONFIG.autoStrikeAfterMs) return false;
  if (row.window_closes_at && Date.parse(row.window_closes_at) - now <= CONFIG.autoStrikeWindowMarginMs) return false;
  return true;
}

// Minutes mark (15 or 60) when a reply or signal card still nobody answered
// crosses it during this run, else null. Stateless: the run is every 15 min,
// so each mark falls in exactly one run's window.
function nudgeDue(row, now) {
  if (!row || row.mode !== 'draft' || !['reply', 'signal'].includes(row.kind) || !['drafted', 'signal'].includes(row.status)) return null;
  const ageMin = (now - Date.parse(row.created_at)) / MIN;
  if (!Number.isFinite(ageMin)) return null;
  for (const mark of CONFIG.nudgeAtMin) if (ageMin >= mark && ageMin < mark + CONFIG.runEveryMin) return mark;
  return null;
}

function nudgeText(row, mark) {
  const waited = mark >= 60 ? `${Math.round(mark / 60)} h` : `${mark} min`;
  const base = `⏰ ${row.contact_name || 'El prospecto'} sigue esperando respuesta (${waited} desde la tarjeta). Contéstele desde su teléfono o GHL.`;
  // Ron, 2026-11-19: the reminder carries Max's suggestion, one tap away.
  const draft = String(row.draft || '').trim();
  if (row.kind !== 'reply' || !draft || row.problems) return base;
  const doubt = reviewDoubt(row);
  return `${base}\nSugerencia de Max: ${draft}\n${doubt ? `⚠️ Max no está seguro: ${doubt}. Revísela antes del ✅.` : '✅ en la tarjeta la envía tal cual.'}`;
}

// Milliseconds of [a, b) that fall in the guardian's open hours, 08:00 to 20:00
// CR, every day of the week (Ron, 2026-11-18: "Monday to Monday").
function openMsBetween(a, b) {
  if (!(Number.isFinite(a) && Number.isFinite(b) && b > a)) return 0;
  let total = 0;
  for (let day = Date.parse(`${crDay(a)}T00:00:00-06:00`); day < b; day += DAY) {
    const open = day + CONFIG.pokeFromHour * H;
    const close = day + CONFIG.pokeToHour * H;
    total += Math.max(0, Math.min(b, close) - Math.max(a, open));
  }
  return total;
}

// Watchdog (Ron, 2026-11-17): the share of leads who WROTE (reply cards, draft
// mode) and waited more than 2 h for a human answer. Replaces "cards expired",
// which counted strikes nobody was meant to answer fast. rows: 7 days of
// guardian_actions. Since 2026-11-18 (Ron): one lead message counts once (when
// the WhatsApp window closes Max re-cards the same message as email, and both
// cards used to count), and the 2 h are open hours, 08:00 to 20:00 CR every day,
// so a 19:50 message answered at 08:15 waited 25 minutes, not 12 hours.
// A lead message still unanswered and under 2 open hours old is not judged yet.
function unansweredShare(rows, now) {
  const leads = new Map();
  let n = 0;
  for (const r of rows || []) {
    n++;
    if (r.kind !== 'reply' || r.mode !== 'draft') continue;
    if (['skipped', 'stopped', 'rejected'].includes(r.status)) continue;
    const created = Date.parse(r.created_at);
    if (!Number.isFinite(created)) continue;
    const leadAt = Date.parse(r.lead_message_at);
    const key = r.contact_id && Number.isFinite(leadAt) ? `${r.contact_id}|${leadAt}` : `row|${r.id || n}`;
    const answeredAt = r.status === 'sent' ? Date.parse(r.sent_at) : r.status === 'handled' ? Date.parse(r.approved_at) : NaN;
    const lead = leads.get(key) || { start: Infinity, answered: Infinity };
    lead.start = Math.min(lead.start, created);
    if (Number.isFinite(answeredAt)) lead.answered = Math.min(lead.answered, answeredAt);
    leads.set(key, lead);
  }
  let judged = 0, late = 0;
  for (const { start, answered } of leads.values()) {
    const done = Number.isFinite(answered);
    const waited = openMsBetween(start, done ? answered : now);
    if (!done && waited < CONFIG.unansweredAfterMs) continue;
    judged++;
    if (!done || waited > CONFIG.unansweredAfterMs) late++;
  }
  return judged ? late / judged : null;
}

// The draft of a light reply card goes in its thread (Ron, 2026-11-17).
function replyDraftThread(action) {
  const body = String(action.draft || '').trim();
  if (body && !(action.problems && action.problems.length)) {
    const doubt = action.doubt ? `\n⚠️ Max no está seguro: ${action.doubt}. Revísela antes del ✅; Max no la enviará solo.` : '';
    return `Sugerencia de Max (✅ en la tarjeta la envía tal cual; responda en este hilo para enviar su texto):\n${body}${doubt}`;
  }
  const why = (action.problems && action.problems.length) ? ` (${action.problems.join('; ')})` : '';
  return `Sugerencia de Max: ${INVALID_DRAFT_MARK}${why}. Responda en este hilo con su texto y marque ✅ para enviarlo.`;
}

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
  // Light reply card (Ron, 2026-11-17): Sebastián answers from his phone, so
  // the card says who wrote, what they want and how long they wait; Max's
  // draft goes in the thread (replyDraftThread) and ✅ still sends it.
  if (action.kind === 'reply') {
    lines.push(LIGHT_REPLY_LINE);
    const exp = expiryFor(action, now);
    const windowNote = action.closesAt ? `ventana de Meta cierra ${crClock(action.closesAt)}` : 'sin ventana';
    lines.push(`Vence: ${crClock(exp)} CR (${windowNote})`);
    lines.push('✅ envía la sugerencia del hilo · ❌ descartar · Max le recuerda a los 15 min y a la hora');
    lines.push(`🔗 ${action.contactLink}`);
    return lines.join('\n');
  }
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
  if (action.kind === 'strike' && body && !(action.problems && action.problems.length)) {
    lines.push(action.doubt ? `⚠️ Max no está seguro: ${action.doubt}. No sale sola: revísela.` : AUTO_STRIKE_LINE);
  }
  lines.push(`🔗 ${action.contactLink}`);
  return lines.join('\n');
}
const LIGHT_REPLY_LINE = 'Contéstele desde su teléfono o GHL. Sugerencia de Max: en el hilo.';
const AUTO_STRIKE_LINE = 'Si no la toca en 2 h, Max la envía sola (❌ para que no salga).';

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
  const light = lines.includes(LIGHT_REPLY_LINE); // light reply card: the draft lives in the thread
  if (light) { /* no body line by design */ }
  else if (bodyIdx < 0) problems.push('missing Borrador/Sugerencia');
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
// The check cron is */15 8-20 CR: 13 hours x 4 = 52 runs a day.
const RUNS_PER_DAY = 52;
const OPEN_HOUR = 8;
// Runs that should have finished by nowMs (CR), allowing graceMin for a run
// to complete and log. The grace comes off today's clock, never off the date:
// taken off the date, the 00:05 read landed on 23:55 of the day before and
// asked the new day (0 runs) for all 52, a false red every midnight.
function expectedRuns(nowMs, graceMin = 10) {
  const d = new Date(nowMs - CR_OFFSET_MS);
  const mins = d.getUTCHours() * 60 + d.getUTCMinutes() - graceMin;
  if (mins < OPEN_HOUR * 60) return 0;
  return Math.min(RUNS_PER_DAY, Math.floor((mins - OPEN_HOUR * 60) / 15) + 1);
}
// Minutes from a lead's message to its card, counted from 08:00 CR when the
// message came in overnight: the guardian is closed 20:00 to 08:00 by design.
function cardLagMinutes(leadMs, cardMs) {
  const open = Date.parse(`${crDay(cardMs)}T0${OPEN_HOUR}:00:00-06:00`);
  return Math.max(0, (cardMs - Math.max(leadMs, open)) / MIN);
}
// m: { runsToday, capStreak, cardsToday, isWeekday, inboundToday, approvals,
//      rejections, medianMinutes, expiredShare, sentInShadowOrOff,
//      sentWithoutApproval, divergence, daysActive, maxDraftsPerRun,
//      expectedRuns (runs due so far; defaults to the full day) }
// Returns [{ metric, level, text }], one per metric; warmup rows (n < 7 days)
// come back green with the text "warmup".
function evaluateWatchdog(m) {
  const out = [];
  const warm = (Number(m.daysActive) || 0) >= 7;
  const push = (metric, level, text) => out.push({ metric, level, text });
  const gated = (metric, level, text) => (warm ? push(metric, level, text) : push(metric, 'green', `${metric}: warmup (day ${m.daysActive || 0} of 7)`));

  // Judged against the runs due by now, not the full day: the watchdog runs
  // hourly, so a full-day target read every red until 20:00.
  const runs = Number(m.runsToday) || 0;
  const due = m.expectedRuns == null ? RUNS_PER_DAY : Number(m.expectedRuns) || 0;
  const missed = due - runs;
  gated('runs', missed <= 0 ? 'green' : missed <= 4 ? 'yellow' : 'red', `runs completed today: ${runs} of ${due} due so far (red when more than 4 missed)`);
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
  // Since 2026-11-17: leads who wrote and waited over 2 h for a human answer.
  // "Cards expired" counted strikes and signals too and read red all week while
  // Sebastián answered from his phone.
  const un = m.unansweredShare;
  gated('unanswered', un == null || un <= 0.10 ? 'green' : un <= 0.25 ? 'yellow' : 'red', `leads who wrote and waited over 2 h (8 AM to 8 PM) for an answer, 7d: ${un == null ? 'n/d' : `${Math.round(un * 100)}%`} (green 10%)`);
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

// Silent cards per run (Ron, 2026-10-29: the whole backlog gets its strikes
// until it strikes out). Cards whose stage moved within recentDays come first,
// newest first, so a lead silent since yesterday is never queued behind one
// silent for 100 days; the rest of the backlog rotates behind them by cursor.
function orderSilentCards(cards, { now, cursor = 0, max, recentDays = 14 }) {
  const touched = (o) => Date.parse(o.lastStageChangeAt || o.updatedAt || o.createdAt || '') || 0;
  const cutoff = now - recentDays * 24 * H;
  const recent = cards.filter((o) => touched(o) >= cutoff).sort((a, b) => touched(b) - touched(a));
  const backlog = cards.filter((o) => touched(o) < cutoff).sort((a, b) => String(a.id).localeCompare(String(b.id)));
  const silent = recent.slice(0, max);
  let next = backlog.length ? cursor % backlog.length : 0;
  const room = Math.min(max - silent.length, backlog.length);
  for (let i = 0; i < room; i++) silent.push(backlog[(next + i) % backlog.length]);
  if (backlog.length) next = (next + room) % backlog.length;
  return { silent, cursor: next, recent: recent.length, backlog: backlog.length };
}

// The daily card cap rations silence follow-ups only. A lead who wrote to us
// always gets a card, however many strikes went out earlier in the day.
function countsTowardDayCap(step) {
  return /^(strike|renurture)_/.test(String(step || ''));
}

module.exports = {
  CONFIG, SALES_PIPELINE_ID, STAGES, STAGE_NAME, GUARDIAN_STAGE_IDS, INTENT_CLASSES, INTENT_LABEL,
  REPLY_INTENTS, STOP_INTENTS, RON_STOP_INTENTS, SIGNAL_INTENTS, APPROVED_URLS,
  CHANNEL_KEY, CHANNEL_LABEL, CHANNEL_SEND_TYPE, META_CHANNELS, DEFAULT_FAQ, PLAYBOOK_RULES, INVALID_DRAFT_MARK,
  guardianScreen, threadState, windowState, strikeStep, detectCallRequest, decideAction,
  buildClassifierPrompt, parseClassification, threadForPrompt, buildDraftPrompt, parseDraft,
  validateDraft, formatCard, validateCard, expiryFor, stepLabel, crClock, crDay, ago,
  stripOpeningMarks, verdictLine, formatDigest, evaluateWatchdog, expectedRuns, cardLagMinutes, median, pickSendText, unSlack, channelKey,
  orderSilentCards, countsTowardDayCap, closeDecision, SIGNAL_CLOSE_MS,
  isFirstReply, buildReviewPrompt, parseReview, reviewMark, reviewPassed, reviewDoubt, REVIEW_OK, REVIEW_OK_FIRST, DOUBT_PREFIX, THREAD_MAX_MESSAGES,
  autoSendDue, nudgeDue, nudgeText, unansweredShare, openMsBetween, isBusinessAutoReply, AUTO_REPLY_RE, replyDraftThread, LIGHT_REPLY_LINE, AUTO_STRIKE_LINE,
  waitingHasMedia, pickTwoSlots, slotLabel, autoReplyDue, AUTO_REPLY_INTENTS, ROBOTIC_PHRASES, alreadyGreeted, firstName,
};
