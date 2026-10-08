'use strict';
// Instagram guide replies, phase A (Ron, 2026-10-07; plan-of-record §1 UPDATE
// 2026-10-07). The VSL reels close with "Comente LinkedIn"; a GHL workflow DMs
// the guide and tags the contact `ig-guia-linkedin`. When that lead writes back
// by Instagram DM, Max drafts the next reply in Ron's voice (Kai's intents, the
// chat triage of the Triage Call Script v1.0) and DMs it to Ron. Nothing reaches
// the lead until Ron reacts ✅. Phase B (autonomous on safe intents) comes only
// after Ron has read two weeks of drafts and says so.
//
// Pure: no Slack, no DB, no GHL, no Anthropic. index.js owns the I/O.
// Tested in test/ig-guide-replies.test.js.

const WINDOW_MS = 24 * 60 * 60 * 1000;          // Meta: reply only within 24 h of the lead's last message
const SETTLE_MS = 2 * 60 * 1000;                 // let a burst of messages finish before drafting
const HOT_REPLY_HOLD_MS = 12 * 60 * 60 * 1000;   // setters are not poked while Ron holds a fresh draft
const MAX_DRAFT_CHARS = 500;
const MAX_CLAIM_NAME = 'Max · Instagram';     // setter_claims name: the leaderboard row for these leads
const INTENTS = ['interested', 'wants_info', 'question', 'objection', 'positive_not_ready', 'not_interested', 'other'];
const STAGES = ['discover', 'qualified', 'booking', 'booked', 'disqualified', 'nurture', 'done'];
const FIELDS = ['offer', 'buyer', 'price', 'channel', 'monthly_clients', 'challenge', 'decision_maker'];
const FIELD_LABEL = {
  offer: 'Vende', buyer: 'A quién', price: 'Cobra', channel: 'Cómo le llegan clientes',
  monthly_clients: 'Clientes nuevos al mes', challenge: 'Reto', decision_maker: 'Decide',
};
const INTENT_LABEL = {
  interested: 'interesado', wants_info: 'pide info', question: 'pregunta', objection: 'objeción',
  positive_not_ready: 'no por ahora', not_interested: 'no le interesa', other: 'otro',
};
const AUTOMATED_SOURCES = new Set(['workflow', 'bot', 'campaign', 'bulk_actions', 'conversation_ai']);
// Only real channel messages can be a team answer. GHL logs pipeline events in
// the thread as outbound TYPE_ACTIVITY_* rows with source 'app' (the Social DM
// Intake's "Opportunity created" lands 4 s after the lead's reply), and those
// must never read as "the team already answered". Same allowlist as hotReplies.
const ANSWER_TYPES = new Set(['TYPE_WHATSAPP', 'TYPE_INSTAGRAM', 'TYPE_FACEBOOK', 'TYPE_SMS', 'TYPE_EMAIL', 'TYPE_CALL']);

const msgTime = (m) => Date.parse(m && (m.dateAdded || m.createdAt)) || 0;
const msgBody = (m) => String((m && (m.body || m.message)) || '').trim();
const lowerTags = (tags) => (Array.isArray(tags) ? tags : []).map((t) => String(t || '').trim().toLowerCase());

/** Does this GHL conversation belong to the reel guide flow? */
function isGuideConversation(convo, { tag, personalTag = 'personal' }) {
  if (!convo || convo.lastMessageType !== 'TYPE_INSTAGRAM' || convo.lastMessageDirection !== 'inbound') return false;
  const tags = lowerTags(convo.tags);
  if (tags.includes(String(personalTag).toLowerCase())) return false;
  return tags.includes(String(tag).toLowerCase());
}

/**
 * The lead message to answer: the newest inbound Instagram message with nothing
 * outbound after it that a person or this feature sent. A workflow send (the
 * guide DM itself) does not count as an answer. Null when the team already
 * answered, the burst is still settling, or the 24 h window has closed.
 */
function latestPendingInbound(messages, { now }) {
  const sorted = (messages || []).slice().sort((a, b) => msgTime(a) - msgTime(b));
  let lastIn = -1;
  sorted.forEach((m, i) => { if (m.direction === 'inbound' && m.messageType === 'TYPE_INSTAGRAM') lastIn = i; });
  if (lastIn < 0) return null;
  const answeredAfter = sorted.slice(lastIn + 1).some((m) => m.direction === 'outbound'
    && ANSWER_TYPES.has(m.messageType)
    && !AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase()));
  if (answeredAfter) return null;
  const m = sorted[lastIn];
  const at = msgTime(m);
  if (now - at < SETTLE_MS) return null;
  if (now - at >= WINDOW_MS) return null;
  // The run of lead messages since the last outbound, so a 3-message burst is one draft.
  let start = lastIn;
  const isActivity = (x) => !ANSWER_TYPES.has(x.messageType) && x.messageType !== 'TYPE_INSTAGRAM_COMMENT';
  while (start > 0 && (sorted[start - 1].direction === 'inbound' || isActivity(sorted[start - 1]))) start--;
  const burst = sorted.slice(start, lastIn + 1).filter((x) => x.direction === 'inbound' && x.messageType === 'TYPE_INSTAGRAM').map(msgBody).filter(Boolean);
  return { id: m.id, at, text: burst.join('\n') };
}

/** Milliseconds left in Meta's 24 h window; negative once closed. */
function windowRemainingMs(leadMessageAt, now) {
  return Date.parse(leadMessageAt instanceof Date ? leadMessageAt.toISOString() : leadMessageAt) + WINDOW_MS - now;
}
function fmtRemaining(ms) {
  if (ms <= 0) return 'cerrada';
  const h = Math.floor(ms / 3600000);
  const m = Math.floor((ms % 3600000) / 60000);
  return h ? `${h} h ${m} min` : `${m} min`;
}

/** Thread as the drafter reads it: oldest first, last 20, roles in Spanish. */
function threadForPrompt(messages) {
  return (messages || []).slice().sort((a, b) => msgTime(a) - msgTime(b)).slice(-20)
    .map((m) => {
      const who = m.direction === 'inbound' ? 'PROSPECTO'
        : AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase()) ? 'NOSOTROS (automático)' : 'NOSOTROS';
      const body = msgBody(m).replace(/\s+/g, ' ').slice(0, 600);
      return body ? `${who}: ${body}` : null;
    }).filter(Boolean).join('\n');
}

/**
 * The system prompt is static (cacheable): Ron's voice, the chat triage of the
 * Triage Call Script v1.0, the word swaps and the hard rules. Everything per lead
 * goes in the user message.
 */
function buildSystemPrompt({ bookingUrl, guideUrl, docUrl }) {
  return `Usted escribe por DM de Instagram como Ron Duarte, fundador de NeuroGrowth. Ron lo revisa antes de enviarlo.

CÓMO ESCRIBE RON EN DM (Ron, 2026-10-07: "very casual, close friend, straight to the point, just a human way")
- Como le escribe a un amigo por WhatsApp: corto, directo, sin rodeos, sin formalidades.
- Trata de usted, como se habla en Costa Rica entre amigos ("a quién le vende", "cuánto cobra"). Nunca tú ni vos.
- Las preguntas van SIN el signo de apertura: "a quién le vende?" y nunca "¿a quién le vende?". Nunca signos de exclamación.
- Nada de "Gracias por escribir", "Excelente", "Es un buen punto de partida", "Le agradezco", "Con gusto", "Estimado". Nada de halagos ni relleno. Va directo a la pregunta. "perfecto" solo al ofrecer la llamada, como lo dice Ron.
- Una o dos frases, máximo 25 palabras, salvo al ofrecer la sesión. Minúscula inicial está bien si suena natural.
- Ejemplos del tono:
  "a quién le vende, empresas o personas?"
  "y más o menos cuánto cobra por cliente?"
  "ok, y hoy cómo le llegan los clientes?"
  "va, eso pasa mucho con los referidos. cuántos cierra al mes más o menos?"

CONTEXTO
- NeuroGrowth instala en el LinkedIn del cliente un sistema que le trae de 10 a 30 reuniones al mes con quien decide, sin publicidad y sin que el cliente prospecte. El sistema queda suyo.
- Esta persona comentó "LinkedIn" en un reel y recibió la guía (${guideUrl}). Ahora respondió. La idea es entender su negocio con pocas preguntas y, si califica, ofrecerle una llamada con usted (Ron) o con su director comercial.
- Postura: evaluamos si podemos ayudar. No rogamos, no perseguimos, no convencemos.

QUÉ AVERIGUAR, UNA COSA A LA VEZ (en este orden, saltando lo que ya dijo)
1. qué vende y a quién (empresas o personas)
2. cuánto cobra por cliente, más o menos
3. cómo le llegan los clientes hoy y cuántos cierra al mes
4. qué es lo que más le cuesta para conseguir clientes, y por qué ahora
5. si decide solo o con alguien más

CUÁNDO OFRECER LA LLAMADA (dos pasos: primero el sí, después el calendario)
- Solo si ya sabe qué vende y a quién, cobra 1,500 dólares o más por cliente, dijo cuál es su problema real y sabe quién decide.
- Usted es Ron: hable en primera persona ("conmigo"), nunca "Ron" en tercera persona. Jose es "mi director comercial".
- PASO 1 (stage "qualified"), SIN enlace: proponga la llamada y pida su confirmación. Frases de Ron: "perfecto, de fijo podemos montar algo bien estructurado." y "lo mejor es que lo revisemos en una llamada conmigo o con mi director comercial", y cierre pidiendo el sí, por ejemplo "le parece? si me confirma le paso el calendario para que agende".
- PASO 2 (stage "booking"): solo cuando el prospecto ya dijo que sí a la llamada (en la conversación, después de que NOSOTROS la propuso), mande el calendario y pida que le confirme cuando agende, ejemplo "listo, aquí puede agendar: ${bookingUrl} me confirma por acá cuando agende y le paso un documento donde explico cómo funciona el sistema".
- PASO 3 (stage "booked"): solo cuando el prospecto dice que ya agendó, mande el documento, ejemplo "listo, aquí está el documento donde explico cómo funciona el sistema, revíselo antes de la llamada: ${docUrl}". El documento no se manda en ningún otro momento.
- Nunca mande el enlace antes de ese sí. Nunca ofrezca la llamada con datos en blanco.
- Si dice que no o que lo piensa: stage "nurture", una frase corta dejando la puerta abierta.

CUÁNDO NO SIGUE (stage "disqualified")
- Sin oferta, sin clientes, o cobra menos de 1,000 dólares: honesto y corto, ejemplo "le soy honesto, esto funciona para servicios de 1,500 dólares en adelante y hoy no es su caso. cuando llegue ahí me escribe y lo vemos."
- Si no es el momento (stage "nurture"): una frase corta dejando la puerta abierta, sin pregunta.

OBJECIONES: una frase y la pregunta que falte
- "mándeme más info": "para mandarle lo que le sirve, qué vende y a quién?"
- "cuánto cuesta": "depende de su caso, primero cuénteme qué vende y a quién"
- "qué es": "un sistema en su LinkedIn que le trae reuniones con quien decide, sin publicidad y sin que usted prospecte." y la pregunta que falte.
- "es un bot / spam": son mensajes escritos por IA en su voz y aprobados por usted, dentro de los límites de LinkedIn.
- "ya me quemó una agencia": no somos agencia, el sistema queda suyo.
- "no tengo tiempo": usted solo aprueba respuestas, como una hora al día.

REGLAS FIJAS
- Nunca diga precios, planes, garantías ni promesas de resultados o de clientes.
- Nunca use: leads, agencia (para nosotros), ticket, ICP, setter, closer, triage, máquina, ecosistema, bot (para nosotros), funnel. Diga: lo que cobra, su cliente ideal, reuniones con quien decide, un sistema en su LinkedIn.
- Sin emojis, sin guiones largos, sin ¿ ni ¡ ni !.
- Solo dos enlaces existen: el calendario, únicamente en el PASO 2, y el documento, únicamente en el PASO 3. Ningún otro.

CUÁNDO PASAR A UNA PERSONA ("handoff": true)
- Pide hablar por WhatsApp, pide una llamada, o da su número para que lo llamen.
- Se queja, menciona un tema legal o de pagos, dice ser cliente actual, o la conversación se vuelve compleja para un DM corto.
- Con handoff true redacte una frase corta: alguien del equipo le escribe ya, sin pedir nada más.

FORMATO DE SALIDA: solo un objeto JSON, sin texto alrededor:
{"intent": "${INTENTS.join('|')}", "stage": "${STAGES.join('|')}", "collected": {${FIELDS.map((f) => `"${f}": "texto corto o null"`).join(', ')}}, "draft": "el mensaje a enviar, o cadena vacía si no hay nada que responder", "handoff": false, "handoff_reason": "una frase o cadena vacía", "reason_es": "una frase de por qué"}
- "collected" resume lo que el prospecto ya dijo en toda la conversación; null si no lo ha dicho.
- intent not_interested con un "no gracias" claro: draft vacío y stage "done".`;
}

function buildUserPrompt({ contactName, thread, pending }) {
  return `Prospecto: ${contactName || 'sin nombre'}
Conversación (más antigua primero):
${thread}

Mensaje(s) a responder:
${pending}`;
}

/** Parse the drafter's JSON. Never throws; a broken reply becomes { ok: false }. */
function parseDraft(text) {
  const raw = String(text || '');
  const start = raw.indexOf('{');
  const end = raw.lastIndexOf('}');
  if (start < 0 || end <= start) return { ok: false, error: 'no JSON object' };
  let j;
  try { j = JSON.parse(raw.slice(start, end + 1)); } catch (e) { return { ok: false, error: `bad JSON: ${e.message}` }; }
  const intent = INTENTS.includes(j.intent) ? j.intent : 'other';
  const stage = STAGES.includes(j.stage) ? j.stage : 'discover';
  const collected = {};
  for (const f of FIELDS) {
    const v = j.collected && j.collected[f];
    collected[f] = v == null || String(v).trim() === '' || String(v).trim().toLowerCase() === 'null' ? null : String(v).trim().slice(0, 160);
  }
  return {
    ok: true, intent, stage, collected, draft: String(j.draft || '').trim(),
    handoff: j.handoff === true, handoff_reason: String(j.handoff_reason || '').trim().slice(0, 200),
    reason_es: String(j.reason_es || '').trim().slice(0, 200),
  };
}

const BANNED_WORDS = ['leads', 'lead', 'ticket', 'icp', 'setter', 'closer', 'triage', 'máquina', 'ecosistema', 'funnel', 'garantizo', 'garantizamos', 'garantizado', 'garantía'];
// Ron, 2026-10-07: casual, close-friend DMs. These openers read as a company, not a person.
const FORMAL_OPENERS = [/\bgracias por (escribir|comentar|su mensaje|responder)\b/i, /^\s*(excelente|genial)\b/i,
  /\ble agradezco\b/i, /\bcon gusto\b/i, /\bestimad[oa]\b/i, /\bes un buen punto de partida\b/i];
const TU_FORMS = /\b(tú|tienes|puedes|quieres|cuéntame|dime|te gustaría|tu negocio|vendes|cobras)\b/i;

/** Criteria on a draft before it reaches Ron (recipe §4a). */
function validateDraft(draft, { bookingUrl, docUrl, intent, stage }) {
  const problems = [];
  const text = String(draft || '');
  if (!text.trim()) {
    if (intent !== 'not_interested') problems.push('empty draft');
    return { ok: problems.length === 0, problems };
  }
  if (text.length > MAX_DRAFT_CHARS) problems.push(`too long (${text.length} chars)`);
  if (/[!¡]/.test(text)) problems.push('exclamation mark');
  if (/¿/.test(text)) problems.push('opening question mark (casual voice: "a quién le vende?")');
  for (const f of FORMAL_OPENERS) if (f.test(text)) problems.push(`formal filler "${text.match(f)[0]}"`);
  if (/[—–]/.test(text)) problems.push('long dash');
  if (/\$\s?\d|\d\s?(usd|dólares)\b/i.test(text) && !/1,?500 dólares en adelante/.test(text)) problems.push('mentions a price');
  if (TU_FORMS.test(text)) problems.push('uses tú');
  const lower = text.toLowerCase();
  for (const w of BANNED_WORDS) if (new RegExp(`(^|[^a-záéíóúñ])${w}([^a-záéíóúñ]|$)`, 'i').test(lower)) problems.push(`banned word "${w}"`);
  const urls = text.match(/https?:\/\/\S+/g) || [];
  // Ron, 2026-10-07: buy-in first (calendar only at stage booking), then the
  // system document only once the lead says they booked (stage booked).
  for (const raw of urls) {
    const u = raw.replace(/[).,]+$/, '');
    if (u === bookingUrl) { if (stage && stage !== 'booking') problems.push('calendar link before the lead confirmed the call'); }
    else if (docUrl && u === docUrl) { if (stage && stage !== 'booked') problems.push('system document before the lead booked'); }
    else problems.push(`unexpected link ${raw}`);
  }
  if (/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u.test(text)) problems.push('emoji');
  return { ok: problems.length === 0, problems };
}

/** Ron's Slack DM for one draft. */
function formatRonDm({ contactName, intent, stage, collected, draft, leadText, remainingMs, problems, reason }) {
  const lines = [`📸 *Instagram · ${contactName || 'Prospecto'} respondió a la guía* · ${INTENT_LABEL[intent] || intent} · ventana: ${fmtRemaining(remainingMs)}`];
  lines.push(...String(leadText || '').slice(0, 500).split('\n').map((l) => `> ${l}`));
  const known = FIELDS.filter((f) => collected && collected[f]).map((f) => `${FIELD_LABEL[f]}: ${collected[f]}`);
  if (known.length) lines.push(`_${known.join(' · ')}_`);
  if (stage === 'qualified') lines.push('_Califica: el borrador propone la llamada y pide su sí (sin calendario todavía)._');
  if (stage === 'booking') lines.push('_Confirmó la llamada: el borrador manda el calendario y pide que avise cuando agende._');
  if (stage === 'booked') lines.push('_Dice que ya agendó: el borrador manda el documento del sistema._');
  if (stage === 'disqualified') lines.push('_No califica: el borrador cierra con respeto._');
  if (draft) {
    lines.push('*Borrador:*');
    lines.push(...draft.split('\n').map((l) => `> ${l}`));
  } else {
    lines.push(`*Sin borrador:* ${reason || 'no hay nada que responder'}.`);
  }
  if (problems && problems.length) lines.push(`⚠️ Revisar: ${problems.join('; ')}`);
  lines.push(draft ? '✅ enviar · ❌ descartar · para editar, responda en este hilo y marque ✅ aquí o en su respuesta' : '❌ para archivar');
  return lines.join('\n');
}

/**
 * Who answers this conversation (plan-of-record §1 UPDATE 2026-10-07, later):
 * Max owns reel guide leads nobody has claimed; a human claim or a handback
 * means the setters own it and Max stays out.
 *   latestClaimName: claimed_by_setter_name of the newest setter_claims row, or null
 *   handedBack: Max already returned this conversation to #ng-sales-goats
 */
function ownershipDecision({ latestClaimName, handedBack }) {
  if (handedBack) return 'skip_handed_back';
  if (!latestClaimName) return 'claim';
  return latestClaimName === MAX_CLAIM_NAME ? 'proceed' : 'skip_human';
}

/** Pending drafts Max should hand back: still pending and older than the hold. */
function dueForHandback(rows, now) {
  return (rows || []).filter((r) => r && r.status === 'pending' && !r.handed_back_at
    && now - Date.parse(r.created_at) >= HOT_REPLY_HOLD_MS);
}

/** The thread note in #ng-sales-goats when Max returns a card. */
function formatHandbackNote({ reason, setterSlackIds }) {
  const mentions = (setterSlackIds || []).map((id) => `<@${id}>`).join(' ');
  const why = { timeout: 'Ron no aprobó la respuesta en 12 h y la ventana de 24 h de Instagram sigue corriendo',
    handoff: 'el prospecto necesita a una persona' }[reason.kind] || 'Max la devuelve';
  return `🔁 Max devuelve esta tarjeta: ${why}${reason.detail ? ` (${reason.detail})` : ''}. ${mentions} el primero que reaccione ✅ la toma y se lleva el crédito.`.trim();
}

/** The text to send: Ron's latest thread reply wins over the draft. */
function pickSendText({ draft, threadReplies, ronUserId }) {
  const ronReplies = (threadReplies || []).filter((m) => m && m.user === ronUserId && String(m.text || '').trim());
  if (ronReplies.length) {
    const last = ronReplies.sort((a, b) => Number(a.ts) - Number(b.ts))[ronReplies.length - 1];
    return { text: unSlack(last.text), edited: true };
  }
  return { text: String(draft || '').trim(), edited: false };
}

/** Slack mrkdwn back to plain text for Instagram: links, entities, quotes. */
function unSlack(text) {
  return String(text || '')
    .replace(/<(https?:\/\/[^|>]+)\|[^>]*>/g, '$1')
    .replace(/<(https?:\/\/[^>]+)>/g, '$1')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/^>\s?/gm, '')
    .trim();
}

module.exports = {
  WINDOW_MS, SETTLE_MS, HOT_REPLY_HOLD_MS, INTENTS, STAGES, FIELDS, MAX_CLAIM_NAME,
  ownershipDecision, dueForHandback, formatHandbackNote,
  isGuideConversation, latestPendingInbound, windowRemainingMs, fmtRemaining, threadForPrompt,
  buildSystemPrompt, buildUserPrompt, parseDraft, validateDraft, formatRonDm, pickSendText, unSlack,
};
