// Sequence email reply alerts.
//
// Prospects answer the post-call, reactivation and pre-call emails (sent from
// contact@neurogrowthconsulting.com by GHL workflows) by email. Those replies
// land in the GHL conversation and many never reach Ron's Gmail, so nobody on
// the sales team saw them. Approved by Ron 2026-10-23: one post per reply in
// #ng-sales-goats, mentioning the setter, the closer or both, with intent tags.
//
// GHL shape (read 2026-10-23): an email thread is ONE conversation message of
// type TYPE_EMAIL whose meta.email.messageIds lists every email in the thread,
// oldest first. The thread message takes the direction and date of its newest
// email, and its `body` is the FIRST email (ours), not the reply. The reply
// itself comes from GET /conversations/messages/email/{id}: HTML body with the
// quoted history, `from`, `subject` ("Re: ..."), `direction`, `dateAdded`.
//
// Pure: no Slack, no DB, no GHL. index.js owns the I/O and the cron.

const hotReplies = require('./hotReplies');

const MIN = 60 * 1000;
const H = 60 * MIN;

const CONFIG = {
  backfillMs: 3 * 24 * H,       // first run, and the hard floor for every run
  overlapMs: 60 * MIN,          // re-read this much before the watermark
  reminderAfterMs: 2 * H,       // one thread reminder if nobody reacted ✅
  reminderMaxAgeMs: 2 * 24 * H, // never remind on an alert older than this
  claimStaleMs: 15 * MIN,       // a claim with no post after this is retried
  quoteMax: 600,
  maxWalk: 6,                   // emails read back per thread
};

const INTERNAL_DOMAINS = ['neurogrowth.io', 'neurogrowthconsulting.com'];
const SKIP_TAGS = { personal: 'personal', 'won-deal': 'client' };
const SALES_TAG_RE = /^(react:|reactivacion-|postcall-|nurture:|follow-up$|call-booked$|call-showed$)/;
const OPEN_DEAL_STAGE_IDS = new Set(['63d30181-4ec0-4daa-8832-a8eebe1afbeb', '1134d60f-d2c3-43d1-81bd-6c113b46b693']);
const CALL_BOOKED_STAGE_IDS = new Set([hotReplies.CALL_BOOKED_STAGE_ID]);
const REACT_LABEL = {
  abierto: 'reactivación · deal abierto',
  perdido: 'reactivación · perdido',
  no_show: 'reactivación · no-show',
  sin_resultado: 'reactivación · sin resultado',
  descalificado: 'reactivación · descalificado',
};

const lowerTags = (tags) => (Array.isArray(tags) ? tags : []).map(t => String(t || '').trim().toLowerCase()).filter(Boolean);

// Lowercase, accents removed, whitespace collapsed. Punctuation kept.
function fold(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
}
// fold() with punctuation turned into spaces, for whole-word matching.
function words(s) {
  return ` ${fold(s).replace(/[^a-z0-9ñ]+/g, ' ').trim()} `;
}

// ── HTML to text ────────────────────────────────────────────────────────────
const NAMED_ENTITIES = {
  nbsp: ' ', amp: '&', lt: '<', gt: '>', quot: '"', apos: "'",
  aacute: 'á', eacute: 'é', iacute: 'í', oacute: 'ó', uacute: 'ú',
  Aacute: 'Á', Eacute: 'É', Iacute: 'Í', Oacute: 'Ó', Uacute: 'Ú',
  agrave: 'à', egrave: 'è', ograve: 'ò', ccedil: 'ç', atilde: 'ã', otilde: 'õ',
  ntilde: 'ñ', Ntilde: 'Ñ', uuml: 'ü', Uuml: 'Ü', iquest: '¿', iexcl: '¡',
  laquo: '«', raquo: '»', ordm: 'º', ordf: 'ª', deg: '°', euro: '€',
  ndash: String.fromCharCode(0x2013), mdash: String.fromCharCode(0x2014), hellip: '…', bull: '•', middot: '·',
  lsquo: String.fromCharCode(0x2018), rsquo: String.fromCharCode(0x2019), ldquo: String.fromCharCode(0x201c), rdquo: String.fromCharCode(0x201d), copy: '©', reg: '®',
};
function decodeEntities(s) {
  return String(s || '')
    .replace(/&#(\d+);/g, (_, n) => { try { return String.fromCodePoint(Number(n)); } catch (_) { return ''; } })
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => { try { return String.fromCodePoint(parseInt(n, 16)); } catch (_) { return ''; } })
    .replace(/&([a-z]+);/gi, (m, name) => (Object.prototype.hasOwnProperty.call(NAMED_ENTITIES, name) ? NAMED_ENTITIES[name] : m));
}

// Where the quoted history starts in an HTML reply. Gmail wraps it in
// div.gmail_quote, Outlook puts an <hr> and div#divRplyFwdMsg before it, Apple
// Mail, Roundcube and Thunderbird use <blockquote>. The earliest marker wins.
const HTML_QUOTE_MARKERS = [
  /<div[^>]*class=["'][^"']*gmail_quote/i,
  /<blockquote[\s>]/i,
  /<div[^>]*id=["']?(x_)?divRplyFwdMsg/i,
  /<div[^>]*id=["']?appendonsend/i,
  /<div[^>]*id=["']?mail-editor-reference-message-container/i,
  /<div[^>]*class=["'][^"']*(moz-cite-prefix|yahoo_quoted|protonmail_quote|OutlookMessageHeader)/i,
  /<hr[\s>/]/i,
];

function htmlToText(html) {
  // Raw newlines in HTML source are whitespace, not line breaks; only tags break lines.
  let s = String(html || '')
    .replace(/\r?\n/g, ' ')
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<(head|style|script|title)[^>]*>[\s\S]*?<\/\1>/gi, '');
  // Block boundaries (opening or closing) become ONE line break however many
  // touch; <br> is an explicit break, so an empty <div><br></div> still reads
  // as a blank line.
  s = s
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<li[^>]*>/gi, '\u0001- ')
    .replace(/<\/?(p|div|li|tr|table|tbody|h[1-6]|blockquote|ul|ol|pre)\b[^>]*>/gi, '\u0001')
    .replace(/<[^>]+>/g, '')
    .replace(/\u0001(?:[ \t]*\u0001)*/g, '\n');
  return normalizeText(decodeEntities(s));
}

function normalizeText(s) {
  return String(s || '')
    .replace(/\r\n?/g, '\n')
    .replace(/[\u00a0\u202f\u2007\u2009\t]/g, ' ')
    .replace(/[\u200b\u200c\u200d\ufeff]/g, '')
    .split('\n').map(l => l.replace(/ {2,}/g, ' ').trim()).join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// ── Quote stripping (text level) ────────────────────────────────────────────
// Zoho wraps it in dashes: "---- On Wed, 21 Oct 2026 08:05:36 -0600 Ron <...> wrote ----".
const ATTRIBUTION_RE = /^[-\s]*(el|on|le|em|am)\b.{0,300}?\b(escribi[oó]|wrote|a [eé]crit|escreveu|schrieb)\s*:?[-\s]*$/i;
const ORIGINAL_RE = /^-{2,}\s*(original message|mensaje original|forwarded message|mensaje reenviado|message d'origine)/i;
const HEADER_START_RE = /^\*?(de|from)\s*:/i;
const HEADER_NEXT_RE = /^\*?(enviado|sent|para|to|asunto|subject|fecha|date|cc)\s*:/i;
const MOBILE_SIGNATURE_RE = /^(enviado desde|sent from|obtener outlook|get outlook|descargar outlook|enviado con|sent with)\b/i;

function stripQuoted(text) {
  const lines = normalizeText(text).split('\n');
  let cut = lines.length;
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i];
    if (/^>/.test(l) || ORIGINAL_RE.test(l) || /^_{8,}$/.test(l)) { cut = i; break; }
    // Attribution lines wrap on phones: "El mié, 21 de oct de 2026, 8:06 a.m.,
    // Ron Duarte <\ncontact+...> escribió:" spans up to three lines. They always
    // carry a date, and never a blank line, which keeps "El proyecto ya no se
    // hizo" (a real answer) from being joined to the attribution below it.
    if (/^[-\s]*(el|on|le|em|am)\b/i.test(l) && /\d/.test(l)) {
      let hit = false;
      for (let n = 1; n <= 3 && !hit; n++) {
        const chunk = lines.slice(i, i + n);
        if (chunk.length < n || chunk.some(x => !x)) break;
        hit = ATTRIBUTION_RE.test(chunk.join(' '));
      }
      if (hit) { cut = i; break; }
    }
    if (HEADER_START_RE.test(l) && lines.slice(i + 1, i + 5).some(x => HEADER_NEXT_RE.test(x))) { cut = i; break; }
  }
  return trimSignature(normalizeText(lines.slice(0, cut).filter(l => !MOBILE_SIGNATURE_RE.test(l)).join('\n')));
}

// Contact data at the END of a reply is a signature: email, web address,
// phone, or an address line with a postal code. Removed from the bottom up,
// stopping at the first line that is not one, so a phone number the prospect
// wrote inside the answer stays.
const SIGNATURE_DATA_RE = [
  /^[^\s@]+@[^\s@]+\.[a-z]{2,}$/i,
  /^(https?:\/\/|www\.)\S+$/i,
  /^((tel|tel[eé]fono|phone|cel|celular|m[oó]vil|mobile|whatsapp|oficina|office|fax|direct[oa]?)\b[^0-9+]{0,6})?\+?[\d\s().-]{7,}$/i,
  /\b\d{5}\b.*,|,.*\b\d{5}\b/,
];
function trimSignature(text) {
  const lines = String(text || '').split('\n');
  while (lines.length > 1) {
    const l = lines[lines.length - 1].trim();
    if (l === '' || (l.length <= 90 && SIGNATURE_DATA_RE.some(re => re.test(l)))) lines.pop();
    else break;
  }
  return lines.join('\n').trim();
}

function looksLikeHtml(s) {
  return /<(html|body|div|p|br|span|table|blockquote)[\s>/]/i.test(String(s || ''));
}

// The prospect's own words: quoted history, mobile signatures and blank runs
// removed. If cutting at the HTML quote marker leaves nothing (a client that
// quotes first and answers below), the whole body is used instead.
function cleanReply(body) {
  const raw = String(body || '');
  if (!looksLikeHtml(raw)) return stripQuoted(raw);
  let cutAt = raw.length;
  for (const re of HTML_QUOTE_MARKERS) {
    const m = re.exec(raw);
    if (m && m.index < cutAt) cutAt = m.index;
  }
  const head = stripQuoted(htmlToText(raw.slice(0, cutAt)));
  if (head) return head;
  return stripQuoted(htmlToText(raw));
}

function clip(text, max = CONFIG.quoteMax) {
  const s = String(text || '').trim();
  if (s.length <= max) return s;
  const cut = s.slice(0, max - 1);
  const sp = cut.lastIndexOf(' ');
  return `${(sp > max * 0.6 ? cut.slice(0, sp) : cut).trimEnd()}…`;
}

// ── Senders and skips ───────────────────────────────────────────────────────
function emailAddress(from) {
  const s = String(from || '').trim();
  const m = /<([^>]+)>/.exec(s);
  return (m ? m[1] : s).trim().toLowerCase();
}
function isInternalAddress(addr) {
  const a = emailAddress(addr);
  const domain = a.split('@')[1] || '';
  return INTERNAL_DOMAINS.some(d => domain === d || domain.endsWith(`.${d}`));
}

const BOUNCE_FROM_RE = /(mailer-daemon|postmaster|mail delivery (subsystem|system)|microsoftexchange)/i;
const BOUNCE_SUBJECT_RE = /^(undeliverable|undelivered mail|delivery status notification|mail delivery failed|delivery has failed|failure notice|returned mail|no se puede entregar|no se pudo entregar|notificacion de estado de entrega|entrega fallida)/;
const AUTO_SUBJECT_RE = /(respuesta automatica|automatic reply|auto[- ]?reply|autoreply|out of (the )?office|fuera de (la )?oficina|reponse automatique|resposta automatica|risposta automatica|mensaje automatico)/;
const AUTO_SUBJECT_START_RE = /^(ausente|ausencia|vacaciones|vacation|on vacation|de vacaciones)\b/;
const AUTO_BODY_RE = /(respuesta automatica|mensaje automatico|correo automatico|fuera de (la )?oficina|out of (the )?office|automatic reply|auto[- ]?reply|this is an automated|este es un mensaje automatico|no responda a este (correo|mensaje)|do not reply to this)/;
const VACATION_RE = /(vacacion|vacation|de viaje|fuera del pais|incapacidad)/;
const RETURN_RE = /(regres|retorn|reincorpor|de vuelta|volvere|estare fuera|back on|return(ing)? on|back in the office|hasta el (\d|lunes|martes|miercoles|jueves|viernes|sabado|domingo))/;
// An out-of-office always tells you what to do meanwhile; a person writing
// "ando de viaje, a mi regreso hablamos" does not, and must still alert.
const AUTO_CUE_RE = /(acceso limitado|sin acceso|limited access|urgen|en mi ausencia|in my absence|contacte a|contactar a|please contact|comunicarse con|respondere|atendere|will respond|will reply|lo antes posible|as soon as possible)/;
const CALENDAR_SUBJECT_RE = /^(accepted|aceptad[oa]|declined|rechazad[oa]|tentative|provisional|invitation|invitacion|updated invitation|invitacion actualizada|canceled event|cancelled event|evento cancelado)\b[^:]{0,30}:/;
const CALENDAR_BODY_RE = /(has accepted this invitation|ha aceptado esta invitacion|has declined this invitation|ha rechazado esta invitacion|accepted this invitation|acepto esta invitacion)/;

// Why this reply must not reach the team, or null. text = cleanReply(body).
function skipReason({ from, subject, text }) {
  const subj = fold(subject);
  const body = fold(text).slice(0, 1200);
  if (BOUNCE_FROM_RE.test(String(from || '')) || BOUNCE_SUBJECT_RE.test(subj)) return 'bounce';
  if (isInternalAddress(from)) return 'internal';
  if (CALENDAR_SUBJECT_RE.test(subj) || CALENDAR_BODY_RE.test(body)) return 'calendar';
  if (AUTO_SUBJECT_RE.test(subj) || AUTO_SUBJECT_START_RE.test(subj) || AUTO_BODY_RE.test(body)) return 'auto_reply';
  if (VACATION_RE.test(body) && RETURN_RE.test(body) && AUTO_CUE_RE.test(body)) return 'auto_reply';
  if (!/[a-z0-9ñ]/.test(body)) return 'empty';
  return null;
}

// ── Conversation screen (no message read yet) ───────────────────────────────
// Only contacts in a sales flow alert: a react:/postcall/nurture/call tag, or
// any opportunity in a sales pipeline. Mailbox-synced fulfillment threads and
// webinar registrants have neither.
function hasSalesContext(tags, opportunities) {
  if (lowerTags(tags).some(t => SALES_TAG_RE.test(t))) return true;
  return (Array.isArray(opportunities) ? opportunities : []).some(o => hotReplies.SALES_PIPELINE_IDS.has(o && o.pipelineId));
}

function tagSkip(tags, personalTag = 'personal') {
  const t = lowerTags(tags);
  if (t.includes(String(personalTag).toLowerCase())) return 'personal';
  for (const [tag, reason] of Object.entries(SKIP_TAGS)) if (t.includes(tag)) return reason;
  return null;
}

function screenConversation(convo, { clientEmails = null, personalTag = 'personal' } = {}) {
  if (!convo) return { keep: false, reason: 'empty' };
  if (convo.lastMessageDirection !== 'inbound') return { keep: false, reason: 'team_spoke_last' };
  if (convo.lastMessageType !== 'TYPE_EMAIL') return { keep: false, reason: 'other_channel' };
  const t = tagSkip(convo.tags, personalTag);
  if (t) return { keep: false, reason: t };
  const email = String(convo.email || '').trim().toLowerCase();
  if (email && clientEmails && clientEmails.has(email)) return { keep: false, reason: 'client' };
  if (email && isInternalAddress(email)) return { keep: false, reason: 'internal' };
  if (!hasSalesContext(convo.tags, convo.opportunities)) return { keep: false, reason: 'no_sales_context' };
  return { keep: true, reason: null };
}

// ── Thread walk ─────────────────────────────────────────────────────────────
// Newest email first, collect inbound replies until an outbound email (the
// team or a workflow wrote after them), an email older than `since`, or one
// already handled. Returns them oldest first. fetchMeta(id) -> { direction, at }
// may throw; the caller counts the conversation as failed and retries it.
async function collectNewReplies({ messageIds, fetchMeta, since, isKnown = () => false, maxWalk = CONFIG.maxWalk }) {
  const out = [];
  const ids = (Array.isArray(messageIds) ? messageIds : []).slice().reverse();
  for (let i = 0; i < ids.length && i < maxWalk; i++) {
    const id = ids[i];
    if (!id || isKnown(id)) break;
    const meta = await fetchMeta(id);
    if (!meta || String(meta.direction || '').toLowerCase() !== 'inbound') break;
    if (!(Number(meta.at) >= since)) break;
    out.push({ id, at: Number(meta.at) });
  }
  return out.reverse();
}

// Stored value of a `seq-reply:<emailId>` key → what a run may do with it.
// free = never seen; done = posted or skipped; running = another run claimed it
// moments ago; stale = a claim whose post never landed (process died), retry.
function claimState(value, now) {
  if (value === undefined || value === null || value === '') return 'free';
  let v = null;
  try { v = JSON.parse(value); } catch (_) { return 'done'; }
  if (!v || v.s !== 'claimed') return 'done';
  return now - Number(v.at || 0) < CONFIG.claimStaleMs ? 'running' : 'stale';
}

// ── Intent tags ─────────────────────────────────────────────────────────────
const COUNTRY_RE = /\b(mexico|costa rica|colombia|peru|chile|argentina|ecuador|guatemala|panama|honduras|el salvador|nicaragua|republica dominicana|puerto rico|venezuela|uruguay|paraguay|bolivia|espana|estados unidos|eeuu|ee uu|canada|latam|latinoamerica|centroamerica)\b/;
const TITLE_RE = /\b(ceo|cfo|coo|cto|cmo|directora?s?|directores|gerentes?|duen[oa]s?|fundador(a|es)?|founders?|presidentes?|vp|vicepresidentes?|jefes?|jefas?|head of|managers?|coordinador(a|es)?|propietari[oa]s?|owners?|rrhh|recursos humanos)\b/;
const TIME_RE = /\b\d{1,2}(:\d{2})?\s*(am|pm|a\.\s?m\.?|p\.\s?m\.?|hrs?|hs)\b/;

// [label, test(w, low, lines)] in display order. w = words(), low = fold().
const INTENTS = [
  ['listo para contrato', (w) => /\s(arranco|arrancamos|arranquemos|contrato|firmemos|firmamos)\s/.test(w)],
  ['eligió PATEK', (w) => /\spatek\s/.test(w)],
  ['eligió ROLEX', (w) => /\srolex\s/.test(w)],
  ['quiere agendar', (w, low) => /\s(horario|horarios|agendar|agendemos|agendamos|agende|agendo|llamada|hablemos)\s/.test(w)
    || (/\s(lunes|martes|miercoles|jueves|viernes)\s/.test(w) && TIME_RE.test(low))],
  ['socio', (w) => /\s(socio|socios|socia|socias)\s/.test(w)],
  ['pidió la búsqueda', (w, low, lines) => /\s(cargo|cargos|pais|busque|busqueda)\s/.test(w)
    || lines.some(l => l.length <= 160 && TITLE_RE.test(l) && COUNTRY_RE.test(l))],
  ['pregunta el precio', (w) => /\s(costo|costos|precio|precios|inversion|tarifa|tarifas|cotizacion)\s/.test(w) || /\scuanto (cuesta|vale|es|sale|cobran)\s/.test(w)],
  ['sigue igual', (w) => /\s(sigue|sigo|seguimos|todo) igual\s/.test(w)],
  ['pausa', (w) => /\spausa\s/.test(w)],
  ['no interesado', (w) => /\sno (me|nos) interesa\s/.test(w) || /\sno (estoy|estamos) interesad[oa]s?\s/.test(w)
    || /\sno gracias\s/.test(w) || /\s(borrar|borrarme|borren|eliminarme|unsubscribe|desuscribir|desuscribirme)\s/.test(w)
    || /\sde baja\s/.test(w) || /\sno me (escriban|envien|contacten|manden)\s/.test(w)
    || /\s(seria|es) un no\s/.test(w) || /\sno es para mi\s/.test(w) || /\sya (contrate|contratamos)\s/.test(w)
    || /\sno estoy buscando\s/.test(w)],
];

function tagIntents(text) {
  const w = words(text);
  const low = fold(text);
  const lines = String(text || '').split('\n').map(fold).filter(Boolean);
  const out = INTENTS.filter(([, test]) => test(w, low, lines)).map(([label]) => label);
  return out.length ? out : ['respuesta'];
}

// ── Labels, mentions, formatting ────────────────────────────────────────────
function answeredSubject(subject) {
  return String(subject || '').replace(/^\s*((re|rv|fw|fwd|aw|sv|antw|r)\s*(\[\d+\])?\s*:\s*)+/i, '').replace(/\s+/g, ' ').trim();
}

// Which flow the contact is in, in Spanish, or null when nothing says.
function groupLabel({ tags, opportunities }) {
  const t = lowerTags(tags);
  const react = t.find(x => x.startsWith('react:'));
  if (react) return REACT_LABEL[react.slice(6)] || 'reactivación';
  if (t.includes('reactivacion-oct26')) return 'reactivación';
  const open = (Array.isArray(opportunities) ? opportunities : []).filter(o => o && String(o.status || '').toLowerCase() === 'open');
  if (open.some(o => OPEN_DEAL_STAGE_IDS.has(o.pipelineStageId))) return 'post-llamada';
  if (open.some(o => CALL_BOOKED_STAGE_IDS.has(o.pipelineStageId))) return 'pre-llamada';
  if (t.some(x => /^(postcall-|nurture:)/.test(x) || x === 'follow-up' || x === 'call-showed')) return 'post-llamada';
  if (t.includes('call-booked')) return 'pre-llamada';
  return null;
}

function customFieldValue(contact, fieldId) {
  const f = ((contact && contact.customFields) || []).find(x => x && x.id === fieldId);
  const v = f ? (f.value ?? f.fieldValue ?? '') : '';
  return Array.isArray(v) ? String(v[0] || '').trim() : String(v || '').trim();
}

function nameKey(name) {
  return fold(name).replace(/[^a-z ]/g, '').replace(/\s+/g, ' ').trim();
}

// Setter = the contact's GHL owner, closer = the closer field. Both when both
// map and differ, whichever maps otherwise, Ron when neither does.
function resolveMentions({ assignedTo, closerEmail, closerName, ownerSlackByGhlId = {}, closerSlackByEmail = {}, closerSlackByName = {}, fallbackSlackId = null }) {
  const setter = assignedTo ? (ownerSlackByGhlId[String(assignedTo).trim().toLowerCase()] || null) : null;
  let closer = closerEmail ? (closerSlackByEmail[String(closerEmail).trim().toLowerCase()] || null) : null;
  if (!closer && closerName) closer = closerSlackByName[nameKey(closerName)] || null;
  const mentions = [];
  if (setter) mentions.push(setter);
  if (closer && closer !== setter) mentions.push(closer);
  const fallback = mentions.length === 0;
  if (fallback && fallbackSlackId) mentions.push(fallbackSlackId);
  return { setter, closer, mentions, fallback };
}

const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const ALERT_HEAD = '📧 *Respuesta por correo*';
const ALERT_FOOT = 'Reaccione ✅ cuando esté atendido.';

// item = { contactName, intents, subject, group, quote, link, mentions, fallback,
// replyAt?, now? }. A reply older than 30 min (the 3-day backfill, a retry)
// says how old it is.
function formatAlert(item) {
  const who = (item.mentions || []).map(id => `<@${id}>`).join(' ');
  const age = item.replyAt && item.now && item.now - item.replyAt >= 30 * MIN ? ` (hace ${ago(item.now - item.replyAt)})` : '';
  const head = `${ALERT_HEAD} · *${esc(item.contactName || 'Sin nombre')}*${age}`;
  const quote = clip(item.quote).split('\n').map(l => l.trim()).filter(Boolean).map(l => `> ${esc(l)}`);
  const intents = item.intents && item.intents.length ? item.intents : ['respuesta'];
  return [
    who ? `${who} ${head}` : head,
    `*Intención:* ${intents.join(' · ')}`,
    `*Respondió a:* ${esc(item.subject) || '(sin asunto)'}${item.group ? ` · *Grupo:* ${item.group}` : ''}`,
    ...quote,
    intents.includes('pausa') ? '⏸️ Pausa: ya pasa al correo mensual.' : null,
    item.fallback ? '_Sin setter ni closer asignado en GHL._' : null,
    `<${item.link}|Abrir en GHL> · ${ALERT_FOOT}`,
  ].filter(Boolean).join('\n');
}

// Contract checked before posting: null when it holds, else the problem. The
// placeholder check skips the quote, which is the prospect's own text.
function validateAlert(text) {
  const t = String(text || '');
  const lines = t.split('\n');
  if (!lines[0] || !lines[0].includes(ALERT_HEAD)) return 'missing header';
  if (!lines.some(l => l.startsWith('*Intención:* '))) return 'missing intent';
  if (!lines.some(l => l.startsWith('> '))) return 'missing quote';
  if (!/<https:\/\/[^|>]+\|Abrir en GHL>/.test(t)) return 'missing GHL link';
  const own = lines.filter(l => !l.startsWith('> ')).join('\n');
  if (/\b(undefined|null|NaN)\b|\[object Object\]/.test(own)) return 'placeholder value';
  return null;
}

function ago(ms) {
  const m = Math.max(0, Math.round(ms / MIN));
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  return h < 24 ? `${h}h` : `${Math.floor(h / 24)}d ${h % 24}h`;
}

function formatReminder({ mentions, contactName, postedAt, now }) {
  const who = (mentions || []).map(id => `<@${id}>`).join(' ');
  const body = `⏰ La respuesta de *${esc(contactName || 'Sin nombre')}* sigue sin atender (hace ${ago(now - postedAt)}). Reaccione ✅ en el mensaje de arriba cuando esté atendida.`;
  return who ? `${who} ${body}` : body;
}

// One reminder, inside the poke window (Mon to Sat, 8 AM to 8 PM CR), 2h of
// window time after the post. Nothing after that.
function reminderDue({ now, postedAt, handledAt = null, remindedAt = null }) {
  if (handledAt || remindedAt || !postedAt) return false;
  if (now - postedAt > CONFIG.reminderMaxAgeMs) return false;
  if (!hotReplies.inPokeWindow(now)) return false;
  return now >= hotReplies.clockStart(postedAt) + CONFIG.reminderAfterMs;
}

// The run window: last 3 days on the first run, else the watermark minus an
// overlap, never older than 3 days.
function sinceFor({ now, watermark }) {
  const floor = now - CONFIG.backfillMs;
  const w = Number(watermark);
  if (!Number.isFinite(w) || w <= 0) return { since: floor, firstRun: true };
  return { since: Math.max(floor, w - CONFIG.overlapMs), firstRun: false };
}

// Was the reply answered in GHL after it arrived? A typed answer on any channel
// (same rule as hot replies), or the email thread itself now ends with an
// outbound email newer than the reply.
function answeredInGhl({ messages, replyAt, threadId = null, isAutomatedBody = () => false }) {
  if (hotReplies.firstAnswerAfter(messages, replyAt, isAutomatedBody)) return true;
  const thread = threadId ? (messages || []).find(m => m && m.id === threadId) : null;
  return !!(thread && thread.direction === 'outbound' && hotReplies.msgTime(thread) > replyAt);
}

module.exports = {
  CONFIG, REACT_LABEL,
  fold, htmlToText, decodeEntities, stripQuoted, cleanReply, clip,
  emailAddress, isInternalAddress, skipReason,
  hasSalesContext, tagSkip, screenConversation,
  collectNewReplies, claimState,
  tagIntents, answeredSubject, groupLabel, customFieldValue, nameKey, resolveMentions,
  formatAlert, validateAlert, formatReminder, reminderDue, sinceFor, answeredInGhl,
  ALERT_HEAD,
};
