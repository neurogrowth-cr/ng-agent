// Hot reply alerts + social intake gap check.
// Plan: ~/.claude/plans/reply-1-hashed-galaxy.md (approved by Ron 2026-10-04).
//
// On 2026-10-04 leads asking "cuál es el costo", "sí claro, podemos conversar"
// and "voy llegando, si quiere llamarme" sat unanswered in GHL next to business
// auto-replies and "gracias". Every older sweep ranks chats by how long they
// waited; this one reads what the lead said. A hot message with no team reply
// pokes the assigned setter at 15 min and again at 2h, and whatever is still
// unanswered lands in Ron's 6 PM sweep DM.
//
// The intake gap rule rides the same run (folded in from the session "IG card
// creation for incoming messages"): on 2026-10-02/03 organic Instagram DMs got
// no card and no Slack post for about 21h and nothing alerted. An Instagram or
// Messenger contact who wrote to us 30+ min ago and has no card, or has a card
// but no Slack lead post, is reported to #ng-pm-agent.
//
// Pure: no Slack, no DB, no GHL. index.js owns the I/O and the cron.

const MIN = 60 * 1000;
const H = 60 * MIN;
// Costa Rica is UTC-6 with no DST (same assumption as lib/leadVolume.js).
const CR_OFFSET_MS = 6 * H;

const CONFIG = {
  lookbackMs: 48 * H,        // a Saturday 3 PM message is still in scope Monday 8 AM
  intakeLookbackMs: 24 * H,
  intakeMinAgeMs: 30 * MIN,  // the intake webhook and Slack post land within seconds
  poke1AfterMs: 15 * MIN,
  poke2AfterMs: 2 * H,
  poke2MinGapMs: 45 * MIN,   // a message classified hot late never gets both pokes at once
  windowFromHour: 8,         // pokes go out 08:00 to 20:00 CR, Monday to Saturday
  windowToHour: 20,
  tailMessages: 6,
  failuresBeforeBrokenPost: 3,
};

// Channels a lead writes to us on. Email and SMS leads are out of scope.
const LEAD_CHANNELS = new Set(['TYPE_WHATSAPP', 'TYPE_INSTAGRAM', 'TYPE_FACEBOOK']);
const INTAKE_CHANNELS = new Set(['TYPE_INSTAGRAM', 'TYPE_FACEBOOK']);
// A team answer on any real channel counts, including a GHL-logged call or an
// email. TYPE_ACTIVITY_* (stage changes, appointments) and internal comments are
// outbound with source 'app' too, which is why this is an allowlist.
const ANSWER_TYPES = new Set(['TYPE_WHATSAPP', 'TYPE_INSTAGRAM', 'TYPE_FACEBOOK', 'TYPE_SMS', 'TYPE_EMAIL', 'TYPE_CALL']);
// Sends nobody typed. A phone-app send arrives as source 'app' with an empty
// userId and IS an answer; a workflow send carries the owner's userId and is not.
const AUTOMATED_SOURCES = new Set(['workflow', 'bot', 'campaign', 'bulk_actions', 'conversation_ai']);

const CHANNEL_LABEL = { TYPE_WHATSAPP: 'WhatsApp', TYPE_INSTAGRAM: 'Instagram', TYPE_FACEBOOK: 'Messenger' };
const CALL_BOOKED_STAGE_ID = 'dc1fba03-abeb-4b47-9d29-6c308002b6c1';
const SKIP_TAGS = new Set(['personal', 'no-fit', 'won-deal']);

const msgTime = (m) => Date.parse(m && (m.dateAdded || m.createdAt)) || 0;
const msgBody = (m) => String((m && (m.body || m.message)) || '').trim();
const lowerTags = (tags) => (Array.isArray(tags) ? tags : []).map(t => String(t || '').trim().toLowerCase());

// ── Conversation-level filter (no message read yet) ─────────────────────────
// Returns { keep: bool, reason } so index.js can count skips.
function screenConversation(convo, { now, personalTag = 'personal', clientEmails = null } = {}) {
  if (!convo) return { keep: false, reason: 'empty' };
  if (convo.lastMessageDirection !== 'inbound') return { keep: false, reason: 'team_spoke_last' };
  if (!LEAD_CHANNELS.has(convo.lastMessageType)) return { keep: false, reason: 'other_channel' };
  if (!(Number(convo.lastMessageDate) >= now - CONFIG.lookbackMs)) return { keep: false, reason: 'too_old' };
  const tags = lowerTags(convo.tags);
  if (tags.includes(String(personalTag).toLowerCase())) return { keep: false, reason: 'personal' };
  for (const t of tags) if (SKIP_TAGS.has(t)) return { keep: false, reason: `tag_${t}` };
  const opps = Array.isArray(convo.opportunities) ? convo.opportunities : [];
  if (opps.length && opps.every(o => String(o.status || '').toLowerCase() === 'lost')) return { keep: false, reason: 'lost' };
  const email = String(convo.email || '').trim().toLowerCase();
  if (email && clientEmails && clientEmails.has(email)) return { keep: false, reason: 'client' };
  return { keep: true, reason: null };
}

function isCallBooked(convo) {
  return (convo.opportunities || []).some(o => o.pipelineStageId === CALL_BOOKED_STAGE_ID && String(o.status || '').toLowerCase() === 'open');
}

// Did a person on the team answer with this message?
function isTeamAnswer(m, isAutomatedBody = () => false) {
  if (!m || m.direction !== 'outbound') return false;
  if (!ANSWER_TYPES.has(m.messageType)) return false;
  if (AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase())) return false;
  if (isAutomatedBody(msgBody(m))) return false;
  return true;
}

// Reads a thread (any order) and returns the unanswered run of lead messages:
// every inbound lead-channel message after the last team answer. The ANCHOR is
// the first of them; its id keys the alert ladder, so three messages in a row
// are one ladder, and a later lead message never restarts the clock until the
// team answers. Returns null when nothing is waiting.
function analyzeThread(messages, { isAutomatedBody } = {}) {
  const sorted = (messages || []).slice().sort((a, b) => msgTime(a) - msgTime(b));
  let lastAnswerIdx = -1;
  sorted.forEach((m, i) => { if (isTeamAnswer(m, isAutomatedBody)) lastAnswerIdx = i; });
  const waiting = sorted.slice(lastAnswerIdx + 1)
    .filter(m => m.direction === 'inbound' && LEAD_CHANNELS.has(m.messageType));
  if (!waiting.length) return null;
  const anchor = waiting[0];
  const newest = waiting[waiting.length - 1];
  return {
    anchorId: anchor.id,
    anchorAt: msgTime(anchor),
    newestId: newest.id,
    newestAt: msgTime(newest),
    channel: anchor.messageType,
    lastAnswerAt: lastAnswerIdx >= 0 ? msgTime(sorted[lastAnswerIdx]) : null,
    waitingCount: waiting.length,
    quote: waiting.map(msgBody).filter(Boolean).join(' / '),
    tail: sorted.slice(-CONFIG.tailMessages),
  };
}

// ── Classifier ──────────────────────────────────────────────────────────────
function buildClassifierPrompt({ contactName, tail, callBooked }) {
  const lines = (tail || []).map(m => {
    const who = m.direction === 'inbound' ? 'LEAD' : 'EQUIPO';
    const body = msgBody(m) || (m.attachments && m.attachments.length ? '[adjunto]' : '[sin texto]');
    return `${who}: ${body.slice(0, 500)}`;
  });
  return [
    'You triage WhatsApp, Instagram and Messenger chats for a sales team that sells a LinkedIn client-acquisition service in Costa Rica and Mexico.',
    'Classify the LEAD\'s latest unanswered messages. Output ONLY one JSON object, no prose:',
    '{"verdict":"hot|normal|noise","reason_es":"<one short Spanish sentence>","summary_es":"<what the lead wants, max 12 words, Spanish>"}',
    '',
    'hot = a buying signal a setter must answer now: asks the price or how it works, wants a call or proposes a time, says yes to a call or next step, sends their business info, website or email after being asked, says they tried to reach us, or asks a direct question about the service.',
    'noise = nothing to answer: the lead\'s own business auto-reply ("Gracias por comunicarte con..."), thanks or emoji only, an opt-out, or "ya contratamos a otro / no me interesa".',
    'normal = everything else (small talk, a vague reply, unclear).',
    callBooked ? 'This lead already has a call booked: any real message from them (not an auto-reply) is hot.' : '',
    '',
    `Contact: ${contactName || 'unknown'}`,
    'Conversation (oldest first):',
    ...lines,
  ].filter(l => l !== '').join('\n');
}

// Never throws. Anything unreadable is 'normal', so a bad model answer can
// never page a setter.
function parseVerdict(text) {
  const fallback = { verdict: 'normal', reason_es: '', summary_es: '', parsed: false };
  const m = String(text || '').match(/\{[\s\S]*\}/);
  if (!m) return fallback;
  try {
    const j = JSON.parse(m[0]);
    const v = String(j.verdict || '').toLowerCase().trim();
    if (!['hot', 'normal', 'noise'].includes(v)) return fallback;
    return { verdict: v, reason_es: String(j.reason_es || '').slice(0, 200), summary_es: String(j.summary_es || '').slice(0, 200), parsed: true };
  } catch (_) { return fallback; }
}

// ── Poke window and ladder ──────────────────────────────────────────────────
function crParts(ms) {
  const d = new Date(ms - CR_OFFSET_MS);
  return { dow: d.getUTCDay(), hour: d.getUTCHours(), y: d.getUTCFullYear(), mo: d.getUTCMonth(), day: d.getUTCDate() };
}
function inPokeWindow(ms) {
  const { dow, hour } = crParts(ms);
  return dow !== 0 && hour >= CONFIG.windowFromHour && hour < CONFIG.windowToHour;
}
// When the clock starts: the message time if it landed inside the window,
// otherwise the next window opening (08:00 CR on the next Monday to Saturday).
function clockStart(ms) {
  if (inPokeWindow(ms)) return ms;
  let { y, mo, day, hour } = crParts(ms);
  let cand = Date.UTC(y, mo, day, CONFIG.windowFromHour) + CR_OFFSET_MS;
  if (hour >= CONFIG.windowFromHour) cand += 24 * H;
  while (!inPokeWindow(cand)) cand += 24 * H;
  return cand;
}
// sent = { poke1At: ms|null, poke2At: ms|null }
function nextAlertStep({ now, anchorAt, verdict, sent = {} }) {
  if (verdict !== 'hot') return null;
  if (!inPokeWindow(now)) return null;
  const start = clockStart(anchorAt);
  if (!sent.poke1At) return now >= start + CONFIG.poke1AfterMs ? 'poke1' : null;
  if (!sent.poke2At && now >= start + CONFIG.poke2AfterMs && now >= sent.poke1At + CONFIG.poke2MinGapMs) return 'poke2';
  return null;
}

// GHL assignedTo → who gets poked. setterSlackByGhlId is keyed lowercase.
function routeOwner(assignedTo, setterSlackByGhlId) {
  const id = String(assignedTo || '').trim();
  if (!id) return { kind: 'unassigned' };
  const slackId = setterSlackByGhlId[id.toLowerCase()];
  if (slackId) return { kind: 'setter', slackId };
  return { kind: 'other' }; // a closer or Ron owns it: not a setter's chat
}

// ── Formatting ──────────────────────────────────────────────────────────────
function ago(ms) {
  const m = Math.max(0, Math.round(ms / MIN));
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  return h < 24 ? `${h}h ${m % 60}min` : `${Math.floor(h / 24)}d ${h % 24}h`;
}
function shortQuote(q, max = 220) {
  const s = String(q || '').replace(/\s+/g, ' ').trim();
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}
// item = { contactName, channel, quote, reason_es, link, anchorAt }
function formatPokeCard(step, item, { now, mention = '' } = {}) {
  const head = step === 'poke2'
    ? `⏰ *Sigue sin respuesta* · ${item.contactName} (${CHANNEL_LABEL[item.channel] || 'chat'}, hace ${ago(now - item.anchorAt)})`
    : `🔥 *Respuesta caliente sin contestar* · ${item.contactName} (${CHANNEL_LABEL[item.channel] || 'chat'}, hace ${ago(now - item.anchorAt)})`;
  return [
    mention ? `${mention} ${head}` : head,
    `> ${shortQuote(item.quote)}`,
    item.reason_es ? `Por qué: ${item.reason_es}` : null,
    `<${item.link}|Abrir en GHL>`,
  ].filter(Boolean).join('\n');
}

// rows = [{ ownerLabel, contactName, channel, quote, anchorAt }]
function formatEodSection(rows, { now }) {
  if (!rows.length) return '';
  const byOwner = new Map();
  for (const r of rows) {
    const k = r.ownerLabel || 'Unassigned';
    if (!byOwner.has(k)) byOwner.set(k, []);
    byOwner.get(k).push(r);
  }
  const out = [`HOT REPLIES STILL UNANSWERED (${rows.length}): prioritize these tomorrow`, ''];
  for (const [owner, list] of [...byOwner.entries()].sort((a, b) => b[1].length - a[1].length)) {
    out.push(`*${owner}* (${list.length})`);
    for (const r of list.sort((a, b) => a.anchorAt - b.anchorAt)) {
      out.push(`• ${r.contactName} (${CHANNEL_LABEL[r.channel] || 'chat'}, waiting ${ago(now - r.anchorAt)}): "${shortQuote(r.quote, 140)}"`);
    }
    out.push('');
  }
  return out.join('\n').trim();
}

// ── Intake gap (Instagram + Messenger, zero LLM calls) ──────────────────────
// The rule needs an inbound message from the contact, so Ron's own unanswered
// outreach DMs (10 of them on 2026-10-03, 19:59 to 20:59 UTC) never alert.
// firstInboundAt: earliest inbound message on an intake channel inside the
// lookback, or null. oppCreatedAt: newest opportunity's createdAt, or null.
function intakeGapVerdict({ convo, firstInboundAt, hasLeadPost, oppCreatedAt = null, now, personalTag = 'personal' }) {
  if (!convo || !INTAKE_CHANNELS.has(convo.lastMessageType)) return null;
  if (lowerTags(convo.tags).includes(String(personalTag).toLowerCase())) return null;
  if (!firstInboundAt) return null;                                   // outbound-only thread
  if (firstInboundAt < now - CONFIG.intakeLookbackMs) return null;    // older than the window
  if (now - firstInboundAt < CONFIG.intakeMinAgeMs) return null;      // intake still has time
  if (hasLeadPost) return null;
  const opps = Array.isArray(convo.opportunities) ? convo.opportunities : [];
  if (!opps.length) return { code: 'no_card' };
  if (oppCreatedAt && oppCreatedAt >= now - CONFIG.intakeLookbackMs) return { code: 'no_post' };
  return null; // an older card with no post predates lead_posts; not this rule's business
}

// items = [{ code, contactName, channel, firstInboundAt, link }]
function formatIntakeGapAlert(items, { now }) {
  const noCard = items.filter(i => i.code === 'no_card');
  const noPost = items.filter(i => i.code === 'no_post');
  const out = ['🚨 *SOCIAL INTAKE GAP*'];
  if (noCard.length) {
    out.push('', `${noCard.length} Instagram/Messenger contact(s) wrote to us and have NO GHL card and NO Slack lead post:`);
    for (const i of noCard) out.push(`• ${i.contactName} (${CHANNEL_LABEL[i.channel] || 'chat'}, first message ${ago(now - i.firstInboundAt)} ago) <${i.link}|GHL>`);
    out.push('Check the GHL workflow "Social DM Intake (on reply)." (published? both reply triggers present?).');
  }
  if (noPost.length) {
    out.push('', `${noPost.length} contact(s) got a GHL card but NO Slack lead post:`);
    for (const i of noPost) out.push(`• ${i.contactName} (${CHANNEL_LABEL[i.channel] || 'chat'}) <${i.link}|GHL>`);
    out.push('Max\'s /webhook/ghl-lead did not post them. Check Railway logs for ghl-lead.');
  }
  return out.join('\n');
}

// ── Reporting rows (lead_reply_signals, migration 017) ─────────────────────
const CHANNEL_KEY = { TYPE_WHATSAPP: 'whatsapp', TYPE_INSTAGRAM: 'instagram', TYPE_FACEBOOK: 'messenger' };

// First real team reply after `afterMs`, or null. Same answer rule as the ladder.
function firstAnswerAfter(messages, afterMs, isAutomatedBody = () => false) {
  const t = (messages || []).filter(m => msgTime(m) > afterMs && isTeamAnswer(m, isAutomatedBody)).map(msgTime);
  return t.length ? Math.min(...t) : null;
}

// One lead_reply_signals row for an unanswered run. `existing` is the stored
// row (or null): ever_hot is sticky, so a run once called hot stays countable
// as hot even if a later message reclassifies it.
function signalRow({ convo, analysis, verdict, ownerLabel, mode, now, existing = null }) {
  return {
    anchor_message_id: analysis.anchorId,
    newest_message_id: analysis.newestId,
    conversation_id: convo.id,
    contact_id: convo.contactId,
    contact_name: convo.contactName || convo.fullName || null,
    channel: CHANNEL_KEY[analysis.channel] || 'whatsapp',
    owner_ghl_id: convo.assignedTo || null,
    owner_label: ownerLabel || 'Unassigned',
    call_booked: isCallBooked(convo),
    verdict: verdict.verdict,
    ever_hot: !!(existing && existing.ever_hot) || verdict.verdict === 'hot',
    reason_es: verdict.reason_es || null,
    summary_es: verdict.summary_es || null,
    message_count: analysis.waitingCount,
    lead_message_at: new Date(analysis.anchorAt).toISOString(),
    classified_at: new Date(now).toISOString(),
    mode,
    updated_at: new Date(now).toISOString(),
  };
}

// Fail-closed counter: returns { count, postBroken } for the next stored value.
function nextFailureState(prevCount, failed) {
  if (!failed) return { count: 0, postBroken: false };
  const count = (Number(prevCount) || 0) + 1;
  return { count, postBroken: count === CONFIG.failuresBeforeBrokenPost };
}

module.exports = {
  CONFIG, CHANNEL_LABEL, CALL_BOOKED_STAGE_ID,
  screenConversation, isCallBooked, isTeamAnswer, analyzeThread,
  buildClassifierPrompt, parseVerdict,
  inPokeWindow, clockStart, nextAlertStep, routeOwner,
  formatPokeCard, formatEodSection,
  intakeGapVerdict, formatIntakeGapAlert, nextFailureState,
  CHANNEL_KEY, firstAnswerAfter, signalRow,
  msgTime,
};
