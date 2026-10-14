'use strict';
// Content Bridge, the 24h setter card. Pure logic, no I/O, so
// test/bridge-reminder.test.js can require it directly.
//
// The day before each flywheel strategy call, Max DMs the setter who owns the
// prospect a card with the WhatsApp text from
// ~/NG-content/content-bridge/ghl-placements.md section (d) and the link to
// video C ("Las tres maneras de conseguir clientes"). index.js owns the reads,
// the setter routing and the Slack posts; this file decides WHICH calls are due
// and WHAT the card says.
//
// Time: Costa Rica is UTC-6 with no DST, the same rule every other CR helper in
// this repo uses. Mexico City is also UTC-6 year round since 2022.

const CR_OFFSET_MS = 6 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
// A call is due once it is this close AND falls on tomorrow's CR date. With
// hourly runs from 8 AM to 8 PM CR, every call on tomorrow's date passes
// through at least one run: the 8 AM run reaches 28h ahead (noon tomorrow),
// and each later run reaches one hour further, until the 8 PM run covers the
// rest of tomorrow up to midnight. So a card lands 24 to 28h before the call,
// except calls before noon tomorrow, which all get theirs at 8 AM today.
const BRIDGE_MAX_LEAD_HOURS = 28;
const VIDEO_PENDING_TEXT = '(video C pendiente de grabar)';

const DIAS = ['domingo', 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado'];
const MESES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];

// CR wall clock read as UTC fields.
function crClock(ms) { return new Date(ms - CR_OFFSET_MS); }

function crDateKey(ms) {
  const d = crClock(ms);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

function crTomorrowKey(nowMs) { return crDateKey(nowMs + 24 * HOUR_MS); }

// "10:00 a. m." / "3:30 p. m." (CR wall clock).
function crTimeEs(ms) {
  const d = crClock(ms);
  const h = d.getUTCHours();
  const m = String(d.getUTCMinutes()).padStart(2, '0');
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return `${h12}:${m} ${h < 12 ? 'a. m.' : 'p. m.'}`;
}

// "jueves 16 de octubre, 10:00 a. m."
function crDateTimeEs(ms) {
  const d = crClock(ms);
  return `${DIAS[d.getUTCDay()]} ${d.getUTCDate()} de ${MESES[d.getUTCMonth()]}, ${crTimeEs(ms)}`;
}

// First word of the full name, title-cased. Empty when the name is missing or
// is really an email address (the portal falls back to the email sometimes).
function firstNameOf(fullName) {
  const raw = String(fullName || '').trim();
  if (!raw || raw.includes('@')) return '';
  const first = raw.split(/\s+/)[0];
  return first.charAt(0).toLocaleUpperCase('es') + first.slice(1).toLocaleLowerCase('es');
}

// The dedupe key. It carries the call's CR date because GHL reuses the same
// appointment row on a reschedule: a call moved to another day gets a fresh
// card for its new date, and the old stamp never blocks it. Shadow and live use
// different prefixes so flipping BRIDGE_24H_LIVE mid-day does not swallow the
// first live cards.
function bridgeKey(appointmentId, startMs, live) {
  return `${live ? 'bridge-24h' : 'bridge-24h-shadow'}:${appointmentId}:${crDateKey(startMs)}`;
}

// Decide one call. Returns 'due' or a skip reason the run log counts.
//   c = { startMs, cancelled, deleted, superseded, stamped, ownerSlackId, hasOwner }
// Order matters: dead calls first, then timing, then routing, so the counts
// say why a real upcoming call got no card.
function classifyBridgeCall(c, nowMs) {
  if (!Number.isFinite(c.startMs)) return 'bad_date';
  if (c.deleted) return 'deleted';
  if (c.cancelled) return 'cancelled';
  if (c.superseded) return 'superseded';
  if (c.startMs <= nowMs) return 'started';
  const key = crDateKey(c.startMs);
  if (key === crDateKey(nowMs)) return c.stamped ? 'already_sent' : 'missed_same_day';
  if (key !== crTomorrowKey(nowMs)) return 'not_yet';
  if (c.startMs - nowMs > BRIDGE_MAX_LEAD_HOURS * HOUR_MS) return 'not_yet';
  if (c.stamped) return 'already_sent';
  if (!c.hasOwner) return 'self_serve';
  if (!c.ownerSlackId) return 'not_active_setter';
  return 'due';
}

// Section (d) of ghl-placements.md, rendered. No name -> the sentence starts
// with "Mañana" so it still reads right.
function buildWhatsAppText({ firstName, startMs, videoUrl }) {
  const time = crTimeEs(startMs);
  const opener = firstName ? `${firstName}, mañana nos vemos (${time}).` : `Mañana nos vemos (${time}).`;
  const link = String(videoUrl || '').trim() || VIDEO_PENDING_TEXT;
  return [
    `${opener} Si tiene 10 minutos hoy, vea esto antes, para que la llamada sea sobre sus números, no lo básico:`,
    link,
    'Y si decide con un socio, mándeselo también. Nos vemos mañana.',
  ].join('\n');
}

// The Slack card. shadowFor = the intended setter's label while
// BRIDGE_24H_LIVE is false (the card then goes to Ron, never the setter).
function buildBridgeCard({ prospectName, startMs, closerName, whatsappText, ghlLink, shadowFor, videoMissing }) {
  const head = `📹 *Víspera de llamada* · ${prospectName || 'Prospecto'} · ${crDateTimeEs(startMs)} (hora CR)`;
  return [
    shadowFor ? `[SHADOW · iría a ${shadowFor}]` : null,
    head,
    closerName ? `Closer: ${closerName}` : null,
    'Mándele este WhatsApp hoy (copie y pegue):',
    '```',
    whatsappText,
    '```',
    'La hora es de Costa Rica. Si el prospecto está en otra zona horaria, ajústela antes de enviar.',
    videoMissing ? `⚠️ El video C todavía no existe: el mensaje dice "${VIDEO_PENDING_TEXT}". No lo envíe así.` : null,
    ghlLink ? `<${ghlLink}|Abrir en GHL>` : null,
  ].filter(Boolean).join('\n');
}

// Structural contract for a card about to be posted (recipe criteria 4a).
// Returns a list of problems; empty means OK.
function validateBridgeCard(card, { videoUrl } = {}) {
  const s = String(card || '');
  const problems = [];
  if (!s.includes('*Víspera de llamada*')) problems.push('missing header');
  if (!s.includes('Si tiene 10 minutos hoy')) problems.push('missing WhatsApp body');
  if (!/\d{1,2}:\d{2} [ap]\. m\./.test(s)) problems.push('missing call time');
  const url = String(videoUrl || '').trim();
  if (url ? !s.includes(url) : !s.includes(VIDEO_PENDING_TEXT)) problems.push('missing video link or placeholder');
  if (/undefined|\[object Object\]|NaN|\bnull\b/.test(s)) problems.push('leaked empty value');
  if (s.includes('\u2014')) problems.push('em dash');
  return problems;
}

module.exports = {
  BRIDGE_MAX_LEAD_HOURS, VIDEO_PENDING_TEXT,
  crDateKey, crTomorrowKey, crTimeEs, crDateTimeEs, firstNameOf, bridgeKey,
  classifyBridgeCall, buildWhatsAppText, buildBridgeCard, validateBridgeCard,
};
