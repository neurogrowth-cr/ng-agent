// AXON health watch: pure logic (classification, streaks, alert text).
// index.js owns the Supabase read and the Slack posts.
//
// Two failure kinds, never mixed:
//   down  = the heartbeat read SUCCEEDED and the beat is stale or missing.
//           AXON itself is the suspect: check Railway, redeploy if CRASHED.
//   blind = the heartbeat read FAILED (error or throw). Max cannot see AXON,
//           which usually means the shared Supabase project is not answering.
//           AXON may be fine. Redeploying it fixes nothing.
// On 2026-11-12 (01:17 to 03:40 UTC) Supabase stopped answering and Max posted
// "AXON looks DOWN ... redeploy it" while AXON was alive the whole time. That
// is the case `blind` exists for.
//
// Each kind keeps its own 3-poll streak, its own 6h re-alert and its own
// recovery line, so the recovery always matches what was alerted.

const AXON_HEARTBEAT_STALE_MS = 20 * 60 * 1000;
const AXON_WATCH_REALERT_MS = 6 * 60 * 60 * 1000;
const AXON_WATCH_STREAK = 3;
const ERROR_LINE_MAX = 200;

const GATEWAY_CODE_LABELS = {
  500: 'internal error',
  502: 'bad gateway',
  503: 'service unavailable',
  504: 'gateway timeout',
  520: 'unknown error',
  521: 'server down',
  522: 'connection timed out',
  523: 'origin unreachable',
  524: 'timeout',
};

function clipLine(s, max) {
  const flat = String(s || '').replace(/\s+/g, ' ').trim();
  return flat.length > max ? `${flat.slice(0, max - 1).trimEnd()}…` : flat;
}

function looksLikeHtml(s) {
  return /<!doctype html|<html[\s>]|<head[\s>]|<body[\s>]|<title[\s>]/i.test(String(s || ''));
}

// One readable line from any error. Supabase behind Cloudflare can hand back a
// whole HTML 522 page as error.message; that must never reach Slack raw.
function errorOneLine(err, max = ERROR_LINE_MAX) {
  const raw = err == null ? ''
    : typeof err === 'string' ? err
    : typeof err.message === 'string' ? err.message
    : String(err);
  if (!looksLikeHtml(raw)) return clipLine(raw, max) || 'error with no message';
  const titleMatch = raw.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const title = titleMatch ? clipLine(titleMatch[1].replace(/<[^>]+>/g, ' '), max) : '';
  const codeMatch = raw.match(/Error code\s*(\d{3})/i) || title.match(/\b(5\d\d)\b/);
  const service = /supabase/i.test(raw) ? 'Supabase' : 'The server';
  if (codeMatch) {
    const code = Number(codeMatch[1]);
    const label = GATEWAY_CODE_LABELS[code];
    return clipLine(`${service} did not respond (${code}${label ? `, ${label}` : ''})`, max);
  }
  if (title) return clipLine(`${service} returned an error page: ${title}`, max);
  return `${service} returned an HTML error page`;
}

// Classify one poll. Inputs mirror what index.js has after the read:
//   { data, error } from supabase-js, or `thrown` when the call threw.
// Returns { kind: 'ok' | 'down' | 'blind', reason }.
function classifyAxonHeartbeat({ data, error, thrown, now = Date.now() } = {}) {
  if (thrown) return { kind: 'blind', reason: `heartbeat check threw: ${errorOneLine(thrown)}` };
  if (error) return { kind: 'blind', reason: `heartbeat read failed: ${errorOneLine(error)}` };
  if (!data || !data.length) {
    return { kind: 'down', reason: 'no heartbeat row exists, AXON has never beaten (pre-heartbeat deploy, or the migration is missing)' };
  }
  const age = now - new Date(data[0].beat_at).getTime();
  if (!(age < AXON_HEARTBEAT_STALE_MS)) {
    return { kind: 'down', reason: `last beat ${Math.round(age / 60000)} min ago (cadence is 5 min)` };
  }
  return { kind: 'ok', reason: null };
}

function newAxonWatchState() {
  return {
    down: { streak: 0, alerted: false, lastAlertAt: 0 },
    blind: { streak: 0, alerted: false, lastAlertAt: 0 },
  };
}

function downAlertText(reason) {
  return `🔴 *AXON looks DOWN*: ${reason}.\n` +
    'Nothing AXON does (campaign monitoring, digests, Document Factory, approvals) is running. ' +
    'Check the ng-fulfillment-AXON Railway service for a crash loop (deployment logs); if the latest deployment says CRASHED, redeploy it. ' +
    'This alert repeats every 6h until the heartbeat returns.';
}

function blindAlertText(reason) {
  return `🟠 *Max cannot read AXON's heartbeat*: ${reason}.\n` +
    'This is a database problem, not proof that AXON is down: the shared Supabase project is not answering Max, and AXON may be running fine. ' +
    '*Do not redeploy AXON.* Check status.supabase.com and the Supabase dashboard for the project; AXON\'s own work that needs the database will also be failing until it recovers. ' +
    'This alert repeats every 6h until the read works again.';
}

const DOWN_RECOVERY_TEXT = '✅ *AXON is beating again*: heartbeat fresh, service recovered.';
const BLIND_RECOVERY_TEXT = '✅ *Max can read AXON\'s heartbeat again*: the database is answering.';

// Advance the watch by one poll. Mutates nothing; returns the next state and
// the messages to post (in order). A blind poll leaves the down streak alone:
// we learned nothing about AXON, so neither count nor reset.
function stepAxonWatch(prev, result, now = Date.now()) {
  const state = {
    down: { ...prev.down },
    blind: { ...prev.blind },
  };
  const posts = [];

  const fire = (k, text) => {
    const s = state[k];
    s.streak += 1;
    if (s.streak < AXON_WATCH_STREAK) return;
    if (s.alerted && now - s.lastAlertAt < AXON_WATCH_REALERT_MS) return;
    s.alerted = true;
    s.lastAlertAt = now;
    posts.push(text);
  };
  const clear = (k, text) => {
    const s = state[k];
    s.streak = 0;
    if (s.alerted) {
      s.alerted = false;
      s.lastAlertAt = 0;
      posts.push(text);
    }
  };

  if (result.kind === 'blind') {
    fire('blind', blindAlertText(result.reason));
    return { state, posts };
  }
  // The read worked, so Max can see again.
  clear('blind', BLIND_RECOVERY_TEXT);
  if (result.kind === 'down') fire('down', downAlertText(result.reason));
  else clear('down', DOWN_RECOVERY_TEXT);
  return { state, posts };
}

// ─── Inbox watch: a cron that must not die quietly ──────────────────────────
// AXON's `inbox_watch` cron (12,42 6-20 * * * Costa Rica) reads Prosp's web
// inbox for replies the webhook has not announced. AXON's own cron alert only
// sees a run that fails. A cron that never fires (AXON down, cron not
// scheduled, no stored Prosp session, PROSP_INBOX_WATCH=off left on) was
// silent, and the dash sweep hides it by bringing the replies in hours later.
// Since ng-axon #373 a run that read the inbox stamps its own row in
// axon.engine_heartbeat. The id is a contract with ng-axon lib/pure.js
// (cronBeatRow); change both the same day.
//
// Rule: inside the cron's hours (06:00 to 20:59 Costa Rica), no completed run
// for 35 minutes is stale. The clock starts at 06:00 each day, so the night
// never counts. The cron runs every 10 minutes since 2026-11-27 (ng-axon #453;
// every 30 before, when this waited 75 minutes). 35 minutes is three missed
// runs plus the time a run takes: one or two failed or skipped runs (a blip,
// two AXON deploys in a row) never alert.
// One alert per outage, keyed by the last completed run, and a recovery line
// when a newer run appears. No streak and no re-alert: the timestamp does not
// flap, and Ron asked for one message. A failed READ decides nothing here; the
// service check above already says "Max cannot read".
const INBOX_WATCH_BEAT_ID = 'cron:inbox_watch';
const INBOX_WATCH_STALE_MS = 35 * 60 * 1000;
const INBOX_WATCH_FIRST_HOUR = 6;
const INBOX_WATCH_LAST_HOUR = 20;
const INBOX_WATCH_NEVER = 'never';
const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // America/Costa_Rica, no DST
const HOUR_MS = 60 * 60 * 1000;
const MESES = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];

// Costa Rica wall clock of an instant: its hour and today's 06:00 as an instant.
function crClock(now) {
  const local = new Date(now - CR_OFFSET_MS);
  const midnight = Date.UTC(local.getUTCFullYear(), local.getUTCMonth(), local.getUTCDate()) + CR_OFFSET_MS;
  return { hour: local.getUTCHours(), dayStart: midnight + INBOX_WATCH_FIRST_HOUR * HOUR_MS };
}

function crTime(ms) {
  const d = new Date(ms - CR_OFFSET_MS);
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
}

function crDay(ms) {
  const d = new Date(ms - CR_OFFSET_MS);
  return `${d.getUTCDate()} ${MESES[d.getUTCMonth()]}`;
}

function spanText(ms) {
  const min = Math.max(0, Math.round(ms / 60000));
  if (min < 120) return `${min} min`;
  const h = Math.floor(min / 60);
  const m = min % 60;
  return m ? `${h} h ${m} min` : `${h} h`;
}

// Classify one poll of the cron's row. Inputs as in classifyAxonHeartbeat.
// Returns { kind: 'blind' } or
//   { kind: 'ok' | 'stale' | 'quiet', beatAt, beatMs, sinceMs, now }
// quiet = outside the cron's hours: never stale, a recovery can still be seen.
function classifyInboxWatch({ data, error, thrown, now = Date.now() } = {}) {
  if (thrown) return { kind: 'blind', reason: `heartbeat check threw: ${errorOneLine(thrown)}` };
  if (error) return { kind: 'blind', reason: `heartbeat read failed: ${errorOneLine(error)}` };
  const row = data && data.length ? data[0] : null;
  const parsed = row && row.beat_at ? new Date(row.beat_at).getTime() : NaN;
  const beatMs = Number.isFinite(parsed) ? parsed : null;
  const beatAt = beatMs == null ? null : new Date(beatMs).toISOString();
  const { hour, dayStart } = crClock(now);
  const sinceMs = now - Math.max(beatMs == null ? 0 : beatMs, dayStart);
  const base = { beatAt, beatMs, sinceMs, now };
  if (hour < INBOX_WATCH_FIRST_HOUR || hour > INBOX_WATCH_LAST_HOUR) return { kind: 'quiet', ...base };
  return { kind: sinceMs >= INBOX_WATCH_STALE_MS ? 'stale' : 'ok', ...base };
}

function inboxWatchAlertText(r) {
  const { dayStart } = crClock(r.now);
  const what = r.beatMs == null
    ? 'nunca ha registrado una lectura completa'
    : r.beatMs < dayStart
      ? `hoy no ha completado ninguna lectura (la última fue el ${crDay(r.beatMs)} a las ${crTime(r.beatMs)}, hora de Costa Rica)`
      : `no completa una lectura desde las ${crTime(r.beatMs)}, hora de Costa Rica (hace ${spanText(r.now - r.beatMs)})`;
  return `🔴 *La vigilancia del inbox de AXON dejó de correr*: ${what}. Debe correr cada 10 minutos entre las 06:00 y las 20:59.\n` +
    'Las respuestas de los prospectos siguen entrando por el webhook de Prosp y por el barrido del dash, pero pueden tardar horas en llegar a Kai.\n' +
    'Qué revisar: (1) si AXON avisó en #ng-fulfillment-axon que el cron `inbox_watch` falló, es la misma causa; si pide volver a entrar a Prosp, corra `node scripts/prosp-web-session.js` en ng-axon. ' +
    '(2) Si AXON no avisó nada, revise el servicio ng-fulfillment-AXON en Railway (caído o reiniciando) y que la variable `PROSP_INBOX_WATCH` no esté en `off`.\n' +
    'Este aviso sale una sola vez por caída. Max avisa aquí cuando vuelva a correr.';
}

function inboxWatchRecoveryText(r, alertedKey) {
  const prev = alertedKey && alertedKey !== INBOX_WATCH_NEVER ? new Date(alertedKey).getTime() : NaN;
  const gap = Number.isFinite(prev) && r.beatMs > prev ? `, después de ${spanText(r.beatMs - prev)} sin correr` : '';
  return `✅ *La vigilancia del inbox de AXON volvió a correr*: completó una lectura a las ${crTime(r.beatMs)}, hora de Costa Rica${gap}.`;
}

function newInboxWatchState() {
  return { alertedKey: null };
}

// Max keeps this state in memory and redeploys several times a day. The alert
// and the recovery are also written to agent_activity (event_type
// axon_inbox_watch), and the first poll after a boot reads the newest row
// back, so a redeploy in the middle of an outage does not post it again.
function restoreInboxWatchState(rows) {
  const last = rows && rows.length ? rows[0] : null;
  const key = last && last.action === 'stale_alert' && last.metadata ? last.metadata.beat_key : null;
  return { alertedKey: key ? String(key) : null };
}

// Advance by one poll. Returns the next state, the messages to post and the
// event to record ({ action, beat_key }) when something was said. Shared by
// every cron watch in this file; each one brings its own two texts.
function stepCronWatch(prev, r, alertText, recoveryText) {
  const state = { alertedKey: (prev && prev.alertedKey) || null };
  if (!r || r.kind === 'blind') return { state, posts: [], event: null };
  const key = r.beatAt || INBOX_WATCH_NEVER;
  if (state.alertedKey && r.beatAt && key !== state.alertedKey) {
    const text = recoveryText(r, state.alertedKey);
    state.alertedKey = null;
    return { state, posts: [text], event: { action: 'recovered', beat_key: key } };
  }
  if (r.kind === 'stale' && state.alertedKey !== key) {
    state.alertedKey = key;
    return { state, posts: [alertText(r)], event: { action: 'stale_alert', beat_key: key } };
  }
  return { state, posts: [], event: null };
}

function stepInboxWatch(prev, r) {
  return stepCronWatch(prev, r, inboxWatchAlertText, inboxWatchRecoveryText);
}

// ─── Prosp reader watch: the same rule, around the clock ────────────────────
// AXON's Prosp reader moved from Ron's Mac to Railway on 2026-11-23 (ng-axon
// #381, #396). Its cron (5-55/10 * * * *, every hour of every day) reads the
// campaigns in Prosp for every QA check, and stamps axon.engine_heartbeat id
// 'cron:prosp_web_reader' at the end of every run, in shadow and in live. AXON
// alerts Ron itself when the reader reads nothing for 3 hours or its Prosp
// session is down; a cron that stops ticking is seen only from here.
// The row 'cron:prosp_web_reader_live' is NOT watched: it is stamped only in
// live, so a rollback to shadow makes it go stale on purpose.
//
// Rule: no run for 40 minutes is stale, at any hour (three missed runs plus an
// AXON deploy restart). Same shape as the inbox watch: one alert per outage,
// keyed by the last run, and a recovery line when a newer run appears.
const PROSP_READER_BEAT_ID = 'cron:prosp_web_reader';
const PROSP_READER_STALE_MS = 40 * 60 * 1000;

// Returns { kind: 'blind' } or { kind: 'ok' | 'stale', beatAt, beatMs, now }.
function classifyProspReader({ data, error, thrown, now = Date.now() } = {}) {
  if (thrown) return { kind: 'blind', reason: `heartbeat check threw: ${errorOneLine(thrown)}` };
  if (error) return { kind: 'blind', reason: `heartbeat read failed: ${errorOneLine(error)}` };
  const row = data && data.length ? data[0] : null;
  const parsed = row && row.beat_at ? new Date(row.beat_at).getTime() : NaN;
  const beatMs = Number.isFinite(parsed) ? parsed : null;
  const beatAt = beatMs == null ? null : new Date(beatMs).toISOString();
  const stale = beatMs == null || now - beatMs >= PROSP_READER_STALE_MS;
  return { kind: stale ? 'stale' : 'ok', beatAt, beatMs, now };
}

function prospReaderAlertText(r) {
  const what = r.beatMs == null
    ? 'nunca ha registrado una corrida'
    : `no corre desde ${crDay(r.beatMs) === crDay(r.now) ? '' : `el ${crDay(r.beatMs)} a `}las ${crTime(r.beatMs)}, hora de Costa Rica (hace ${spanText(r.now - r.beatMs)})`;
  return `🔴 *El lector de Prosp de AXON dejó de correr*: ${what}. Debe correr cada 10 minutos, a toda hora.\n` +
    'Mientras no corra, AXON no lee las campañas en Prosp y el QA no se hace: ni antes del lanzamiento, ni en el lanzamiento, ni en la revisión diaria, ni cuando alguien escribe `@AXON QA` con el nombre del cliente.\n' +
    'Qué revisar: (1) primero el servicio ng-fulfillment-AXON en Railway (caído o reiniciando). ' +
    '(2) Si el servicio está arriba, que la variable `PROSP_WEB_READER` no esté en `off`.\n' +
    'Este aviso sale una sola vez por caída. Max avisa aquí cuando vuelva a correr.';
}

function prospReaderRecoveryText(r, alertedKey) {
  const prev = alertedKey && alertedKey !== INBOX_WATCH_NEVER ? new Date(alertedKey).getTime() : NaN;
  const gap = Number.isFinite(prev) && r.beatMs > prev ? `, después de ${spanText(r.beatMs - prev)} sin correr` : '';
  return `✅ *El lector de Prosp de AXON volvió a correr*: corrió a las ${crTime(r.beatMs)}, hora de Costa Rica${gap}.`;
}

function stepProspReaderWatch(prev, r) {
  return stepCronWatch(prev, r, prospReaderAlertText, prospReaderRecoveryText);
}

module.exports = {
  AXON_HEARTBEAT_STALE_MS,
  AXON_WATCH_REALERT_MS,
  AXON_WATCH_STREAK,
  errorOneLine,
  classifyAxonHeartbeat,
  newAxonWatchState,
  stepAxonWatch,
  downAlertText,
  blindAlertText,
  DOWN_RECOVERY_TEXT,
  BLIND_RECOVERY_TEXT,
  INBOX_WATCH_BEAT_ID,
  INBOX_WATCH_STALE_MS,
  INBOX_WATCH_NEVER,
  classifyInboxWatch,
  newInboxWatchState,
  restoreInboxWatchState,
  stepInboxWatch,
  inboxWatchAlertText,
  inboxWatchRecoveryText,
  PROSP_READER_BEAT_ID,
  PROSP_READER_STALE_MS,
  classifyProspReader,
  stepProspReaderWatch,
  prospReaderAlertText,
  prospReaderRecoveryText,
};
