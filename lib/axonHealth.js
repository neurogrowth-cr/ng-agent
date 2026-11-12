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
};
