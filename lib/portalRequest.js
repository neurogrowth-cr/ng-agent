'use strict';
// Portal requests: one Slack post per help request (Ayuda) or campaign request
// (Campañas) a client sends in the dash. Clients are told to ask there instead
// of the WhatsApp group (onboarding video, Nov 2026), so every request must
// reach the team. The dash calls POST /webhook/portal-request right after it
// saves the row (src/lib/alerts/portal-request-alert.ts in the dash repo), with
// the shared CLIENT_ATTENTION_FEED_SECRET; Max checks, formats and posts it to
// #ng-marktech-internal tagging @fulfillment, or to Ron's DM on a dry run.
//
// Everything here is pure (no Slack, no network) and tested in
// test/portal-request.test.js.

const KINDS = {
  help: { emoji: '🆘', title: 'help request', labelName: 'Category' },
  campaign: { emoji: '📣', title: 'campaign request', labelName: 'Type' },
};
const MODES = new Set(['live', 'dry_run']);
const MAX_EXCERPT = 400;
// Only links back into the dash admin, so the endpoint cannot be used to post
// arbitrary links into a team channel.
const ADMIN_URL = /^https:\/\/dash\.neurogrowth\.io\/admin[^\s|<>]*$/;

/** Slack mrkdwn treats &, < and > as control characters. */
function escapeSlack(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function oneLine(s) {
  return String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
}

function clip(s, max) {
  return s.length <= max ? s : `${s.slice(0, max - 1).trimEnd()}…`;
}

/** Validates the dash payload. Fails closed on anything unexpected. */
function checkRequest(body) {
  if (!body || typeof body !== 'object') return { ok: false, problem: 'body is not an object' };
  const id = oneLine(body.id);
  if (!id || id.length > 120) return { ok: false, problem: 'id missing' };
  if (!KINDS[body.kind]) return { ok: false, problem: `unknown kind ${JSON.stringify(body.kind)}` };
  const clientName = oneLine(body.clientName);
  if (!clientName) return { ok: false, problem: 'clientName missing' };
  const label = oneLine(body.label);
  if (!label) return { ok: false, problem: 'label missing' };
  const excerpt = oneLine(body.excerpt);
  if (!excerpt) return { ok: false, problem: 'excerpt missing' };
  const adminUrl = oneLine(body.adminUrl);
  if (!ADMIN_URL.test(adminUrl)) return { ok: false, problem: 'adminUrl is not a dash admin link' };
  const priority = body.priority == null ? null : oneLine(body.priority) || null;
  // A missing mode is a dry run: only an explicit "live" reaches the channel.
  const mode = MODES.has(body.mode) ? body.mode : 'dry_run';
  return {
    ok: true,
    request: {
      id,
      kind: body.kind,
      clientName: clip(clientName, 120),
      label: clip(label, 80),
      priority: priority && priority !== '-' ? clip(priority, 20) : null,
      excerpt: clip(excerpt, MAX_EXCERPT),
      adminUrl,
      mode,
    },
  };
}

/** The Slack text. `mention` is the usergroup mention, or '' on a dry run. */
function formatRequest(request, { mention = '' } = {}) {
  const kind = KINDS[request.kind];
  const head = `${mention ? `${mention} ` : ''}${kind.emoji} New ${kind.title} from *${escapeSlack(request.clientName)}*`;
  const meta = `${kind.labelName}: ${escapeSlack(request.label)}${request.priority ? ` · Priority: ${escapeSlack(request.priority)}` : ''}`;
  return [
    head,
    meta,
    `> ${escapeSlack(request.excerpt)}`,
    `<${request.adminUrl}|Open in admin> · Answer in the portal so the client sees it there.`,
  ].join('\n');
}

/** Remembers request ids for `ttlMs`, so a repeated call never posts twice. */
function createSeen(ttlMs, now = () => Date.now()) {
  const seen = new Map();
  const prune = () => {
    const t = now();
    for (const [id, at] of seen) if (t - at > ttlMs) seen.delete(id);
  };
  return {
    has(id) { prune(); return seen.has(id); },
    add(id) { seen.set(id, now()); },
    delete(id) { seen.delete(id); },
    get size() { return seen.size; },
  };
}

// ─── Updates: taken, updated, overdue, closed ────────────────────────────────
// The dash calls POST /webhook/portal-request-update when a team member takes
// a help request ("Take it" in Admin, Feedback), sends the client an update
// ("Send update"), or closes it, and its hourly feedback-overdue job calls it
// once when a request passes its response time (Ron, 2026-11-27: High 24 h,
// Medium 48 h, Low 72 h, until the first update or the close). Max answers in
// the thread of its own alert, so the channel shows what is still waiting.
// The dash stores the alert's channel and ts (returned by the first call);
// requests alerted before that have none and get a standalone line instead.

const EVENTS = new Set(['claimed', 'updated', 'overdue', 'closed']);
const RESPONSE_HOURS = new Set([24, 48, 72]);
const MAX_ANSWER = 300;
const SLACK_TS = /^\d{9,11}\.\d{1,9}$/;
const SLACK_CHANNEL = /^[CDG][A-Z0-9]{6,20}$/;

/** Validates an update payload. Fails closed on anything unexpected. */
function checkUpdate(body) {
  if (!body || typeof body !== 'object') return { ok: false, problem: 'body is not an object' };
  const id = oneLine(body.id);
  if (!id || id.length > 120) return { ok: false, problem: 'id missing' };
  if (!EVENTS.has(body.event)) return { ok: false, problem: `unknown event ${JSON.stringify(body.event)}` };
  // Overdue comes from the dash's job, not a person: no "by", but the response
  // time it missed. Everything else is a team member's action.
  const overdue = body.event === 'overdue';
  const by = oneLine(body.by);
  if (!by && !overdue) return { ok: false, problem: 'by missing' };
  const hours = overdue ? Number(body.hours) : null;
  if (overdue && !RESPONSE_HOURS.has(hours)) return { ok: false, problem: `hours must be 24, 48 or 72` };
  const clientName = oneLine(body.clientName);
  if (!clientName) return { ok: false, problem: 'clientName missing' };
  // What the client was told: the update, or the answer on close.
  const answer = body.event === 'closed' || body.event === 'updated' ? oneLine(body.answer) : '';
  if (body.event === 'updated' && !answer) return { ok: false, problem: 'update text missing' };
  const claimedBy = overdue ? oneLine(body.claimedBy) : '';
  const channel = oneLine(body.channel);
  const ts = oneLine(body.ts);
  const hasThread = SLACK_CHANNEL.test(channel) && SLACK_TS.test(ts);
  const mode = MODES.has(body.mode) ? body.mode : 'dry_run';
  return {
    ok: true,
    update: {
      id,
      event: body.event,
      by: by ? clip(by, 80) : '',
      clientName: clip(clientName, 120),
      answer: answer ? clip(answer, MAX_ANSWER) : '',
      hours,
      claimedBy: claimedBy ? clip(claimedBy, 80) : '',
      channel: hasThread ? channel : null,
      ts: hasThread ? ts : null,
      mode,
    },
  };
}

/**
 * Where the update goes. In the alert's thread when the dash sent one that
 * Max itself could have posted: the request channel when live, a DM (Ron's,
 * where dry runs land) otherwise. Anything else is ignored and the update
 * posts as a standalone line, so a payload can never steer Max into another
 * channel's threads.
 */
function threadTarget(update, { liveChannel }) {
  if (!update.channel || !update.ts) return null;
  if (update.mode === 'live' ? update.channel === liveChannel : update.channel.startsWith('D')) {
    return { channel: update.channel, ts: update.ts };
  }
  return null;
}

/**
 * The Slack text. In a thread it is short; standalone it names the client.
 * `mention` (the @fulfillment usergroup) is only used on overdue, live.
 */
function formatUpdate(update, { threaded, mention = '' }) {
  const by = `*${escapeSlack(update.by)}*`;
  const client = `*${escapeSlack(update.clientName)}*`;
  if (update.event === 'claimed') {
    return threaded ? `👀 ${by} took it.` : `👀 ${by} took the help request from ${client}.`;
  }
  if (update.event === 'updated') {
    const head = threaded ? `💬 ${by} sent the client an update:` : `💬 ${by} sent ${client} an update on their help request:`;
    return `${head}\n> ${escapeSlack(update.answer)}`;
  }
  if (update.event === 'overdue') {
    const who = update.claimedBy ? `Taken by *${escapeSlack(update.claimedBy)}*.` : 'Nobody has taken it.';
    const what = threaded
      ? `Past its ${update.hours} h response time with no update to the client.`
      : `The help request from ${client} is past its ${update.hours} h response time with no update to the client.`;
    return `${mention ? `${mention} ` : ''}⏰ ${what} ${who}`;
  }
  const head = threaded ? `✅ Closed by ${by}.` : `✅ ${by} closed the help request from ${client}.`;
  return update.answer
    ? `${head} Answer sent to the client:\n> ${escapeSlack(update.answer)}`
    : `${head} Closed with no answer: the client only sees Resolved.`;
}

/** Reactions on the original alert: 👀 while someone has it, ⏰ while overdue, ✅ once closed. */
function updateReactions(event) {
  switch (event) {
    case 'claimed':
      return { add: ['eyes'], remove: [] };
    case 'updated':
      return { add: ['eyes'], remove: ['alarm_clock'] };
    case 'overdue':
      return { add: ['alarm_clock'], remove: [] };
    default:
      return { add: ['white_check_mark'], remove: ['eyes', 'alarm_clock'] };
  }
}

module.exports = {
  checkRequest,
  formatRequest,
  createSeen,
  escapeSlack,
  MAX_EXCERPT,
  checkUpdate,
  threadTarget,
  formatUpdate,
  updateReactions,
  MAX_ANSWER,
};
