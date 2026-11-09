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

module.exports = { checkRequest, formatRequest, createSeen, escapeSlack, MAX_EXCERPT };
