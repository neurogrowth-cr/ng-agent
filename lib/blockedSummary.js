'use strict';
// One line of signal for a blocked client (Ron, 2026-11-06: the ops brief listed
// every blocked activity, 17 titles for one client, and nobody reads that).
// When the portal blocks a client it flips EVERY activity to 'blocked', so the
// list is the template roster, not a reason. The reason, when anyone logged one,
// lives on client_dashboards.blocked_reason (required by the dash Kanban since
// dash #430, 2026-11-07) or, before that, in customer_activities.notes.
// Pure, tested in test/blocked-summary.test.js.

const MAX_REASON = 140;

/**
 * acts: customer_activities rows ({ status, notes, updated_at }).
 * dash: the client_dashboards row ({ customer_status, blocked_reason, blocked_at }), optional.
 * Returns e.g. "blocked since 2026-11-05: client has not sent LinkedIn access"
 * or "blocked since 2026-11-05: no reason logged in the portal".
 * Returns '' when nothing is blocked (no blocked activity and the client is not blocked).
 */
function blockedSummary(acts = [], dash = null) {
  const blocked = (acts || []).filter(a => a && a.status === 'blocked');
  const clientBlocked = !!dash && dash.customer_status === 'blocked';
  if (!blocked.length && !clientBlocked) return '';
  // The portal fields count only while the client is blocked; the dash clears
  // them on unblock, but a stale pair must never label a phase_1 client.
  const portalReason = clientBlocked && typeof dash.blocked_reason === 'string' ? dash.blocked_reason.trim() : '';
  const noted = blocked
    .filter(a => typeof a.notes === 'string' && a.notes.trim())
    .sort((a, b) => String(b.updated_at || '').localeCompare(String(a.updated_at || '')));
  const since = (clientBlocked && dash.blocked_at) || blocked.map(a => a.updated_at).filter(Boolean).sort()[0];
  const head = since ? `blocked since ${String(since).slice(0, 10)}` : 'blocked';
  let reason = portalReason
    ? portalReason.replace(/\s+/g, ' ')
    : noted.length ? noted[0].notes.replace(/\s+/g, ' ').trim() : 'no reason logged in the portal';
  if (reason.length > MAX_REASON) reason = `${reason.slice(0, MAX_REASON - 1).trimEnd()}…`;
  return `${head}: ${reason}`;
}

module.exports = { blockedSummary, MAX_REASON };
