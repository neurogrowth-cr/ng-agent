'use strict';
// One line of signal for a blocked client (Ron, 2026-11-06: the ops brief listed
// every blocked activity, 17 titles for one client, and nobody reads that).
// When the portal blocks a client it flips EVERY activity to 'blocked', so the
// list is the template roster, not a reason. The reason, when anyone logged one,
// lives in customer_activities.notes. Pure, tested in test/blocked-summary.test.js.

const MAX_REASON = 140;

/**
 * acts: customer_activities rows ({ status, notes, updated_at }).
 * Returns e.g. "blocked since 2026-11-05: client has not sent LinkedIn access"
 * or "blocked since 2026-11-05: no reason logged in the portal".
 * Returns '' when no activity is blocked.
 */
function blockedSummary(acts = []) {
  const blocked = (acts || []).filter(a => a && a.status === 'blocked');
  if (!blocked.length) return '';
  const noted = blocked
    .filter(a => typeof a.notes === 'string' && a.notes.trim())
    .sort((a, b) => String(b.updated_at || '').localeCompare(String(a.updated_at || '')));
  const since = blocked.map(a => a.updated_at).filter(Boolean).sort()[0];
  const head = since ? `blocked since ${String(since).slice(0, 10)}` : 'blocked';
  let reason = noted.length ? noted[0].notes.replace(/\s+/g, ' ').trim() : 'no reason logged in the portal';
  if (reason.length > MAX_REASON) reason = `${reason.slice(0, MAX_REASON - 1).trimEnd()}…`;
  return `${head}: ${reason}`;
}

module.exports = { blockedSummary, MAX_REASON };
