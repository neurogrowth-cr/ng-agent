'use strict';
// Client attention: the Mon + Thu report and urgent alerts for the fulfillment
// team in #ng-fullfillment-ops (Ron, 2026-09-23; v2 on 2026-10-04: one card
// per client, every line owned, fleet items, benchmark drift off Slack).
// Recipe: ~/automations/ops/recipes/client-attention.md.
//
// The list is computed in the dash (GET /api/ops/client-attention, the same
// engine as Admin -> Campaign health); Max only formats, checks and posts it.
// Everything here is pure (no Slack, no network) and tested in
// test/client-attention.test.js.
//
// Feed contract. v1: items with clientId, clientName, code, level, reason,
// firstMove, fingerprint. v2 adds owner ('ours' | 'client') on every item,
// counts.adminOnly (clients with open items that never reach Slack), and
// fleet items (clientId 'fleet', several clients hit by the same signal on
// the same day, with members[]). Both are accepted so the dash and Max can
// deploy in either order; v1 simply renders without owner tags or drift.

const CONTRACT_VERSIONS = new Set([1, 2]);
const MAX_CLIENT_CARDS = 10;
const MAX_ALERTS_PER_RUN = 5;
const MAX_METADATA_MEMBERS = 25;
const FEED_MAX_AGE_MS = 15 * 60 * 1000;
const FLEET_ID = 'fleet';
const OWNERS = new Set(['ours', 'client']);

const CARD_LINE = /^(🔴|🟠) \*.+\*$/;
const ITEM_LINE = /^• (\[(ours|client)\] )?.+ → .+$/;
const FLEET_LINE = /^(🔴|🟠) (\[(ours|client)\] )?Fleet \((\d+) clients\): .+ → .+$/;
const OWNER_TAG = /\[(ours|client)\] /;
const MORE_LINE = /^…and (\d+) more clients? \((\d+) items?\) on the admin page\.$/;
const DRIFT_LINE = /^🟡 (\d+) clients? off benchmark, review on the admin page\.$/;
const IN_BAND_LINE = /^✅ (\d+) clients? in band\.$/;
const ALERT_LINE = /^🔴 \*Client attention\* · (\[(ours|client)\] )?.+: .+ → .+ <https:\/\/[^|>]+\|Open>\nReact ✅ here when it is handled, 💤 to snooze 3 days, or mark it in Campaign health\.$/;
// One line the team reads under every report and digest: what the tags mean
// and the two ways to close an item.
const HOW_TO_ACT = '[ours] = our team fixes it · [client] = ask the client. When an item is handled, react ✅ on its alert (💤 snoozes 3 days), or mark it in';
const BANNED = ['undefined', 'null', 'NaN', '[object Object]'];
const DIGEST_LINE = /^(🔴|🟠) (\[(ours|client)\] )?.+: .+ → (open \d+ days? \(alerted [A-Z][a-z]{2} \d{1,2}\)|in the report, never alerted)$/;

const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
const isFleet = (item) => item.clientId === FLEET_ID;
const dot = (item) => (item.level === 'urgent' ? '🔴' : '🟠');

/** Part of a client name before " - " or " | ", for compact lists. */
function shortName(name) {
  return String(name || '').split(/ - | \| /)[0].trim() || String(name || '').trim();
}

/** Is the feed usable? Fails closed on anything unexpected. */
function checkFeed(feed, now) {
  if (!feed || typeof feed !== 'object') return { ok: false, problem: 'feed is not an object' };
  if (!CONTRACT_VERSIONS.has(feed.contractVersion)) {
    return { ok: false, problem: `contract version ${feed.contractVersion} (expected ${[...CONTRACT_VERSIONS].join(' or ')})` };
  }
  const v2 = feed.contractVersion >= 2;
  const generated = Date.parse(feed.generatedAt);
  if (!Number.isFinite(generated)) return { ok: false, problem: 'generatedAt missing' };
  if (now.getTime() - generated > FEED_MAX_AGE_MS) return { ok: false, problem: `feed is stale (generated ${feed.generatedAt})` };
  if (!Array.isArray(feed.items)) return { ok: false, problem: 'items missing' };
  if (typeof feed.inBand !== 'number' || !feed.adminUrl) return { ok: false, problem: 'inBand or adminUrl missing' };
  if (v2 && !(feed.counts && Number.isFinite(feed.counts.adminOnly))) return { ok: false, problem: 'counts.adminOnly missing' };
  for (const i of feed.items) {
    if (!i || !i.clientId || !i.clientName || !i.code || !i.reason || !i.firstMove || !i.fingerprint) {
      return { ok: false, problem: `incomplete item ${JSON.stringify(i).slice(0, 120)}` };
    }
    if (i.level !== 'urgent' && i.level !== 'watch') return { ok: false, problem: `unknown level ${i.level}` };
    if (/[\n\r]/.test(`${i.clientName}${i.reason}${i.firstMove}`)) return { ok: false, problem: `line break in item for ${i.clientName}` };
    if (v2 && !OWNERS.has(i.owner)) return { ok: false, problem: `owner "${i.owner}" on ${i.clientName} (${i.code})` };
    if (isFleet(i)) {
      if (!v2) return { ok: false, problem: 'fleet item in a v1 feed' };
      if (!Array.isArray(i.members) || i.members.length === 0 || i.members.some((m) => !m || !m.clientId || !m.fingerprint)) {
        return { ok: false, problem: `fleet item for ${i.code} has no members` };
      }
    }
  }
  return { ok: true };
}

/** "[ours] " or "[client] " on v2 items; nothing on v1. */
function ownerTag(item) {
  return OWNERS.has(item.owner) ? `[${item.owner}] ` : '';
}

/**
 * One card per client: items grouped in feed order, a card is urgent when any
 * of its items is, urgent cards first (feed order kept inside each level).
 */
function groupByClient(items) {
  const groups = new Map();
  for (const item of items) {
    if (isFleet(item)) continue;
    if (!groups.has(item.clientId)) groups.set(item.clientId, { clientId: item.clientId, clientName: item.clientName.trim(), level: 'watch', items: [] });
    const g = groups.get(item.clientId);
    g.items.push(item);
    if (item.level === 'urgent') g.level = 'urgent';
  }
  const all = [...groups.values()];
  return [...all.filter((g) => g.level === 'urgent'), ...all.filter((g) => g.level !== 'urgent')];
}

function fleetLine(item) {
  return `${dot(item)} ${ownerTag(item)}Fleet (${Number(item.count) || (item.members || []).length} clients): ${item.reason} → ${item.firstMove}`;
}

function crDateLabel(now) {
  return new Intl.DateTimeFormat('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short', month: 'short', day: 'numeric' }).format(now);
}

/**
 * The Mon + Thu report. Fleet items first, then one card per client (urgent
 * cards first) up to MAX_CLIENT_CARDS, then "…and N more clients", one line
 * for benchmark drift (v2), the in-band count and the admin link. The
 * @fulfillment mention, when configured, appears once in the header.
 */
function formatReport(feed, now, { mention = '' } = {}) {
  const v2 = feed.contractVersion >= 2;
  const lines = [`*Client attention · ${crDateLabel(now)}*${mention ? ` ${mention}` : ''}`];
  const fleet = feed.items.filter(isFleet);
  const groups = groupByClient(feed.items);

  if (feed.items.length === 0) lines.push('Nothing needs attention today.');

  if (fleet.length > 0) {
    lines.push('*Fleet*');
    for (const item of fleet) lines.push(fleetLine(item));
  }
  const shown = groups.slice(0, MAX_CLIENT_CARDS);
  const hidden = groups.slice(MAX_CLIENT_CARDS);
  if (shown.length > 0) {
    lines.push('*Clients*');
    for (const g of shown) {
      lines.push(`${g.level === 'urgent' ? '🔴' : '🟠'} *${g.clientName}*`);
      for (const item of g.items) lines.push(`• ${ownerTag(item)}${item.reason} → ${item.firstMove}`);
    }
  }
  if (hidden.length > 0) {
    const hiddenItems = hidden.reduce((n, g) => n + g.items.length, 0);
    lines.push(`…and ${plural(hidden.length, 'more client', 'more clients')} (${plural(hiddenItems, 'item', 'items')}) on the admin page.`);
  }
  if (v2 && feed.counts.adminOnly > 0) {
    lines.push(`🟡 ${plural(feed.counts.adminOnly, 'client', 'clients')} off benchmark, review on the admin page.`);
  }
  lines.push(`✅ ${plural(feed.inBand, 'client', 'clients')} in band.`);
  lines.push(`${HOW_TO_ACT} <${feed.adminUrl}|Campaign health>.`);
  return lines.join('\n');
}

/**
 * Structural and numeric criteria (recipe §4a/§4b). Every feed item must be
 * accounted for (an item line, a fleet line, or the "…and N more" items
 * count); every client must have a card or be in the "more clients" count;
 * v2 lines carry an owner tag; drift and in-band numbers equal the feed's;
 * the mention appears exactly once when configured and never otherwise.
 */
function validateReport(text, feed, { mention = '' } = {}) {
  const v2 = feed.contractVersion >= 2;
  const problems = [];
  for (const token of BANNED) if (text.includes(token)) problems.push(`contains "${token}"`);
  if (!text.includes(feed.adminUrl)) problems.push('admin link missing');

  let items = 0;
  let cards = 0;
  let moreClients = 0;
  let drift = 0;
  let inBand = null;
  let untagged = 0;
  for (const line of text.split('\n')) {
    const more = MORE_LINE.exec(line);
    const band = IN_BAND_LINE.exec(line);
    const driftM = DRIFT_LINE.exec(line);
    const fleet = FLEET_LINE.exec(line);
    if (fleet) { items += 1; if (v2 && !OWNER_TAG.test(line)) untagged += 1; }
    else if (CARD_LINE.test(line)) cards += 1;
    else if (ITEM_LINE.test(line)) { items += 1; if (v2 && !OWNER_TAG.test(line)) untagged += 1; }
    else if (more) { moreClients += Number(more[1]); items += Number(more[2]); }
    else if (driftM) drift = Number(driftM[1]);
    else if (band) inBand = Number(band[1]);
  }
  const clients = new Set(feed.items.filter((i) => !isFleet(i)).map((i) => i.clientId)).size;
  if (items !== feed.items.length) problems.push(`report accounts for ${items} items, feed has ${feed.items.length}`);
  if (cards + moreClients !== clients) problems.push(`report accounts for ${cards + moreClients} clients, feed has ${clients}`);
  if (untagged > 0) problems.push(`${untagged} item line(s) without an owner tag`);
  if (v2 && drift !== feed.counts.adminOnly) problems.push(`drift line says ${drift}, feed has ${feed.counts.adminOnly}`);
  if (inBand !== feed.inBand) problems.push(`in-band line says ${inBand}, feed has ${feed.inBand}`);
  if (mention) {
    const n = text.split(mention).length - 1;
    if (n !== 1) problems.push(`mention appears ${n} times`);
  }
  return { ok: problems.length === 0, problems };
}

/** Dedupe key for an urgent alert: the same evidence alerts once. */
function alertKey(item) {
  return `attention:${item.clientId}:${item.code}:${item.fingerprint}`;
}

/** The keys a fleet alert also covers: each member's own item. */
function memberKeys(item) {
  if (!isFleet(item)) return [];
  return (item.members || []).map((m) => `attention:${m.clientId}:${item.code}:${m.fingerprint}`);
}

function formatAlert(item, adminUrl) {
  const who = isFleet(item) ? `Fleet (${Number(item.count) || (item.members || []).length} clients)` : item.clientName.trim();
  return `🔴 *Client attention* · ${ownerTag(item)}${who}: ${item.reason} → ${item.firstMove} <${adminUrl}|Open>\nReact ✅ here when it is handled, 💤 to snooze 3 days, or mark it in Campaign health.`;
}

function validateAlert(text) {
  const problems = [];
  for (const token of BANNED) if (text.includes(token)) problems.push(`contains "${token}"`);
  if (!ALERT_LINE.test(text)) problems.push('alert line does not match the expected shape');
  return { ok: problems.length === 0, problems };
}

/**
 * Urgent items not alerted yet, at most MAX_ALERTS_PER_RUN, plus how many were
 * held back. A fleet item is new while its own key is unseen and at least one
 * member was never alerted on its own; a per-client item is covered once its
 * fleet alerted (the fleet alert marks every member key).
 */
function planAlerts(feed, alreadyAlerted) {
  const fresh = feed.items.filter((i) => {
    if (i.level !== 'urgent' || alreadyAlerted.has(alertKey(i))) return false;
    const members = memberKeys(i);
    return members.length === 0 || members.some((k) => !alreadyAlerted.has(k));
  });
  return { send: fresh.slice(0, MAX_ALERTS_PER_RUN), overflow: Math.max(fresh.length - MAX_ALERTS_PER_RUN, 0) };
}

function formatAlertOverflow(n, adminUrl) {
  return `🔴 *Client attention* · ${plural(n, 'more urgent item', 'more urgent items')} this run: <${adminUrl}|see the admin page>`;
}

/**
 * Slack message metadata on every alert, so a ✅ on it can be traced back to
 * the item without any lookup table (the reaction handler reads the message
 * back). Values are strings; a fleet's members are capped so the payload
 * stays small, the agent_knowledge record keeps the full list.
 */
function alertMetadata(item) {
  const payload = {
    client_id: String(item.clientId),
    client_name: String(item.clientName || '').trim().slice(0, 200),
    code: String(item.code),
    fingerprint: String(item.fingerprint),
  };
  if (isFleet(item)) {
    payload.members = JSON.stringify((item.members || []).slice(0, MAX_METADATA_MEMBERS).map((m) => [m.clientId, m.fingerprint]));
  }
  return { event_type: 'client_attention_alert', event_payload: payload };
}

/** What agent_knowledge keeps per alert key: enough for the digest and the scorecard. */
function alertRecord(item, posted, now = new Date()) {
  return JSON.stringify({
    clientName: String(item.clientName || '').trim(),
    code: item.code,
    fingerprint: item.fingerprint,
    channel: (posted && posted.channel) || null,
    ts: (posted && posted.ts) || null,
    postedAt: now.toISOString(),
    members: isFleet(item) ? (item.members || []).map((m) => [m.clientId, m.fingerprint]) : undefined,
  });
}

/** Reads a record; tolerates the v1 value "Client name · code" (no post time). */
function parseAlertRecord(value) {
  const s = String(value || '');
  if (s.startsWith('{')) {
    try {
      const parsed = JSON.parse(s);
      if (parsed && typeof parsed === 'object') return { postedAt: null, ...parsed };
    } catch { /* fall through to the legacy shape */ }
  }
  const sep = s.lastIndexOf(' · ');
  return { clientName: sep >= 0 ? s.slice(0, sep) : s, code: sep >= 0 ? s.slice(sep + 3) : null, fingerprint: null, channel: null, ts: null, postedAt: null };
}

/**
 * What a ✅ (or 💤) on an alert marks: the item itself, or every member of a
 * fleet alert. Reads the Slack message metadata payload Max posted
 * (alertMetadata), so nothing has to be looked up.
 */
function handledTargets(payload) {
  if (!payload || !payload.code || !payload.client_id) return [];
  if (payload.client_id === FLEET_ID) {
    let members = [];
    try { members = JSON.parse(payload.members || '[]'); } catch { members = []; }
    return members
      .filter((m) => Array.isArray(m) && m[0] && m[1])
      .map(([clientId, fingerprint]) => ({ customerId: String(clientId), signalCode: String(payload.code), fingerprint: String(fingerprint) }));
  }
  if (!payload.fingerprint) return [];
  return [{ customerId: String(payload.client_id), signalCode: String(payload.code), fingerprint: String(payload.fingerprint) }];
}

/** The thread reply after a reaction was recorded. */
function handledReply(actorMention, action, n) {
  const what = action === 'snoozed' ? 'Snoozed 3 days' : 'Marked handled';
  const scope = n > 1 ? ` for ${n} clients` : '';
  const back = action === 'snoozed' ? 'It comes back after that.' : 'It comes back if the evidence changes.';
  return `${what} by ${actorMention}${scope}. ${back}`;
}

function crShortDate(iso) {
  return new Intl.DateTimeFormat('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric' }).format(new Date(iso));
}

/**
 * The Friday "still open" digest: every urgent item in the feed with how long
 * it has been open (from the alert record's post time). Items that were never
 * alerted (they were already open when alerts were armed) say so.
 */
function formatOpenDigest(feed, recordsByKey, now, { mention = '' } = {}) {
  const urgent = feed.items.filter((i) => i.level === 'urgent');
  const lines = [`*Client attention · still open · ${crDateLabel(now)}*${mention ? ` ${mention}` : ''}`];
  if (urgent.length === 0) {
    lines.push('No urgent item is open. Good week.');
    return lines.join('\n');
  }
  for (const item of urgent) {
    const rec = recordsByKey.get(alertKey(item));
    const postedAt = rec && rec.postedAt ? Date.parse(rec.postedAt) : NaN;
    const age = Number.isFinite(postedAt)
      ? `open ${plural(Math.max(1, Math.round((now.getTime() - postedAt) / 864e5)), 'day', 'days')} (alerted ${crShortDate(rec.postedAt)})`
      : 'in the report, never alerted';
    const who = isFleet(item) ? `Fleet (${Number(item.count) || (item.members || []).length} clients)` : item.clientName.trim();
    lines.push(`${dot(item)} ${ownerTag(item)}${who}: ${item.reason} → ${age}`);
  }
  lines.push(`${HOW_TO_ACT} <${feed.adminUrl}|Campaign health>.`);
  return lines.join('\n');
}

function validateOpenDigest(text, feed, { mention = '' } = {}) {
  const problems = [];
  for (const token of BANNED) if (text.includes(token)) problems.push(`contains "${token}"`);
  const urgent = feed.items.filter((i) => i.level === 'urgent').length;
  const lines = text.split('\n').filter((l) => DIGEST_LINE.test(l)).length;
  if (urgent === 0 && !text.includes('No urgent item is open.')) problems.push('empty digest without the all-clear line');
  if (urgent > 0 && lines !== urgent) problems.push(`digest has ${lines} item lines, feed has ${urgent} urgent items`);
  if (urgent > 0 && !text.includes(feed.adminUrl)) problems.push('admin link missing');
  if (mention) {
    const n = text.split(mention).length - 1;
    if (n !== 1) problems.push(`mention appears ${n} times`);
  }
  return { ok: problems.length === 0, problems };
}

/**
 * Monday scorecard, per signal: alerts sent in the window (agent_knowledge
 * alert keys), how many of those a human handled or snoozed (the dash's
 * actions table, matched by client + code + fingerprint), how many came from
 * Slack, and how many are still open in today's feed. Pure.
 *   sent: [{ key, record }] from agent_knowledge (key 'attention:<client>:<code>:<fp>')
 *   actions: [{ customerId, signalCode, fingerprint, action, actorUserId }] from the dash
 *   feed: today's feed or null
 */
function buildScorecard({ sent, actions, feed }) {
  const byCode = new Map();
  const row = (code) => {
    if (!byCode.has(code)) byCode.set(code, { code, sent: 0, handled: 0, handledSlack: 0, snoozed: 0, open: 0 });
    return byCode.get(code);
  };
  const sentKeys = new Set();
  for (const { key } of sent) {
    const parts = String(key).split(':');
    if (parts.length !== 4 || parts[0] !== 'attention' || parts[1] === 'alerts-armed') continue;
    sentKeys.add(key);
    row(parts[2]).sent += 1;
  }
  const seen = new Set();
  for (const a of actions) {
    const key = `attention:${a.customerId}:${a.signalCode}:${a.fingerprint}`;
    if (!sentKeys.has(key) || seen.has(`${key}|${a.action}`)) continue;
    seen.add(`${key}|${a.action}`);
    const r = row(a.signalCode);
    if (a.action === 'handled') {
      r.handled += 1;
      if (String(a.actorUserId || '').startsWith('slack:')) r.handledSlack += 1;
    } else if (a.action === 'snoozed') r.snoozed += 1;
  }
  if (feed) {
    for (const item of feed.items) {
      if (item.level !== 'urgent' || !sentKeys.has(alertKey(item))) continue;
      row(item.code).open += 1;
    }
  }
  return [...byCode.values()].sort((a, b) => b.sent - a.sent || a.code.localeCompare(b.code));
}

function formatScorecard(rows, { days = 28, feedKnown = true } = {}) {
  const totals = rows.reduce((t, r) => ({ sent: t.sent + r.sent, handled: t.handled + r.handled, handledSlack: t.handledSlack + r.handledSlack, snoozed: t.snoozed + r.snoozed, open: t.open + r.open }), { sent: 0, handled: 0, handledSlack: 0, snoozed: 0, open: 0 });
  const lines = [`*Client attention scorecard · last ${days} days*`];
  if (rows.length === 0) {
    lines.push('No alerts were sent in this window.');
    return lines.join('\n');
  }
  for (const r of rows) {
    const slack = r.handledSlack > 0 ? ` (${r.handledSlack} from Slack)` : '';
    lines.push(`• ${r.code}: ${plural(r.sent, 'alert', 'alerts')} · ${r.handled} handled${slack} · ${r.snoozed} snoozed · ${feedKnown ? `${r.open} still open` : 'open unknown'}`);
  }
  lines.push(`Total: ${plural(totals.sent, 'alert', 'alerts')} · ${totals.handled} handled · ${totals.snoozed} snoozed${feedKnown ? ` · ${totals.open} still open` : ''}`);
  if (totals.sent > 0 && totals.handled + totals.snoozed === 0) {
    lines.push('⚠️ Nothing was handled or snoozed in this window. Either the alerts are noise or nobody is on them: decide which signals to keep.');
  } else {
    const ignored = rows.filter((r) => r.sent >= 3 && r.handled + r.snoozed === 0).map((r) => r.code);
    if (ignored.length > 0) lines.push(`⚠️ Never acted on: ${ignored.join(', ')}. Candidates to retire or demote.`);
  }
  return lines.join('\n');
}

module.exports = {
  CONTRACT_VERSIONS,
  MAX_CLIENT_CARDS,
  MAX_ALERTS_PER_RUN,
  MAX_METADATA_MEMBERS,
  FLEET_ID,
  checkFeed,
  ownerTag,
  groupByClient,
  formatReport,
  validateReport,
  alertKey,
  memberKeys,
  formatAlert,
  validateAlert,
  planAlerts,
  formatAlertOverflow,
  alertMetadata,
  alertRecord,
  parseAlertRecord,
  handledTargets,
  handledReply,
  formatOpenDigest,
  validateOpenDigest,
  buildScorecard,
  formatScorecard,
  shortName,
};
