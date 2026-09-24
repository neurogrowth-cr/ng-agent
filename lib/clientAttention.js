'use strict';
// Client attention: the Mon + Thu report and urgent alerts for the fulfillment
// team in #ng-fullfillment-ops (Ron, 2026-09-23). Recipe:
// ~/automations/ops/recipes/client-attention.md.
//
// The list is computed in the dash (GET /api/ops/client-attention, the same
// engine as Admin -> Campaign health); Max only formats, checks and posts it.
// Everything here is pure (no Slack, no network) and tested in
// test/client-attention.test.js.

const CONTRACT_VERSION = 1;
const MAX_ITEM_LINES = 15;
const MAX_ALERTS_PER_RUN = 5;
const FEED_MAX_AGE_MS = 15 * 60 * 1000;

const ITEM_LINE = /^(🔴|🟠) .+: .+ → .+$/;
const POSITIVE_LINE = /^🔴 Positive replies waiting over 24h: (\d+) clients?, (\d+) repl(?:y|ies)\. Most: .+ → .+$/;
const MORE_LINE = /^…and (\d+) more on the admin page\.$/;
const IN_BAND_LINE = /^✅ (\d+) clients? in band\.$/;
const ALERT_LINE = /^🔴 \*Client attention\* · .+: .+ → .+ <https:\/\/[^|>]+\|Open>$/;
const BANNED = ['undefined', 'null', 'NaN', '[object Object]'];

const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;

/** Part of a client name before " - " or " | ", for compact lists. */
function shortName(name) {
  return String(name || '').split(/ - | \| /)[0].trim() || String(name || '').trim();
}

/** Is the feed usable? Fails closed on anything unexpected. */
function checkFeed(feed, now) {
  if (!feed || typeof feed !== 'object') return { ok: false, problem: 'feed is not an object' };
  if (feed.contractVersion !== CONTRACT_VERSION) {
    return { ok: false, problem: `contract version ${feed.contractVersion} (expected ${CONTRACT_VERSION})` };
  }
  const generated = Date.parse(feed.generatedAt);
  if (!Number.isFinite(generated)) return { ok: false, problem: 'generatedAt missing' };
  if (now.getTime() - generated > FEED_MAX_AGE_MS) return { ok: false, problem: `feed is stale (generated ${feed.generatedAt})` };
  if (!Array.isArray(feed.items)) return { ok: false, problem: 'items missing' };
  if (typeof feed.inBand !== 'number' || !feed.adminUrl) return { ok: false, problem: 'inBand or adminUrl missing' };
  for (const i of feed.items) {
    if (!i || !i.clientId || !i.clientName || !i.code || !i.reason || !i.firstMove || !i.fingerprint) {
      return { ok: false, problem: `incomplete item ${JSON.stringify(i).slice(0, 120)}` };
    }
    if (i.level !== 'urgent' && i.level !== 'watch') return { ok: false, problem: `unknown level ${i.level}` };
    if (/[\n\r]/.test(`${i.clientName}${i.reason}${i.firstMove}`)) return { ok: false, problem: `line break in item for ${i.clientName}` };
  }
  return { ok: true };
}

function itemLine(item) {
  return `${item.level === 'urgent' ? '🔴' : '🟠'} ${item.clientName.trim()}: ${item.reason} → ${item.firstMove}`;
}

function crDateLabel(now) {
  return new Intl.DateTimeFormat('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short', month: 'short', day: 'numeric' }).format(now);
}

/**
 * The Mon + Thu report. Positive replies waiting fold into one line (they are
 * the long tail and would bury everything else); every other item gets its own
 * line, urgent before watch, up to MAX_ITEM_LINES, then "…and N more".
 */
function formatReport(feed, now) {
  const lines = [`*Client attention · ${crDateLabel(now)}*`];
  const positives = feed.items.filter((i) => i.code === 'positive_waiting');
  const others = feed.items.filter((i) => i.code !== 'positive_waiting');
  const urgentOthers = others.filter((i) => i.level === 'urgent');
  const watch = others.filter((i) => i.level === 'watch');

  if (feed.items.length === 0) lines.push('Nothing needs attention today.');

  let budget = MAX_ITEM_LINES;
  let shown = 0;
  if (positives.length > 0 || urgentOthers.length > 0) {
    lines.push('*Urgent*');
    if (positives.length > 0) {
      const total = positives.reduce((n, i) => n + (Number(i.count) || 0), 0);
      const top = positives
        .slice(0, 3)
        .map((i) => `${shortName(i.clientName)} (${Number(i.count) || 0})`)
        .join(', ');
      lines.push(
        `🔴 Positive replies waiting over 24h: ${plural(positives.length, 'client', 'clients')}, ${plural(total, 'reply', 'replies')}. Most: ${top} → Answer them in LinkedIn today, then mark each handled.`
      );
      budget -= 1;
    }
    for (const item of urgentOthers) {
      if (budget <= 0) break;
      lines.push(itemLine(item));
      budget -= 1;
      shown += 1;
    }
  }
  const watchShown = watch.slice(0, Math.max(budget, 0));
  if (watchShown.length > 0) {
    lines.push('*Watch*');
    for (const item of watchShown) lines.push(itemLine(item));
    shown += watchShown.length;
  }
  const more = others.length - shown;
  if (more > 0) lines.push(`…and ${more} more on the admin page.`);
  lines.push(`✅ ${plural(feed.inBand, 'client', 'clients')} in band.`);
  lines.push(`Mark items handled or snoozed: <${feed.adminUrl}|Campaign health>`);
  return lines.join('\n');
}

/**
 * Structural and numeric criteria (recipe §4a/§4b). Every open item in the feed
 * must be accounted for: one line each, the positive-replies line counting its
 * clients, plus "…and N more".
 */
function validateReport(text, feed) {
  const problems = [];
  for (const token of BANNED) if (text.includes(token)) problems.push(`contains "${token}"`);
  if (!text.includes(feed.adminUrl)) problems.push('admin link missing');

  let counted = 0;
  let inBand = null;
  for (const line of text.split('\n')) {
    const positive = POSITIVE_LINE.exec(line);
    const more = MORE_LINE.exec(line);
    const band = IN_BAND_LINE.exec(line);
    if (positive) counted += Number(positive[1]);
    else if (ITEM_LINE.test(line)) counted += 1;
    else if (more) counted += Number(more[1]);
    else if (band) inBand = Number(band[1]);
  }
  if (counted !== feed.items.length) problems.push(`report accounts for ${counted} items, feed has ${feed.items.length}`);
  if (inBand !== feed.inBand) problems.push(`in-band line says ${inBand}, feed has ${feed.inBand}`);
  return { ok: problems.length === 0, problems };
}

/** Dedupe key for an urgent alert: the same evidence alerts once. */
function alertKey(item) {
  return `attention:${item.clientId}:${item.code}:${item.fingerprint}`;
}

function formatAlert(item, adminUrl) {
  return `🔴 *Client attention* · ${item.clientName.trim()}: ${item.reason} → ${item.firstMove} <${adminUrl}|Open>`;
}

function validateAlert(text) {
  const problems = [];
  for (const token of BANNED) if (text.includes(token)) problems.push(`contains "${token}"`);
  if (!ALERT_LINE.test(text)) problems.push('alert line does not match the expected shape');
  return { ok: problems.length === 0, problems };
}

/** Urgent items not alerted yet, at most MAX_ALERTS_PER_RUN, plus how many were held back. */
function planAlerts(feed, alreadyAlerted) {
  const fresh = feed.items.filter((i) => i.level === 'urgent' && !alreadyAlerted.has(alertKey(i)));
  return { send: fresh.slice(0, MAX_ALERTS_PER_RUN), overflow: Math.max(fresh.length - MAX_ALERTS_PER_RUN, 0) };
}

function formatAlertOverflow(n, adminUrl) {
  return `🔴 *Client attention* · ${plural(n, 'more urgent item', 'more urgent items')} this run: <${adminUrl}|see the admin page>`;
}

module.exports = {
  CONTRACT_VERSION,
  MAX_ITEM_LINES,
  MAX_ALERTS_PER_RUN,
  checkFeed,
  formatReport,
  validateReport,
  alertKey,
  formatAlert,
  validateAlert,
  planAlerts,
  formatAlertOverflow,
  shortName,
};
