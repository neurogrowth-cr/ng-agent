'use strict';
// Weekly performance report status: the Monday verdict line for
// #ng-fullfillment-ops (Ron, 2026-09-25). Recipe:
// ~/automations/ops/recipes/weekly-performance-report.md.
//
// The dash sends the Monday client email and computes the status
// (GET /api/ops/weekly-report-status: eligible recipients against the sends
// table). Max only checks the feed, formats one message and posts it. Pure
// (no Slack, no network), tested in test/weekly-report-status.test.js.

const CONTRACT_VERSION = 1;
const FEED_MAX_AGE_MS = 15 * 60 * 1000;
const MAX_DETAIL_LINES = 6;
const BANNED = ['undefined', 'null', 'NaN', '[object Object]'];

const LINE = /^(✅|⚠️) WPR \d{4}-W\d{2} \((live|dry_run)\): .+$/;

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
  if (!/^\d{4}-W\d{2}$/.test(String(feed.isoWeek || ''))) return { ok: false, problem: 'isoWeek missing' };
  if (feed.mode === 'off') return { ok: true };
  if (feed.mode !== 'live' && feed.mode !== 'dry_run') return { ok: false, problem: `unknown mode ${feed.mode}` };
  if (feed.verdict !== 'green' && feed.verdict !== 'red') return { ok: false, problem: `unknown verdict ${feed.verdict}` };
  if (typeof feed.line !== 'string' || !LINE.test(feed.line)) return { ok: false, problem: `verdict line malformed: ${String(feed.line).slice(0, 80)}` };
  if ((feed.verdict === 'green') !== feed.line.startsWith('✅')) return { ok: false, problem: 'verdict and line disagree' };
  for (const key of ['eligibleRecipients', 'sent', 'dryRun', 'retryQueued', 'inFlight']) {
    if (!Number.isFinite(feed[key])) return { ok: false, problem: `${key} missing` };
  }
  if (!Array.isArray(feed.missing) || !Array.isArray(feed.failures)) return { ok: false, problem: 'missing or failures not a list' };
  for (const m of feed.missing) {
    if (!m || !m.clientName || !m.recipient) return { ok: false, problem: `incomplete missing entry ${JSON.stringify(m).slice(0, 80)}` };
  }
  for (const f of feed.failures) {
    if (!f || !f.clientName || !f.recipient || !f.outcome) return { ok: false, problem: `incomplete failure entry ${JSON.stringify(f).slice(0, 80)}` };
  }
  return { ok: true };
}

/**
 * The Monday post. Null when the report is off (nothing to say). Green is the
 * verdict line alone. Red adds up to MAX_DETAIL_LINES of who is missing or
 * failed, then the count of the rest.
 */
function formatPost(feed) {
  if (feed.mode === 'off') return null;
  const lines = [feed.line];
  if (feed.verdict === 'red') {
    const details = [
      ...feed.missing.map((m) => `• missing: ${shortName(m.clientName)} <${m.recipient}>`),
      ...feed.failures.map((f) => `• ${f.outcome}${f.attempt ? ` (attempt ${f.attempt})` : ''}: ${shortName(f.clientName)} <${f.recipient}>${f.error ? ` · ${String(f.error).slice(0, 80)}` : ''}`),
    ];
    lines.push(...details.slice(0, MAX_DETAIL_LINES));
    if (details.length > MAX_DETAIL_LINES) lines.push(`…and ${details.length - MAX_DETAIL_LINES} more in the sends table.`);
    if (feed.lastRunAt === null) lines.push('The cron never claimed anything this week: check vercel.json and the Vercel cron logs.');
  }
  return lines.join('\n');
}

/** Criteria on the text, before it is posted (recipe §4a). */
function validatePost(text) {
  const problems = [];
  const lines = String(text || '').split('\n');
  if (!LINE.test(lines[0] || '')) problems.push('first line is not a verdict line');
  if (lines.length > 2 + MAX_DETAIL_LINES + 1) problems.push(`too many lines (${lines.length})`);
  for (const bad of BANNED) if (text.includes(bad)) problems.push(`contains "${bad}"`);
  if (/\{\w+\}/.test(text)) problems.push('contains an unreplaced placeholder');
  return { ok: problems.length === 0, problems };
}

module.exports = { CONTRACT_VERSION, FEED_MAX_AGE_MS, checkFeed, formatPost, validatePost, shortName };
