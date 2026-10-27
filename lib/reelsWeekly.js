'use strict';
// Reels Weekly: the Monday 08:00 CR summary of last week's Instagram reels for
// #ng-content (Ron, 2026-10-07). Recipe: ~/automations/ops/recipes/reels-weekly.md.
//
// Sources are the Instagram Graph API /media call (the reel list with per-reel
// reach, views, saves and shares) and the GHL Social Planner statistics (the
// account's impressions, reach and new followers). This module is pure (no
// Slack, no network, no Supabase): it picks the week, normalises the reels,
// builds the reel_stats rows, formats one message and validates it.
// Tested in test/reels-weekly.test.js. index.js does the I/O.

const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // America/Costa_Rica has no DST
const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_REELS_LISTED = 8;
const HOOK_MAX = 70;
const { table, cell } = require('./slackTable');
const outlier = require('./reelsOutlier');
const BANNED = ['undefined', 'null', 'NaN', '[object Object]'];
const HEADER = /^\*📊 Reels · semana \d{4}-W\d{2} \(.+\)\*$/;
const COUNT_LINE = /^Reels publicados: \d+/m;
const RANKING_HEADER = /^\*Ranking por (.+)\*$/;

/** Mon 00:00 CR to Sun 24:00 CR of the ISO week BEFORE `now`, as UTC instants. */
function weekWindow(now) {
  const cr = new Date(now.getTime() - CR_OFFSET_MS);          // wall clock in CR, read as UTC fields
  const dow = (cr.getUTCDay() + 6) % 7;                        // Mon=0 … Sun=6
  const thisMonCr = Date.UTC(cr.getUTCFullYear(), cr.getUTCMonth(), cr.getUTCDate()) - dow * DAY_MS;
  const startCr = thisMonCr - 7 * DAY_MS;
  const start = new Date(startCr + CR_OFFSET_MS);
  const end = new Date(thisMonCr + CR_OFFSET_MS);              // exclusive
  return { start, end, isoWeek: isoWeekLabel(new Date(startCr)), label: rangeLabel(new Date(startCr), new Date(thisMonCr - DAY_MS)) };
}

function isoWeekLabel(d) {
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const yearStart = Date.UTC(t.getUTCFullYear(), 0, 1);
  const week = Math.ceil(((t - yearStart) / DAY_MS + 1) / 7);
  return `${t.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}

const MONTHS = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];
function rangeLabel(mon, sun) {
  const a = `${mon.getUTCDate()}`;
  const b = `${sun.getUTCDate()} ${MONTHS[sun.getUTCMonth()]}`;
  return mon.getUTCMonth() === sun.getUTCMonth() ? `${a} a ${b}` : `${a} ${MONTHS[mon.getUTCMonth()]} a ${b}`;
}

/** First sentence of the caption, as the reel's name in the ranking. */
function hookOf(summary) {
  const first = String(summary || '').split('\n').map((s) => s.trim()).find(Boolean) || '(sin caption)';
  const sentence = first.split(/(?<=[.?!])\s/)[0];
  return sentence.length > HOOK_MAX ? `${sentence.slice(0, HOOK_MAX - 1).trimEnd()}…` : sentence;
}

const num = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);

/**
 * Meta /{ig-user}/media rows (fields IG_REEL_FIELDS plus the expansion
 * insights.metric(reach,saved,shares,views), see index.js fetchIgReels) inside
 * the window, reduced to what the report needs. REELS only. Missing insights
 * stay null and print as s/d, never 0. Since 2026-10-26 this is the reel list:
 * GHL's published posts only held reels scheduled through GHL.
 */
function mediaToPosts(items, window) {
  const out = [];
  for (const m of Array.isArray(items) ? items : []) {
    if (!m || m.media_product_type !== 'REELS' || !m.id) continue;
    const at = Date.parse(m.timestamp || '');
    if (!Number.isFinite(at) || at < window.start.getTime() || at >= window.end.getTime()) continue;
    const ins = {};
    for (const row of (m.insights && m.insights.data) || []) {
      const v = row && row.values && row.values[0] ? Number(row.values[0].value) : NaN;
      ins[row.name] = Number.isFinite(v) ? v : null;
    }
    out.push({
      postId: String(m.id), ghlPostId: null, type: 'reel',
      publishedAt: new Date(at).toISOString(), permalink: m.permalink || null, hook: hookOf(m.caption),
      likes: num(m.like_count), comments: num(m.comments_count),
      shares: ins.shares ?? null, reach: ins.reach ?? null, views: ins.views ?? null, saved: ins.saved ?? null,
    });
  }
  return out.sort((a, b) => a.publishedAt.localeCompare(b.publishedAt));
}

/**
 * The multiple is measured on views when Meta gave any (ig-audit's definition),
 * on reach otherwise. `history` = reel_stats rows of the trailing weeks (null
 * when unread); below the floor the median comes from this week's batch.
 */
function medianBase(posts, history) {
  const basis = posts.some((p) => Number.isFinite(p.views) && p.views > 0) ? 'views' : 'reach';
  return outlier.medianBase(basis, posts, history);
}

/** Posts with `multiple` and `sendsPer1k`, ranked: multiple, then sends per reach, then reach, then engagement. */
function rankReels(posts, history = null) {
  return outlier.rank(outlier.score(posts, medianBase(posts, history)));
}

/** Rows for reel_stats: one per reel plus one `account` row with the GHL totals. */
function buildRows(posts, account, window, capturedAt) {
  const rows = posts.map((p) => ({
    iso_week: window.isoWeek, post_id: p.postId, ghl_post_id: p.ghlPostId || null, kind: 'reel',
    hook: p.hook, permalink: p.permalink, published_at: p.publishedAt,
    likes: p.likes, comments: p.comments, shares: p.shares ?? 0, saved: p.saved, reach: p.reach, views: p.views,
    captured_at: capturedAt.toISOString(),
  }));
  if (account) {
    rows.push({
      iso_week: window.isoWeek, post_id: 'account', ghl_post_id: null, kind: 'account',
      hook: null, permalink: null, published_at: null,
      likes: num(account.likes), comments: num(account.comments), shares: num(account.shares), saved: null,
      reach: Number.isFinite(account.reach) ? account.reach : null, views: Number.isFinite(account.impressions) ? account.impressions : null,
      followers: Number.isFinite(account.followers) ? account.followers : null,
      captured_at: capturedAt.toISOString(),
    });
  }
  return rows;
}

/** Totals from the GHL statistics endpoint, flattened. Null fields when absent. */
function accountTotals(stats) {
  const r = stats && stats.results ? stats.results : stats;
  if (!r || typeof r !== 'object') return null;
  const t = r.totals || {};
  const eng = (((r.breakdowns || {}).engagement || {}).instagram) || {};
  const reach = (((r.breakdowns || {}).reach || {}).total);
  return {
    impressions: Number.isFinite(Number(t.impressions)) ? Number(t.impressions) : null,
    reach: Number.isFinite(Number(reach)) ? Number(reach) : null,
    likes: num(t.likes), comments: num(t.comments), shares: num(eng.shares),
    followers: Number.isFinite(Number(t.followers)) ? Number(t.followers) : null,
    posts: num(t.posts),
  };
}

/** '(▲ +3 WoW)' / '(▼ −2 WoW)' / '(= flat WoW)'; '' when either side is missing. */
function fmtDelta(cur, prev) {
  if (cur == null || prev == null) return '';
  const d = cur - prev;
  if (d === 0) return ' (= flat WoW)';
  return d > 0 ? ` (▲ +${Math.abs(d).toLocaleString('en-US')} WoW)` : ` (▼ −${Math.abs(d).toLocaleString('en-US')} WoW)`;
}
const fmt = (n) => (n == null ? '-' : Number(n).toLocaleString('en-US'));

/** Delta cell for the account table: '+1,132' / '-6' / '=' / '' when either side is missing. */
function deltaCell(cur, prev) {
  if (cur == null || prev == null) return '';
  const d = cur - prev;
  if (d === 0) return '=';
  return `${d > 0 ? '+' : '-'}${Math.abs(d).toLocaleString('en-US')}`;
}

/**
 * The Monday post (Ron, 2026-10-10: bold header, account numbers and the
 * ranking as aligned tables). One line under the header when nothing was
 * published. Links do not render inside the table, so the top reels get a
 * "Ver:" line under it. Since 2026-10-24 the ranking is by outlier multiple
 * (xMed) then sends per 1,000 reached (Env/1k), with reach and views kept as
 * columns, a footnote on the median, and a one-line verdict (lib/reelsOutlier.js).
 * `history` = reel_stats rows of the trailing weeks, or null when unread.
 */
function formatPost({ window, posts, account, prevAccount, metaWired, history = null }) {
  const lines = [`*📊 Reels · semana ${window.isoWeek} (${window.label})*`];
  if (!posts.length) {
    lines.push('Reels publicados: 0. Sin publicaciones esta semana, no hay nada que medir.');
    return lines.join('\n');
  }
  lines.push(`Reels publicados: ${posts.length}`);
  const a = account || {};
  const pa = prevAccount || {};
  const acct = [['Impresiones', a.impressions, pa.impressions], ['Alcance', a.reach, pa.reach], ['Seguidores nuevos', a.followers, pa.followers]]
    .filter(([, v]) => v != null);
  if (acct.length) {
    const withPrev = acct.some(([, v, pv]) => pv != null);
    lines.push(table(['', 'Semana', ...(withPrev ? ['vs anterior'] : [])],
      acct.map(([k, v, pv]) => [k, fmt(v), ...(withPrev ? [deltaCell(v, pv)] : [])]), ['l', 'r', 'r']));
  }
  const base = medianBase(posts, history);
  const ranked = outlier.rank(outlier.score(posts, base));
  const key = outlier.sortKey(ranked);
  const listed = ranked.slice(0, MAX_REELS_LISTED);
  const haveMultiple = ranked.some((p) => p.multiple != null);
  const haveSends = ranked.some((p) => p.sendsPer1k != null);
  const haveReach = ranked.some((p) => Number.isFinite(p.reach));
  const haveViews = ranked.some((p) => Number.isFinite(p.views));
  const haveSaved = ranked.some((p) => Number.isFinite(p.saved));
  const opt = (v) => (Number.isFinite(v) ? fmt(v) : 's/d');
  // Column order puts the sort key first after the reel name; validatePost
  // checks the header against that column, so the two cannot drift apart.
  lines.push('', `*Ranking por ${outlier.HEADER_FOR_KEY[key]}*`);
  lines.push(table(
    ['#', 'Reel', ...(haveMultiple ? ['xMed'] : []), ...(haveSends ? ['Env/1k'] : []), ...(haveReach ? ['Alcance'] : []), ...(haveViews ? ['Vistas'] : []),
      'Likes', 'Com', 'Comp', ...(haveSaved ? ['Guard'] : [])],
    listed.map((p, i) => [String(i + 1), cell(p.hook, 30), ...(haveMultiple ? [outlier.fmtMultiple(p.multiple)] : []), ...(haveSends ? [outlier.fmtPer1k(p.sendsPer1k)] : []),
      ...(haveReach ? [opt(p.reach)] : []), ...(haveViews ? [opt(p.views)] : []),
      fmt(p.likes), fmt(p.comments), opt(p.shares), ...(haveSaved ? [opt(p.saved)] : [])]),
    ['l', 'l']));
  const linked = listed.filter((p) => p.permalink);
  if (linked.length) lines.push(`Ver: ${listed.map((p, i) => (p.permalink ? `<${p.permalink}|${i + 1}>` : null)).filter(Boolean).join(' · ')}`);
  if (ranked.length > MAX_REELS_LISTED) lines.push(`…y ${ranked.length - MAX_REELS_LISTED} más en reel_stats.`);
  if (haveReach || haveViews) lines.push(outlier.footnote(base, 'esta semana'));
  // No verdict without per-reel reach: the Meta caveat below already says why.
  if (metaWired || haveReach) {
    const verdict = outlier.verdict({ reels: ranked, reach: a.reach ?? null, prevReach: pa.reach ?? null, newFollowers: a.followers ?? null });
    lines.push(outlier.formatVerdict(verdict, 'la semana anterior'));
  }
  if (!metaWired) lines.push('_Alcance y vistas por reel: Meta no entregó los insights esta semana (revisar META_IG_INSIGHTS_TOKEN y sus permisos)._');
  return lines.join('\n');
}

/**
 * The ranking header must name the key the table is sorted by: the column
 * right after "Reel" is the primary sort key by construction (formatPost), so a
 * header that names another key means the sort and the label drifted apart.
 */
function rankingProblems(lines) {
  const problems = [];
  const hi = lines.findIndex((l) => RANKING_HEADER.test(l));
  if (hi === -1) return ['missing ranking header'];
  const named = lines[hi].match(RANKING_HEADER)[1];
  const key = Object.keys(outlier.HEADER_FOR_KEY).find((k) => outlier.HEADER_FOR_KEY[k] === named);
  if (!key) return [`ranking header does not name a known sort key: "${named}"`];
  const th = lines.slice(hi + 1).find((l) => l.startsWith('#  Reel'));
  if (!th) return ['ranking table header missing'];
  const cols = th.trim().split(/\s{2,}/);
  const first = cols[cols.indexOf('Reel') + 1];
  if (first !== outlier.COLUMN_FOR_KEY[key]) problems.push(`ranking header says "${key}" but the table sorts by "${first || '(none)'}"`);
  return problems;
}

/** Criteria on the text, before it is posted (recipe §4a). */
function validatePost(text, { posts = [], account = null } = {}) {
  const problems = [];
  const lines = String(text || '').split('\n');
  if (!HEADER.test(lines[0] || '')) problems.push('first line is not the header');
  if (!COUNT_LINE.test(text)) problems.push('missing "Reels publicados: N"');
  for (const bad of BANNED) if (new RegExp(`\\b${bad.replace(/[[\]]/g, '\\$&')}\\b`).test(text)) problems.push(`contains "${bad}"`);
  if (/\{\w+\}/.test(text)) problems.push('contains an unreplaced placeholder');
  if (String(text).includes('—')) problems.push('contains an em dash');
  if ((String(text).match(/```/g) || []).length % 2) problems.push('unclosed table');
  if (posts.length > 0) problems.push(...rankingProblems(lines));
  // §4b: reels went out but the account shows zero impressions = the stats feed
  // is broken, not a quiet week. Never post a zero that is really an error.
  if (posts.length > 0 && account && account.impressions === 0) problems.push('reels published but account impressions are 0 (stats feed suspect)');
  return { ok: problems.length === 0, problems };
}

module.exports = {
  weekWindow, isoWeekLabel, hookOf, mediaToPosts, medianBase, rankReels, buildRows, accountTotals,
  fmtDelta, deltaCell, formatPost, validatePost, MAX_REELS_LISTED,
};
