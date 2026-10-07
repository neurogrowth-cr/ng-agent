'use strict';
// Reels Weekly: the Monday 08:00 CR summary of last week's Instagram reels for
// #ng-content (Ron, 2026-10-07). Recipe: ~/automations/ops/recipes/reels-weekly.md.
//
// Sources are the GHL Social Planner (published posts with like/comment/share
// insights, plus account statistics) and, when META_IG_INSIGHTS_TOKEN is set,
// the Instagram Graph API for per-reel reach, views and saves. This module is
// pure (no Slack, no network, no Supabase): it picks the week, normalises the
// posts, builds the reel_stats rows, formats one message and validates it.
// Tested in test/reels-weekly.test.js. index.js does the I/O.

const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // America/Costa_Rica has no DST
const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_REELS_LISTED = 8;
const HOOK_MAX = 70;
const BANNED = ['undefined', 'null', 'NaN', '[object Object]'];
const HEADER = /^📊 Reels · semana \d{4}-W\d{2} \(.+\)$/;
const COUNT_LINE = /^Reels publicados: \d+/m;

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
 * GHL child posts (status published, platform instagram) inside the window,
 * reduced to what the report needs. Drops anything that is not a published reel
 * or has no Instagram media id.
 */
function normalizePosts(ghlPosts, window) {
  const out = [];
  for (const p of Array.isArray(ghlPosts) ? ghlPosts : []) {
    if (!p || p.status !== 'published' || p.platform !== 'instagram' || p.deleted) continue;
    const at = Date.parse(p.publishedAt || p.displayDate || '');
    if (!Number.isFinite(at) || at < window.start.getTime() || at >= window.end.getTime()) continue;
    if (!p.postId) continue;
    const ins = p.insights || {};
    out.push({
      postId: String(p.postId),
      ghlPostId: String(p._id || ''),
      type: p.type || 'post',
      publishedAt: new Date(at).toISOString(),
      permalink: p.previewLink || null,
      hook: hookOf(p.summary),
      likes: num(ins.like), comments: num(ins.comment), shares: num(ins.share),
      reach: null, views: null, saved: null,
    });
  }
  return out.sort((a, b) => a.publishedAt.localeCompare(b.publishedAt));
}

/**
 * Meta insights keyed by media id: { reach, views, saved, shares, likes, comments }.
 * Meta's counts win over GHL's when present (GHL syncs insights lazily).
 */
function mergeMetaInsights(posts, byMediaId) {
  if (!byMediaId) return posts;
  return posts.map((p) => {
    const m = byMediaId[p.postId];
    if (!m) return p;
    return {
      ...p,
      reach: Number.isFinite(m.reach) ? m.reach : p.reach,
      views: Number.isFinite(m.views) ? m.views : p.views,
      saved: Number.isFinite(m.saved) ? m.saved : p.saved,
      shares: Number.isFinite(m.shares) ? m.shares : p.shares,
      likes: Number.isFinite(m.likes) ? m.likes : p.likes,
      comments: Number.isFinite(m.comments) ? m.comments : p.comments,
    };
  });
}

/** Engagement for ranking when reach is unknown. */
const engagement = (p) => p.likes + p.comments + p.shares + (p.saved || 0);

function rankReels(posts) {
  const haveReach = posts.some((p) => Number.isFinite(p.reach));
  return [...posts].sort((a, b) => (haveReach ? (b.reach || 0) - (a.reach || 0) : engagement(b) - engagement(a)));
}

/** Rows for reel_stats: one per reel plus one `account` row with the GHL totals. */
function buildRows(posts, account, window, capturedAt) {
  const rows = posts.map((p) => ({
    iso_week: window.isoWeek, post_id: p.postId, ghl_post_id: p.ghlPostId || null, kind: 'reel',
    hook: p.hook, permalink: p.permalink, published_at: p.publishedAt,
    likes: p.likes, comments: p.comments, shares: p.shares, saved: p.saved, reach: p.reach, views: p.views,
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

/**
 * The Monday post. One line when nothing was published. Otherwise: header,
 * totals with WoW deltas, ranking (reach when Meta is wired, engagement
 * otherwise), the hook that won, and the data caveats.
 */
function formatPost({ window, posts, account, prevAccount, metaWired }) {
  const lines = [`📊 Reels · semana ${window.isoWeek} (${window.label})`];
  if (!posts.length) {
    lines.push('Reels publicados: 0. Sin publicaciones esta semana, no hay nada que medir.');
    return lines.join('\n');
  }
  const a = account || {};
  const pa = prevAccount || {};
  const totals = [`Reels publicados: ${posts.length}`];
  if (a.impressions != null) totals.push(`Impresiones: ${fmt(a.impressions)}${fmtDelta(a.impressions, pa.impressions)}`);
  if (a.reach != null) totals.push(`Alcance: ${fmt(a.reach)}${fmtDelta(a.reach, pa.reach)}`);
  if (a.followers != null) totals.push(`Seguidores nuevos: ${fmt(a.followers)}${fmtDelta(a.followers, pa.followers)}`);
  lines.push(totals.join(' · '));
  const ranked = rankReels(posts);
  const haveReach = ranked.some((p) => Number.isFinite(p.reach));
  lines.push(haveReach ? 'Ranking por alcance:' : 'Ranking por interacciones (likes + comentarios + compartidos):');
  ranked.slice(0, MAX_REELS_LISTED).forEach((p, i) => {
    const parts = [];
    if (Number.isFinite(p.reach)) parts.push(`${fmt(p.reach)} alcance`);
    if (Number.isFinite(p.views)) parts.push(`${fmt(p.views)} reproducciones`);
    parts.push(`${p.likes} likes`, `${p.comments} comentarios`, `${p.shares} compartidos`);
    if (Number.isFinite(p.saved)) parts.push(`${p.saved} guardados`);
    const link = p.permalink ? ` <${p.permalink}|ver>` : '';
    lines.push(`${i + 1}. ${p.hook}${link} · ${parts.join(' · ')}`);
  });
  if (ranked.length > MAX_REELS_LISTED) lines.push(`…y ${ranked.length - MAX_REELS_LISTED} más en reel_stats.`);
  lines.push(`Lo que más funcionó: "${ranked[0].hook}"`);
  if (!metaWired) lines.push('Alcance y reproducciones por reel: pendiente el token de Meta con insights (META_IG_INSIGHTS_TOKEN).');
  return lines.join('\n');
}

/** Criteria on the text, before it is posted (recipe §4a). */
function validatePost(text, { posts = [], account = null } = {}) {
  const problems = [];
  const lines = String(text || '').split('\n');
  if (!HEADER.test(lines[0] || '')) problems.push('first line is not the header');
  if (!COUNT_LINE.test(text)) problems.push('missing "Reels publicados: N"');
  for (const bad of BANNED) if (new RegExp(`\\b${bad.replace(/[[\]]/g, '\\$&')}\\b`).test(text)) problems.push(`contains "${bad}"`);
  if (/\{\w+\}/.test(text)) problems.push('contains an unreplaced placeholder');
  if (lines.length > 4 + MAX_REELS_LISTED + 3) problems.push(`too many lines (${lines.length})`);
  // §4b: reels went out but the account shows zero impressions = the stats feed
  // is broken, not a quiet week. Never post a zero that is really an error.
  if (posts.length > 0 && account && account.impressions === 0) problems.push('reels published but account impressions are 0 (stats feed suspect)');
  return { ok: problems.length === 0, problems };
}

module.exports = {
  weekWindow, isoWeekLabel, hookOf, normalizePosts, mergeMetaInsights, rankReels, buildRows, accountTotals,
  fmtDelta, formatPost, validatePost, MAX_REELS_LISTED,
};
