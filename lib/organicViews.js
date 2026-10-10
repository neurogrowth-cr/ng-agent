'use strict';
// Organic views across Instagram, YouTube and TikTok (Ron, 2026-10-09): one
// place to count views per platform and overall, by week and by month.
//
// Every platform only gives a video's LIFETIME views, so a nightly job stores
// one snapshot per video per CR day in video_views (migration 021). Views
// gained in a period = how much those lifetime totals grew between the last
// snapshot before the period and the last snapshot inside it. That counts views
// on old videos too, which is the real organic number. A video first seen
// inside the period counts from 0 if it was published inside the period;
// otherwise it counts from its first snapshot and the period is marked partial
// (the history before tracking started is unknown, never guessed).
//
// Sources: Instagram = Meta media insights `views`; YouTube = Data API v3
// statistics.viewCount for every upload on the channel; TikTok = playCount on
// the public video page of every TikTok post GHL published (no official API
// without an app review). Pure module: no network, no Slack, no Supabase.
// Tested in test/organic-views.test.js.

const { table } = require('./slackTable');

const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // America/Costa_Rica, no DST
const DAY_MS = 24 * 60 * 60 * 1000;
const PLATFORMS = ['instagram', 'youtube', 'tiktok'];
const LABEL = { instagram: 'Instagram', youtube: 'YouTube', tiktok: 'TikTok' };
const MONTHS_LONG = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
const MONTHS = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];

const fmt = (n) => (n == null ? '-' : Number(n).toLocaleString('en-US'));

/** CR calendar date of an instant, as 'YYYY-MM-DD'. */
function crDate(d) {
  return new Date(d.getTime() - CR_OFFSET_MS).toISOString().slice(0, 10);
}
const addDays = (dateStr, n) => new Date(Date.parse(`${dateStr}T00:00:00Z`) + n * DAY_MS).toISOString().slice(0, 10);
/** UTC instant of 00:00 CR on a 'YYYY-MM-DD' CR date. */
const crStart = (dateStr) => new Date(Date.parse(`${dateStr}T00:00:00Z`) + CR_OFFSET_MS);
const shortDate = (dateStr) => `${Number(dateStr.slice(8, 10))} ${MONTHS[Number(dateStr.slice(5, 7)) - 1]}`;

// ── normalizers: one shape for every platform ────────────────────────────────
// { platform, videoId, views, publishedAt (ISO|null), title }

/** Meta /{ig-user}/media rows with insights.metric(views) expanded. Media without a views number are skipped. */
function normalizeIg(media) {
  const out = [];
  for (const m of media || []) {
    const row = ((m.insights && m.insights.data) || []).find((r) => r.name === 'views');
    const views = Number(row && row.values && row.values[0] && row.values[0].value);
    if (!m.id || !Number.isFinite(views)) continue;
    out.push({ platform: 'instagram', videoId: String(m.id), views, publishedAt: m.timestamp ? new Date(m.timestamp).toISOString() : null,
      title: firstLine(m.caption) });
  }
  return out;
}

/** YouTube Data API videos.list items (part=snippet,statistics). */
function normalizeYt(items) {
  const out = [];
  for (const v of items || []) {
    const views = Number(v.statistics && v.statistics.viewCount);
    if (!v.id || !Number.isFinite(views)) continue;
    out.push({ platform: 'youtube', videoId: String(v.id), views, publishedAt: (v.snippet && v.snippet.publishedAt) || null,
      title: firstLine(v.snippet && v.snippet.title) });
  }
  return out;
}

/** The video's own playCount from a TikTok video page, or null when the page has none (blocked, captcha, removed). */
function parseTikTokPlayCount(html) {
  const m = /"stats":\{[^{}]*?"playCount":(\d+)/.exec(String(html || ''));
  return m ? Number(m[1]) : null;
}

/** Published TikTok child posts from GHL posts/list: { videoId, url, publishedAt, title }. */
function tiktokPostsFromGhl(posts) {
  const seen = new Set();
  const out = [];
  for (const p of posts || []) {
    if (p.deleted || p.platform !== 'tiktok' || p.status !== 'published' || !p.postId || !p.previewLink) continue;
    if (seen.has(p.postId)) continue;
    seen.add(p.postId);
    out.push({ videoId: String(p.postId), url: p.previewLink, publishedAt: p.publishedAt || null, title: firstLine(p.summary) });
  }
  return out;
}

function firstLine(s) {
  const line = String(s || '').split('\n').map((x) => x.trim()).find(Boolean) || '';
  return line.length > 80 ? `${line.slice(0, 79)}…` : line;
}

/** video_views rows for one CR day. */
function snapshotRows(dateStr, videos, capturedAt) {
  return videos.map((v) => ({
    snapshot_date: dateStr, platform: v.platform, video_id: v.videoId, views: v.views,
    published_at: v.publishedAt, title: v.title || null, captured_at: capturedAt.toISOString(),
  }));
}

// ── periods ──────────────────────────────────────────────────────────────────

/** Last full CR week (Mon..Sun) before `now`, and the week before it. */
function weekPeriods(now) {
  const today = crDate(now);
  const dow = (new Date(`${today}T00:00:00Z`).getUTCDay() + 6) % 7; // Mon=0
  const thisMon = addDays(today, -dow);
  const cur = { from: addDays(thisMon, -7), to: addDays(thisMon, -1) };
  const prev = { from: addDays(thisMon, -14), to: addDays(thisMon, -8) };
  cur.label = `${shortDate(cur.from)} a ${shortDate(cur.to)}`;
  return { cur, prev };
}

/** Last full CR month before `now`, and the month before it. */
function monthPeriods(now) {
  const today = crDate(now);
  const firstThis = `${today.slice(0, 7)}-01`;
  const lastPrev = addDays(firstThis, -1);
  const firstPrev = `${lastPrev.slice(0, 7)}-01`;
  const lastPrev2 = addDays(firstPrev, -1);
  return {
    cur: { from: firstPrev, to: lastPrev, label: `${MONTHS_LONG[Number(firstPrev.slice(5, 7)) - 1]} ${firstPrev.slice(0, 4)}` },
    prev: { from: `${lastPrev2.slice(0, 7)}-01`, to: lastPrev2 },
  };
}

/** Day 1 of the CR month that holds `to`, through `to` ("Acumulado mensual"). */
function monthToDatePeriod(to) {
  return { from: `${to.slice(0, 7)}-01`, to, label: MONTHS_LONG[Number(to.slice(5, 7)) - 1] };
}

/** Yesterday (CR) as a one-day period. */
function dayPeriod(now) {
  const y = addDays(crDate(now), -1);
  return { from: y, to: y, label: shortDate(y) };
}

/**
 * Views gained per platform in a period [from..to] (CR dates, inclusive), from
 * video_views rows. Returns { byPlatform, total, lifetime, partialSince, platformsSeen }:
 *  - byPlatform[p] is null when the platform has no snapshot inside the period
 *    (a dead feed must read "sin datos", never 0);
 *  - lifetime[p] = sum of each video's latest lifetime views at `to` (kept for the data, not shown);
 *  - partialSince = first snapshot date when some video's baseline is unknown.
 */
function periodViews(rows, { from, to }) {
  const baseDate = addDays(from, -1);
  const fromInstant = crStart(from).getTime();
  const byVideo = new Map();
  for (const r of rows || []) {
    if (r.snapshot_date > to) continue;
    const k = `${r.platform}|${r.video_id}`;
    if (!byVideo.has(k)) byVideo.set(k, []);
    byVideo.get(k).push(r);
  }
  const byPlatform = {}; const lifetime = {}; const seenInPeriod = {};
  let partialSince = null;
  for (const p of PLATFORMS) { byPlatform[p] = 0; lifetime[p] = 0; seenInPeriod[p] = false; }
  for (const list of byVideo.values()) {
    list.sort((a, b) => (a.snapshot_date < b.snapshot_date ? -1 : 1));
    const p = list[0].platform;
    if (!PLATFORMS.includes(p)) continue;
    const inPeriod = list.filter((r) => r.snapshot_date >= from);
    if (!inPeriod.length) continue; // video vanished before the period (deleted): nothing to add
    seenInPeriod[p] = true;
    const end = Number(inPeriod[inPeriod.length - 1].views);
    const before = list.filter((r) => r.snapshot_date <= baseDate);
    let base;
    if (before.length) base = Number(before[before.length - 1].views);
    else if (list[0].published_at && Date.parse(list[0].published_at) >= fromInstant) base = 0;
    else {
      base = Number(inPeriod[0].views);
      if (!partialSince || inPeriod[0].snapshot_date < partialSince) partialSince = inPeriod[0].snapshot_date;
    }
    // Platforms occasionally revise a count down (spam views removed); never report negative growth.
    byPlatform[p] += Math.max(0, end - base);
    lifetime[p] += end;
  }
  let total = 0;
  for (const p of PLATFORMS) {
    if (!seenInPeriod[p]) { byPlatform[p] = null; lifetime[p] = null; } else total += byPlatform[p];
  }
  const platformsSeen = PLATFORMS.filter((p) => seenInPeriod[p]);
  return { byPlatform, total: platformsSeen.length ? total : null, lifetime, partialSince, platformsSeen };
}

function pctDelta(cur, prev) {
  if (cur == null || prev == null || prev === 0) return '';
  const pct = Math.round(((cur - prev) / prev) * 100);
  if (pct === 0) return ' (= vs anterior)';
  return pct > 0 ? ` (▲ ${pct}% vs anterior)` : ` (▼ ${Math.abs(pct)}% vs anterior)`;
}

/**
 * The views section of the weekly and monthly reels posts (Ron, 2026-10-10:
 * header with the total, then an aligned table per platform).
 * column = label of the period column ('Semana', 'Octubre').
 * funnel = { comments, conversations, booked } (each a number or null = sin datos).
 * monthToDate = periodViews() from day 1 of the month to the end of `cur`, plus a `label`
 * (Ron, 2026-10-09: "Acumulado mensual" instead of lifetime totals); omitted on the monthly post.
 */
function formatViewsBlock({ title, cur, prev, funnel, monthToDate = null, column = 'Periodo', prevComparable = true }) {
  if (cur.total == null) return [`*👁️ Vistas orgánicas, ${title}*`, '⚠️ Sin datos de vistas todavía (la foto nocturna no ha corrido o falló).'].join('\n');
  const comparable = prevComparable && prev && prev.total != null && !prev.partialSince;
  const lines = [`*👁️ Vistas orgánicas, ${title}* · total ${fmt(cur.total)}${comparable ? pctDelta(cur.total, prev.total) : ''}`];
  const mtd = monthToDate && monthToDate.total != null ? monthToDate : null;
  const cellOf = (v) => (v == null ? 's/d' : fmt(v));
  const headers = ['', column, ...(mtd ? [`Acum. ${mtd.label}`] : [])];
  const rows = PLATFORMS.map((p) => [LABEL[p], cellOf(cur.byPlatform[p]), ...(mtd ? [cellOf(mtd.byPlatform[p])] : [])]);
  rows.push(['Total', fmt(cur.total), ...(mtd ? [fmt(mtd.total)] : [])]);
  lines.push(table(headers, rows, ['l', 'r', 'r']));
  if (funnel) {
    const step = (n, one, many) => (n == null ? `sin datos de ${many}` : `${fmt(n)} ${n === 1 ? one : many}`);
    lines.push(`Embudo: ${fmt(cur.total)} vistas → ${step(funnel.comments, 'comentario "LinkedIn"', 'comentarios "LinkedIn"')} → ${step(funnel.conversations, 'conversación', 'conversaciones')} → ${step(funnel.booked, 'llamada agendada', 'llamadas agendadas')}`);
  }
  if (cur.partialSince) lines.push(`_Parcial: la medición empezó el ${shortDate(cur.partialSince)}, las vistas de antes no se cuentan._`);
  return lines.join('\n');
}

/** The views section of the daily brief, or null without data. */
function formatDailyBlock(cur) {
  if (!cur || cur.total == null) return null;
  const rows = PLATFORMS.filter((p) => cur.byPlatform[p] != null).map((p) => [LABEL[p], fmt(cur.byPlatform[p])]);
  return [`*👁️ Vistas de ayer: ${fmt(cur.total)}*${cur.partialSince ? ' _(parcial)_' : ''}`, table(null, rows, ['l', 'r'])].join('\n');
}

const BANNED = ['undefined', 'null', 'NaN', '[object Object]', '—'];
function validateBlock(text) {
  const problems = [];
  for (const b of BANNED) if (String(text).includes(b)) problems.push(`contains "${b}"`);
  if (!/^\*👁️ Vistas (orgánicas, .+|de ayer: [\d,]+)\*/m.test(text)) problems.push('missing header');
  if ((String(text).match(/```/g) || []).length % 2) problems.push('unclosed table');
  return { ok: problems.length === 0, problems };
}

/** Comments that ask for the guide: text mentions LinkedIn, posted inside [startMs, endMs). */
function countLinkedInComments(comments, startMs, endMs) {
  return (comments || []).filter((c) => /linked\s*in/i.test(String(c.text || ''))
    && Date.parse(c.timestamp) >= startMs && Date.parse(c.timestamp) < endMs).length;
}

module.exports = {
  PLATFORMS, crDate, crStart, addDays, normalizeIg, normalizeYt, parseTikTokPlayCount, tiktokPostsFromGhl, snapshotRows,
  weekPeriods, monthPeriods, monthToDatePeriod, dayPeriod, periodViews, pctDelta, formatViewsBlock, formatDailyBlock, validateBlock,
  countLinkedInComments,
};
