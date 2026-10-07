'use strict';
// Reels briefs for Ron's DM (Ron, 2026-10-07): a daily morning brief (what goes
// out today on @linkedin.papi, how every reel of the week and the month is doing,
// anything scheduled that never published) and a monthly ranking on day 1.
// Moved here from two desktop scheduled tasks so they no longer depend on the Mac.
// Recipes: ~/automations/ops/recipes/ig-reels-daily-brief.md and
// ~/automations/ops/recipes/ig-reels-monthly-report.md.
//
// Metrics come from Meta (one /media call with insights field expansion), never
// from GHL: GHL's per-post `insights` read 0 likes on reels Meta counts likes on.
// GHL is the source for the schedule (what is queued, what published, what not).
// Pure module: no network, no Slack, no Supabase. Tested in test/reels-briefs.test.js.

const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // America/Costa_Rica, no DST
const DAY_MS = 24 * 60 * 60 * 1000;
const MISSED_GRACE_MS = 30 * 60 * 1000;
const BANNED = ['undefined', 'null', 'NaN', '[object Object]'];
const DAYS_SHORT = ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'];
const DAYS_LONG = ['domingo', 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado'];
const MONTHS = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
const MONTHS_SHORT = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];

// ── time ─────────────────────────────────────────────────────────────────────
/** Wall clock in CR, as a Date whose UTC fields read as CR local time. */
const crClock = (d) => new Date(d.getTime() - CR_OFFSET_MS);
/** UTC instant of CR midnight for the CR calendar day of `d`. */
function crMidnight(d) {
  const c = crClock(d);
  return new Date(Date.UTC(c.getUTCFullYear(), c.getUTCMonth(), c.getUTCDate()) + CR_OFFSET_MS);
}

/** Today, this ISO week (Mon 00:00 CR to next Mon) and last week, as UTC instants. */
function dayWindows(now) {
  const today = crMidnight(now);
  const dow = (crClock(now).getUTCDay() + 6) % 7; // Mon=0
  const weekStart = new Date(today.getTime() - dow * DAY_MS);
  return {
    todayStart: today, todayEnd: new Date(today.getTime() + DAY_MS),
    weekStart, weekEnd: new Date(weekStart.getTime() + 7 * DAY_MS),
    prevWeekStart: new Date(weekStart.getTime() - 7 * DAY_MS),
    monthStart: monthStart(now, 0),
    isMonday: dow === 0,
  };
}

/** UTC instant of 00:00 CR on day 1 of the month `offset` months from `now`'s CR month. */
function monthStart(now, offset) {
  const c = crClock(now);
  return new Date(Date.UTC(c.getUTCFullYear(), c.getUTCMonth() + offset, 1) + CR_OFFSET_MS);
}

/** The month before `now` (reported) and the one before that (compared against). */
function monthWindows(now) {
  const reportedStart = monthStart(now, -1);
  const c = crClock(reportedStart);
  return {
    reportedStart, reportedEnd: monthStart(now, 0), previousStart: monthStart(now, -2),
    label: `${MONTHS[c.getUTCMonth()]} ${c.getUTCFullYear()}`,
    period: `${c.getUTCFullYear()}-${String(c.getUTCMonth() + 1).padStart(2, '0')}`,
    previousName: MONTHS[crClock(monthStart(now, -2)).getUTCMonth()],
  };
}

function crTime(d) {
  const c = crClock(d);
  let h = c.getUTCHours(); const m = String(c.getUTCMinutes()).padStart(2, '0');
  const ap = h >= 12 ? 'pm' : 'am'; h = h % 12 || 12;
  return `${h}:${m} ${ap}`;
}
const crDayShort = (d) => DAYS_SHORT[crClock(d).getUTCDay()];
const crDate = (d) => { const c = crClock(d); return `${c.getUTCDate()} ${MONTHS_SHORT[c.getUTCMonth()]}`; };
const crLongDate = (d) => { const c = crClock(d); return `${DAYS_LONG[c.getUTCDay()]} ${c.getUTCDate()} de ${MONTHS[c.getUTCMonth()]}`; };

// ── text helpers ─────────────────────────────────────────────────────────────
const fmt = (n) => Number(n).toLocaleString('en-US');
const plural = (n, one, many) => `${fmt(n)} ${n === 1 ? one : many}`;
/** First non-empty caption line, trimmed to `max` characters. */
function firstLine(caption, max = 45) {
  const line = String(caption || '').split('\n').map((s) => s.trim()).find(Boolean) || '(sin caption)';
  return line.length > max ? `${line.slice(0, max - 1).trimEnd()}…` : line;
}
const link = (url) => (url ? ` · <${url}|ver>` : '');
const finite = (v) => (v === null || v === undefined || v === '' ? null : (Number.isFinite(Number(v)) ? Number(v) : null));

// ── data ─────────────────────────────────────────────────────────────────────
/**
 * Meta /{ig-user}/media rows -> reels. `insights` comes from the field expansion
 * insights.metric(reach,saved,shares); when it is missing, reach/saved/shares are
 * null (shown as "sin datos", never as 0).
 */
function normalizeMedia(items) {
  const out = [];
  for (const m of Array.isArray(items) ? items : []) {
    if (!m || m.media_product_type !== 'REELS') continue;
    const at = Date.parse(m.timestamp || '');
    if (!Number.isFinite(at)) continue;
    const ins = {};
    for (const row of (m.insights && m.insights.data) || []) ins[row.name] = finite(row.values && row.values[0] && row.values[0].value);
    out.push({
      id: String(m.id), publishedAt: new Date(at), caption: m.caption || '', permalink: m.permalink || null,
      likes: finite(m.like_count) ?? 0, comments: finite(m.comments_count) ?? 0,
      reach: ins.reach ?? null, saved: ins.saved ?? null, shares: ins.shares ?? null,
    });
  }
  return out.sort((a, b) => b.publishedAt - a.publishedAt);
}

const inRange = (reels, start, end) => reels.filter((r) => r.publishedAt >= start && r.publishedAt < end);

function totals(reels) {
  const sum = (k) => reels.reduce((s, r) => s + (r[k] || 0), 0);
  const withReach = reels.filter((r) => r.reach !== null);
  return {
    n: reels.length, reach: sum('reach'), likes: sum('likes'), comments: sum('comments'),
    saved: sum('saved'), shares: sum('shares'), reachKnown: withReach.length === reels.length,
    best: withReach.length ? withReach.reduce((a, b) => (b.reach > a.reach ? b : a)) : null,
  };
}

/**
 * GHL posts/list rows (type all) -> schedule entries. GHL keeps the parent post
 * `scheduled` forever; the published result is a child with parentPostId. A
 * parent whose child is in the list is dropped so a reel is never counted twice.
 */
function normalizeSchedule(ghlPosts) {
  // No platform filter: the request is already scoped to the Instagram account,
  // and GHL labels still-scheduled posts `platform: 'google'` (seen 2026-10-07).
  const posts = (Array.isArray(ghlPosts) ? ghlPosts : []).filter((p) => p && !p.deleted);
  const childOf = new Set(posts.filter((p) => p.parentPostId).map((p) => String(p.parentPostId)));
  return posts
    .filter((p) => !childOf.has(String(p._id)))
    .map((p) => ({
      id: String(p._id), status: p.status || 'unknown',
      at: new Date(Date.parse(p.publishedAt || p.scheduleDate || p.displayDate || '')),
      scheduledAt: new Date(Date.parse(p.scheduleDate || p.displayDate || '')),
      caption: p.summary || '', permalink: p.previewLink || null,
    }))
    .filter((p) => Number.isFinite(p.scheduledAt.getTime()))
    .sort((a, b) => a.scheduledAt - b.scheduledAt);
}

/** Scheduled posts whose time passed more than 30 min ago without publishing, plus any `failed`. */
function missedPosts(schedule, now) {
  return schedule.filter((p) => p.status === 'failed'
    || (p.status !== 'published' && p.scheduledAt.getTime() < now.getTime() - MISSED_GRACE_MS));
}

// ── daily brief ──────────────────────────────────────────────────────────────
function reelRow(r, now) {
  const parts = [r.reach === null ? '⚠️ sin datos' : `alcance ${fmt(r.reach)}`,
    plural(r.likes, 'like', 'likes'), plural(r.comments, 'comentario', 'comentarios')];
  if (r.saved !== null) parts.push(plural(r.saved, 'guardado', 'guardados'));
  const fresh = now.getTime() - r.publishedAt.getTime() < DAY_MS ? ' (menos de 24 h)' : '';
  return `• ${crDayShort(r.publishedAt)} · ${firstLine(r.caption)} · ${parts.join(' · ')}${link(r.permalink)}${fresh}`;
}

function totalsLine(label, t) {
  const reach = t.reachKnown ? `alcance ${fmt(t.reach)}` : `alcance ${fmt(t.reach)} (incompleto)`;
  return `${label}: ${plural(t.n, 'reel', 'reels')} · ${reach} · ${plural(t.likes, 'like', 'likes')} · ${plural(t.comments, 'comentario', 'comentarios')}`;
}

/**
 * The morning DM. `schedule` null = GHL unreadable; `reels` null = Meta unreadable.
 * Every failure becomes a ⚠️ line; nothing is printed as 0 that was not read as 0.
 */
function formatDaily({ now, schedule, reels }) {
  const w = dayWindows(now);
  const lines = [`*Reels de hoy, ${crLongDate(now)}*`, ''];
  const warnings = [];

  lines.push('*Hoy se publica*');
  if (!schedule) {
    lines.push('⚠️ No pude leer el calendario de GHL.');
    warnings.push('calendario de GHL ilegible');
  } else {
    const today = schedule.filter((p) => p.scheduledAt >= w.todayStart && p.scheduledAt < w.todayEnd);
    if (!today.length) lines.push('Hoy no hay reels agendados.');
    for (const p of today) lines.push(`• ${crTime(p.scheduledAt)} · ${firstLine(p.caption, 60)}${p.status === 'published' ? ' (ya publicado)' : ''}`);
  }
  lines.push('');

  lines.push('*Semana* (desde el lunes)');
  let commented = [];
  if (!reels) {
    lines.push('⚠️ No pude leer las métricas de Meta.');
    warnings.push('métricas de Meta ilegibles');
  } else {
    const week = inRange(reels, w.weekStart, w.weekEnd);
    if (!week.length) lines.push('Todavía no hay reels publicados esta semana.');
    for (const r of week) lines.push(reelRow(r, now));
    lines.push('');
    const tw = totals(week);
    if (week.length) {
      lines.push(totalsLine('Total semana', tw));
      if (tw.best) lines.push(`Mejor de la semana: ${firstLine(tw.best.caption, 60)} (alcance ${fmt(tw.best.reach)})`);
    }
    const month = inRange(reels, w.monthStart, w.todayEnd);
    const tm = totals(month);
    if (month.length) lines.push(`${totalsLine('Mes en curso', tm)}${tm.best ? ` · mejor: ${firstLine(tm.best.caption, 40)} (alcance ${fmt(tm.best.reach)})` : ''}`);
    if (w.isMonday) {
      const tp = totals(inRange(reels, w.prevWeekStart, w.weekStart));
      lines.push(tp.n ? totalsLine('Semana pasada', tp) : 'Semana pasada: sin reels.');
    }
    if (week.some((r) => r.reach === null)) warnings.push('Meta no dio alcance de algún reel');
    commented = week.filter((r) => r.comments > 0);
  }

  if (schedule) {
    const upcoming = schedule.filter((p) => p.status !== 'published' && p.scheduledAt >= w.todayEnd && p.scheduledAt < w.weekEnd).length;
    lines.push('', `Próximos esta semana: ${upcoming}`);
    for (const p of missedPosts(schedule, now)) {
      lines.push(`⚠️ No se publicó: ${crDayShort(p.scheduledAt)} ${crDate(p.scheduledAt)} ${crTime(p.scheduledAt)} · ${firstLine(p.caption, 50)}`);
      warnings.push('un reel agendado no salió');
    }
  }

  lines.push('', warnings.length ? `⚠️ ${[...new Set(warnings)].join('; ')}.` : '✅ Todo publicado a tiempo.');
  if (commented.length) lines.push('', `Reels con comentarios esta semana: ${commented.map((r) => `<${r.permalink}|${firstLine(r.caption, 25)}>`).join(', ')}`);
  return lines.join('\n');
}

// ── monthly report ───────────────────────────────────────────────────────────
function pct(cur, prev) {
  if (!prev) return 'sin base';
  const d = Math.round(((cur - prev) / prev) * 100);
  return `${d > 0 ? '+' : ''}${d}%`;
}
const engagementRate = (r) => (r.likes + r.comments + (r.saved || 0) + (r.shares || 0)) / r.reach;

function formatMonthly({ now, reels }) {
  const mw = monthWindows(now);
  const lines = [`*Reels de ${mw.label}*`, ''];
  if (!reels) return [...lines, '⚠️ No pude leer las métricas de Meta. No hay números este mes.'].join('\n');
  const cur = inRange(reels, mw.reportedStart, mw.reportedEnd);
  const prev = inRange(reels, mw.previousStart, mw.reportedStart);
  if (!cur.length) return [...lines, `No se publicaron reels en ${mw.label}.`].join('\n');
  const t = totals(cur); const p = totals(prev);
  lines.push(`Total: ${plural(t.n, 'reel', 'reels')} · alcance ${fmt(t.reach)}${t.reachKnown ? '' : ' (incompleto)'} · ${plural(t.likes, 'like', 'likes')} · ${plural(t.comments, 'comentario', 'comentarios')} · ${plural(t.saved, 'guardado', 'guardados')} · ${plural(t.shares, 'compartido', 'compartidos')}`);
  if (prev.length) {
    lines.push(`Contra ${mw.previousName}: alcance ${pct(t.reach, p.reach)} · likes ${pct(t.likes, p.likes)} · reels publicados ${p.n} a ${t.n}`);
    lines.push(`Alcance promedio por reel: ${fmt(Math.round(t.reach / t.n))} (${mw.previousName}: ${fmt(Math.round(p.reach / p.n))})`);
  } else {
    lines.push(`Alcance promedio por reel: ${fmt(Math.round(t.reach / t.n))} (sin reels en ${mw.previousName} para comparar)`);
  }
  const ranked = cur.filter((r) => r.reach !== null).sort((a, b) => b.reach - a.reach);
  lines.push('', '*Top 3 por alcance*');
  ranked.slice(0, 3).forEach((r, i) => lines.push(`${i + 1}. ${firstLine(r.caption, 50)} · ${crDate(r.publishedAt)} · alcance ${fmt(r.reach)} · ${plural(r.likes, 'like', 'likes')} · ${plural(r.comments, 'comentario', 'comentarios')}${link(r.permalink)}`));
  const eligible = ranked.filter((r) => r.reach >= 100);
  if (eligible.length) {
    const top = eligible.reduce((a, b) => (engagementRate(b) > engagementRate(a) ? b : a));
    lines.push('', '*Mejor en interacción* (likes, comentarios, guardados y compartidos sobre alcance)',
      `${firstLine(top.caption, 50)} · ${(engagementRate(top) * 100).toFixed(1)}%${link(top.permalink)}`);
  }
  if (ranked.length > 3) {
    lines.push('', '*Los 3 más bajos*');
    for (const r of ranked.slice(-3).reverse()) lines.push(`• ${firstLine(r.caption, 50)} · alcance ${fmt(r.reach)}${link(r.permalink)}`);
  }
  lines.push('', '_Métricas acumuladas a hoy; un reel de fin de mes tuvo menos días para sumar._');
  return lines.join('\n');
}

/** reel_stats rows for the month snapshot. iso_week holds the period 'YYYY-MM'; kind 'month' keeps them apart from the weekly rows. */
function monthRows(now, reels) {
  const mw = monthWindows(now);
  return inRange(reels || [], mw.reportedStart, mw.reportedEnd).map((r) => ({
    iso_week: mw.period, post_id: r.id, kind: 'month', hook: firstLine(r.caption, 70), permalink: r.permalink,
    published_at: r.publishedAt.toISOString(), likes: r.likes, comments: r.comments, shares: r.shares ?? 0,
    saved: r.saved, reach: r.reach, views: null, captured_at: now.toISOString(),
  }));
}

// ── criteria (recipes §4a) ───────────────────────────────────────────────────
function commonProblems(text) {
  const problems = [];
  for (const bad of BANNED) if (new RegExp(`\\b${bad.replace(/[[\]]/g, '\\$&')}\\b`).test(text)) problems.push(`contains "${bad}"`);
  if (text.includes('\u2014')) problems.push('contains an em dash');
  if (/\{\w+\}/.test(text)) problems.push('contains an unreplaced placeholder');
  return problems;
}

function validateDaily(text) {
  const problems = commonProblems(text);
  if (!/^\*Reels de hoy, .+\*$/m.test(text)) problems.push('missing header');
  if (!text.includes('*Hoy se publica*')) problems.push('missing "*Hoy se publica*"');
  if (!text.includes('*Semana*')) problems.push('missing "*Semana*"');
  if (!/^(✅ Todo publicado a tiempo\.|⚠️ .+)$/m.test(text)) problems.push('missing verdict line');
  return { ok: problems.length === 0, problems };
}

function validateMonthly(text, { reportedCount }) {
  const problems = commonProblems(text);
  if (!/^\*Reels de [a-z]+ \d{4}\*$/m.test(text)) problems.push('missing header');
  if (reportedCount > 0) {
    if (!/^Total: \d/m.test(text)) problems.push('missing "Total:" line');
    if (!text.includes('*Top 3 por alcance*')) problems.push('missing "*Top 3 por alcance*"');
    const ranked = (text.match(/^\d\. .+ · alcance [\d,]+/gm) || []).length;
    if (ranked < Math.min(3, reportedCount)) problems.push(`top list has ${ranked} rows`);
  }
  return { ok: problems.length === 0, problems };
}

module.exports = {
  dayWindows, monthWindows, normalizeMedia, normalizeSchedule, missedPosts, totals,
  formatDaily, formatMonthly, monthRows, validateDaily, validateMonthly, firstLine, crTime, plural,
};
