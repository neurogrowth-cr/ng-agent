'use strict';
// Reels briefs for #ng-content (Ron, 2026-10-07): a daily morning brief (what goes
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
const { table, cell } = require('./slackTable');
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
// Layout (Ron, 2026-10-10): a bold header per section carrying its key number,
// and aligned tables (lib/slackTable.js) instead of long bullet lines.
const HOOK_CELL = 30;
const cellNum = (v) => (v === null || v === undefined ? 's/d' : fmt(v));

/** '11 reels · alcance 7,270 · 95 likes · 6 coment.' */
function statsLine(t) {
  const reach = t.reachKnown ? `alcance ${fmt(t.reach)}` : `alcance ${fmt(t.reach)} (incompleto)`;
  return `${plural(t.n, 'reel', 'reels')} · ${reach} · ${plural(t.likes, 'like', 'likes')} · ${fmt(t.comments)} coment.`;
}
const totalsLine = (label, t) => `${label}: ${statsLine(t)}`;
const reelLink = (r, max) => (r.permalink ? `<${r.permalink}|${firstLine(r.caption, max)}>` : firstLine(r.caption, max));

/** Week reels as a table, best reach first. A reel younger than 24 h gets a `*` (footnote under the table). */
function weekTable(reels, now) {
  const rows = [...reels].sort((a, b) => (b.reach ?? -1) - (a.reach ?? -1)).map((r) => {
    const fresh = now.getTime() - r.publishedAt.getTime() < DAY_MS;
    return [crDayShort(r.publishedAt), `${cell(firstLine(r.caption, 200), fresh ? HOOK_CELL - 1 : HOOK_CELL)}${fresh ? '*' : ''}`,
      cellNum(r.reach), fmt(r.likes), fmt(r.comments), cellNum(r.saved)];
  });
  return table(['Día', 'Reel', 'Alcance', 'Likes', 'Com', 'Guard'], rows);
}

/**
 * The morning brief. `schedule` null = GHL unreadable; `reels` null = Meta unreadable.
 * `viewsBlock` = the views section from lib/organicViews.js (or null to leave it out).
 * Every failure becomes a ⚠️ line; nothing is printed as 0 that was not read as 0.
 */
function formatDaily({ now, schedule, reels, viewsBlock = null }) {
  const w = dayWindows(now);
  const warnings = [];
  const today = schedule ? schedule.filter((p) => p.scheduledAt >= w.todayStart && p.scheduledAt < w.todayEnd) : [];
  const missed = schedule ? missedPosts(schedule, now) : [];
  if (!schedule) warnings.push('calendario de GHL ilegible');
  if (!reels) warnings.push('métricas de Meta ilegibles');
  if (missed.length) warnings.push('un reel agendado no salió');
  const week = reels ? inRange(reels, w.weekStart, w.weekEnd) : [];
  if (week.some((r) => r.reach === null)) warnings.push('Meta no dio alcance de algún reel');
  const verdict = warnings.length ? `⚠️ ${[...new Set(warnings)].join('; ')}.` : '✅ Todo publicado a tiempo.';

  const c = crClock(now);
  const lines = [`*📅 Reels · ${DAYS_LONG[c.getUTCDay()]} ${crDate(now)}*`, ''];

  lines.push(`*Hoy se publica* · ${verdict}`);
  if (!schedule) lines.push('⚠️ No pude leer el calendario de GHL.');
  else if (!today.length) lines.push('Hoy no hay reels agendados.');
  else {
    const missedIds = new Set(missed.map((p) => p.id));
    const status = (p) => (p.status === 'published' ? 'publicado' : missedIds.has(p.id) ? 'no salió' : '');
    lines.push(table(null, today.map((p) => [crTime(p.scheduledAt), cell(firstLine(p.caption, 200), 44), status(p)]), ['r', 'l', 'l']));
  }
  for (const p of missed) lines.push(`⚠️ No se publicó: ${crDayShort(p.scheduledAt)} ${crDate(p.scheduledAt)} ${crTime(p.scheduledAt)} · ${firstLine(p.caption, 50)}`);
  if (schedule) {
    const upcoming = schedule.filter((p) => p.status !== 'published' && p.scheduledAt >= w.todayEnd && p.scheduledAt < w.weekEnd).length;
    lines.push(`Próximos esta semana: ${upcoming}`);
  }

  if (viewsBlock) lines.push('', viewsBlock);

  lines.push('');
  if (!reels) {
    lines.push('*📊 Semana*', '⚠️ No pude leer las métricas de Meta.');
  } else {
    const tw = totals(week);
    lines.push(week.length ? `*📊 Semana* · ${statsLine(tw)}` : '*📊 Semana*');
    if (!week.length) lines.push('Todavía no hay reels publicados esta semana.');
    else {
      lines.push(weekTable(week, now));
      const notes = [];
      if (week.some((r) => now.getTime() - r.publishedAt.getTime() < DAY_MS)) notes.push('_* menos de 24 h_');
      const commented = week.filter((r) => r.comments > 0);
      if (commented.length) notes.push(`Con comentarios: ${commented.map((r) => reelLink(r, 25)).join(', ')}`);
      if (notes.length) lines.push(notes.join(' · '));
    }
    if (w.isMonday) {
      const tp = totals(inRange(reels, w.prevWeekStart, w.weekStart));
      lines.push(tp.n ? totalsLine('Semana pasada', tp) : 'Semana pasada: sin reels.');
    }
    const month = inRange(reels, w.monthStart, w.todayEnd);
    const tm = totals(month);
    if (month.length) {
      lines.push('', `*🗓️ Mes en curso* · ${statsLine(tm)}`);
      if (tm.best) lines.push(`Mejor: ${reelLink(tm.best, 60)} (alcance ${fmt(tm.best.reach)})`);
    }
  }
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
  const lines = [`*🗓️ Reels de ${mw.label}*`, ''];
  if (!reels) return [...lines, '⚠️ No pude leer las métricas de Meta. No hay números este mes.'].join('\n');
  const cur = inRange(reels, mw.reportedStart, mw.reportedEnd);
  const prev = inRange(reels, mw.previousStart, mw.reportedStart);
  if (!cur.length) return [...lines, `No se publicaron reels en ${mw.label}.`].join('\n');
  const t = totals(cur); const p = totals(prev);
  const curName = mw.label.split(' ')[0];
  const perReel = (x) => (x.n ? Math.round(x.reach / x.n) : null);
  const metrics = [
    ['Reels', t.n, p.n], [`Alcance${t.reachKnown ? '' : ' (incompl.)'}`, t.reach, p.reach], ['Alcance/reel', perReel(t), perReel(p)],
    ['Likes', t.likes, p.likes], ['Comentarios', t.comments, p.comments], ['Guardados', t.saved, p.saved], ['Compartidos', t.shares, p.shares],
  ];
  if (prev.length) {
    lines.push(`*Resumen* · contra ${mw.previousName}`);
    lines.push(table(['', curName, mw.previousName, 'Cambio'], metrics.map(([k, a, b]) => [k, fmt(a), fmt(b), pct(a, b)]), ['l', 'r', 'r', 'r']));
  } else {
    lines.push(`*Resumen* · sin reels en ${mw.previousName} para comparar`);
    lines.push(table(['', curName], metrics.map(([k, a]) => [k, fmt(a)]), ['l', 'r']));
  }
  const ranked = cur.filter((r) => r.reach !== null).sort((a, b) => b.reach - a.reach);
  const top = ranked.slice(0, 3);
  lines.push('', '*Top 3 por alcance*');
  lines.push(table(['#', 'Reel', 'Fecha', 'Alcance', 'Likes', 'Com'],
    top.map((r, i) => [String(i + 1), cell(firstLine(r.caption, 200), HOOK_CELL), crDate(r.publishedAt), fmt(r.reach), fmt(r.likes), fmt(r.comments)]),
    ['l', 'l', 'l', 'r', 'r', 'r']));
  if (top.some((r) => r.permalink)) lines.push(`Ver: ${top.map((r, i) => (r.permalink ? `<${r.permalink}|${i + 1}>` : `${i + 1}`)).join(' · ')}`);
  const eligible = ranked.filter((r) => r.reach >= 100);
  if (eligible.length) {
    const best = eligible.reduce((a, b) => (engagementRate(b) > engagementRate(a) ? b : a));
    lines.push('', `*Mejor en interacción* · ${reelLink(best, 50)} · ${(engagementRate(best) * 100).toFixed(1)}%`,
      '_likes, comentarios, guardados y compartidos sobre alcance_');
  }
  if (ranked.length > 3) {
    const low = ranked.slice(-3).reverse();
    lines.push('', '*Los 3 más bajos*');
    lines.push(table(['Reel', 'Alcance'], low.map((r) => [cell(firstLine(r.caption, 200), HOOK_CELL), fmt(r.reach)]), ['l', 'r']));
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

// ── monthly "Lectura" (one Claude call in index.js; these helpers are pure) ──
/** Prompt for the 2-3 line read of the month: the reported month's reels, ranked by reach, as data only. */
function lecturaPrompt(now, reels) {
  const mw = monthWindows(now);
  const cur = inRange(reels || [], mw.reportedStart, mw.reportedEnd).filter((r) => r.reach !== null).sort((a, b) => b.reach - a.reach);
  const prevCount = inRange(reels || [], mw.previousStart, mw.reportedStart).length;
  const rows = cur.map((r, i) => `${i + 1}. alcance ${r.reach} · ${r.likes} likes · ${r.comments} comentarios · ${r.saved ?? 0} guardados · ${crDate(r.publishedAt)} · caption: ${String(r.caption).replace(/\s+/g, ' ').slice(0, 220)}`);
  return [
    `Estos son los reels de Instagram de @linkedin.papi publicados en ${mw.label}, ordenados por alcance (el mes anterior se publicaron ${prevCount}).`,
    '', ...rows, '',
    'Escriba 2 o 3 líneas en español, trato de usted: qué tienen en común los reels con más alcance frente a los de menos alcance (tema, tipo de gancho, idioma, formato que se infiere del caption) y una sugerencia concreta para el próximo mes.',
    'Reglas: solo lo que estos datos muestran; si no hay un patrón claro, dígalo en una línea. Sin títulos, sin viñetas, sin emojis, sin signos de exclamación y sin guion largo. No repita los totales del mes.',
  ].join('\n');
}

/** Model output -> 1 to 3 clean lines, or null when unusable (the report then goes out without it). */
function cleanLectura(raw) {
  let t = String(raw || '').trim();
  if (!t) return null;
  t = t.replace(/\s*\u2014\s*/g, ', ').replace(/¡/g, '').replace(/!+/g, '.');
  t = t.split('\n').map((l) => l.replace(/^\s*(?:[-*•]|\d+\.)\s*/, '').replace(/^\*?Lectura\*?:?\s*/i, '').trim()).filter(Boolean).slice(0, 3).join('\n');
  if (t.length < 40 || t.length > 700) return null;
  if (commonProblems(t).length) return null;
  return t;
}

/** Inserts the *Lectura* block right before the closing caveat line. */
function withLectura(text, lectura) {
  if (!lectura) return text;
  const block = `\n\n*Lectura*\n${lectura}`;
  const i = text.lastIndexOf('\n\n_Métricas acumuladas');
  return i === -1 ? text + block : text.slice(0, i) + block + text.slice(i);
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
  if (!/^\*📅 Reels · .+\*$/m.test(text)) problems.push('missing header');
  if (!text.includes('*Hoy se publica*')) problems.push('missing "*Hoy se publica*"');
  if (!text.includes('*📊 Semana*')) problems.push('missing "*📊 Semana*"');
  if (!/(✅ Todo publicado a tiempo\.|⚠️ .+)$/m.test(text)) problems.push('missing verdict');
  if ((text.match(/```/g) || []).length % 2) problems.push('unclosed table');
  return { ok: problems.length === 0, problems };
}

function validateMonthly(text, { reportedCount }) {
  const problems = commonProblems(text);
  if (!/^\*🗓️ Reels de [a-z]+ \d{4}\*$/m.test(text)) problems.push('missing header');
  if (reportedCount > 0) {
    if (!text.includes('*Resumen*')) problems.push('missing "*Resumen*"');
    if (!text.includes('*Top 3 por alcance*')) problems.push('missing "*Top 3 por alcance*"');
    const ranked = (text.match(/^[1-3] {2}\S/gm) || []).length;
    if (ranked < Math.min(3, reportedCount)) problems.push(`top list has ${ranked} rows`);
  }
  if ((text.match(/```/g) || []).length % 2) problems.push('unclosed table');
  return { ok: problems.length === 0, problems };
}

module.exports = {
  dayWindows, monthWindows, normalizeMedia, normalizeSchedule, missedPosts, totals,
  formatDaily, formatMonthly, monthRows, validateDaily, validateMonthly, firstLine, crTime, plural,
  lecturaPrompt, cleanLectura, withLectura,
};
