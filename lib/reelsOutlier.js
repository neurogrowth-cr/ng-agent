'use strict';
// Outlier multiple and sends per reach for the reels reports (Ron, 2026-10-23,
// ported from the ig-audit method in Jakeschincariol/instagram-agent-skill).
// Raw views and reach mostly measure how many people already follow the
// account, so a reel is ranked by how far it sits from the account's own recent
// median (the multiple) and then by shares per 1,000 reached (a send is a person
// putting their name on it). Pure: no network, no Supabase. index.js reads the
// trailing weeks of reel_stats and hands the rows in. Shared by
// lib/reelsWeekly.js and lib/reelsBriefs.js; tested through their test files.

const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // America/Costa_Rica has no DST
const DAY_MS = 24 * 60 * 60 * 1000;
const HISTORY_WEEKS = 8;      // the median comes from this many ISO weeks of reel_stats
const MEDIAN_FLOOR = 5;       // no multiple when the median rests on fewer reels
const VERDICT_FLOOR = 5;      // no verdict with fewer reels with reach in the period
const HIT_MULTIPLE = 2;       // a reel at 2x the median travelled
const LOW_FOLLOWS_PER_1K = 5; // under 5 new followers per 1,000 reached, the profile did not convert
const FLAT_REACH_PCT = 10;    // period reach within +10% of the previous one is flat

// ── ISO weeks (CR calendar) ──────────────────────────────────────────────────
/** 'YYYY-Www' of the CR calendar day that contains `instant`. */
function isoWeekOf(instant) {
  const cr = new Date(instant.getTime() - CR_OFFSET_MS); // CR wall clock, read as UTC fields
  const t = new Date(Date.UTC(cr.getUTCFullYear(), cr.getUTCMonth(), cr.getUTCDate()));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const yearStart = Date.UTC(t.getUTCFullYear(), 0, 1);
  const week = Math.ceil(((t - yearStart) / DAY_MS + 1) / 7);
  return `${t.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}

/** The `n` ISO weeks before the week that contains `instant`, oldest first (the reported week itself is not in it). */
function trailingWeeks(instant, n = HISTORY_WEEKS) {
  const out = [];
  for (let i = n; i >= 1; i--) out.push(isoWeekOf(new Date(instant.getTime() - i * 7 * DAY_MS)));
  return out;
}

/** ISO weeks whose Monday 00:00 CR falls inside [start, end). */
function weeksStartingIn(start, end) {
  const out = [];
  for (let t = start.getTime(); t < end.getTime(); t += DAY_MS) {
    const d = new Date(t);
    if ((new Date(t - CR_OFFSET_MS)).getUTCDay() === 1) out.push(isoWeekOf(d));
  }
  return out;
}

// ── numbers ──────────────────────────────────────────────────────────────────
const positive = (v) => Number.isFinite(v) && v > 0;

function median(values) {
  const v = values.filter(positive).sort((a, b) => a - b);
  if (!v.length) return null;
  const mid = Math.floor(v.length / 2);
  return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

/**
 * The median the multiples are measured against.
 *   basis    'views' | 'reach': the field the report has for its reels.
 *   batch    the reels of the period, each with `views` and `reach` (null when unknown).
 *   history  reel_stats rows (kind 'reel') of the trailing weeks, or null when unread.
 * History wins; when it holds fewer than MEDIAN_FLOOR reels the median falls back to the
 * batch (source 'batch'). Below the floor on both, median is null and no reel gets a
 * multiple. `n` is the count the median rests on; `historyN` says how thin the history was.
 */
function medianBase(basis, batch, history) {
  const hist = (Array.isArray(history) ? history : []).filter((r) => r && r.kind === 'reel').map((r) => Number(r[basis]));
  const histN = hist.filter(positive).length;
  if (histN >= MEDIAN_FLOOR) return { basis, median: median(hist), n: histN, source: 'history', historyN: histN };
  const own = (batch || []).map((p) => Number(p[basis]));
  const ownN = own.filter(positive).length;
  const historyN = Array.isArray(history) ? histN : null; // null = reel_stats could not be read
  if (ownN >= MEDIAN_FLOOR) return { basis, median: median(own), n: ownN, source: 'batch', historyN };
  return { basis, median: null, n: Math.max(ownN, histN), source: 'none', historyN };
}

/** views (or reach) over the median, one decimal; null when either side is missing. */
function multiple(value, base) {
  if (!base || !positive(base.median) || !positive(value)) return null;
  return Math.round((value / base.median) * 10) / 10;
}

/** shares per 1,000 reached, one decimal; null when reach is unknown or zero. */
function sendsPer1k(shares, reach) {
  if (!positive(reach) || !Number.isFinite(shares)) return null;
  return Math.round((shares / reach) * 10000) / 10;
}

const fmtMultiple = (x) => (x == null ? 's/d' : `${x.toFixed(1)}x`);
const fmtPer1k = (x) => (x == null ? 's/d' : x.toFixed(1));

/**
 * Adds `multiple` and `sendsPer1k` to each reel. `basis` picks the field for the
 * multiple; reels missing it get null (printed as s/d, never 0).
 */
function score(reels, base) {
  return reels.map((p) => ({
    ...p,
    multiple: multiple(Number(p[base.basis]), base),
    sendsPer1k: sendsPer1k(Number(p.shares), Number(p.reach)),
  }));
}

/** Nulls last, then descending. */
const desc = (a, b) => {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  return b - a;
};
const engagementOf = (p) => (p.likes || 0) + (p.comments || 0) + (p.shares || 0) + (p.saved || 0);

/** Multiple first, sends per reach second, then reach, then engagement. Returns a sorted copy. */
function rank(scored) {
  return [...scored].sort((a, b) => desc(a.multiple, b.multiple) || desc(a.sendsPer1k, b.sendsPer1k)
    || desc(positive(a.reach) ? a.reach : null, positive(b.reach) ? b.reach : null) || (engagementOf(b) - engagementOf(a)));
}

/**
 * Which key the ranking actually used, so the header can name it and the
 * contract can check the header against the table. 'múltiplo' when any reel
 * has one, else 'envíos' when any has sends per reach, else 'alcance', else
 * 'interacciones'.
 */
function sortKey(scored) {
  if (scored.some((p) => p.multiple != null)) return 'múltiplo';
  if (scored.some((p) => p.sendsPer1k != null)) return 'envíos';
  if (scored.some((p) => positive(p.reach))) return 'alcance';
  return 'interacciones';
}

/** Header fragment per sort key; the table's first metric column must match (see COLUMN_FOR_KEY). */
const HEADER_FOR_KEY = {
  'múltiplo': 'múltiplo, luego env/1k',
  'envíos': 'env/1k (sin mediana)',
  'alcance': 'alcance',
  'interacciones': 'interacciones (likes + comentarios + compartidos)',
};
const COLUMN_FOR_KEY = { 'múltiplo': 'xMed', 'envíos': 'Env/1k', 'alcance': 'Alcance', 'interacciones': 'Likes' };

/**
 * One footnote line explaining xMed and Env/1k for this report, with the n the
 * median rests on. `period` is the phrase for the batch fallback: 'esta semana' / 'este mes'.
 */
function footnote(base, period = 'este periodo', { sends = true } = {}) {
  const what = base.basis === 'views' ? 'vistas' : 'alcance';
  const env = sends ? ' Env/1k = compartidos por cada 1,000 alcanzados.' : '';
  const why = base.historyN == null ? 'reel_stats ilegible' : `historial corto: ${base.historyN} reels en reel_stats, mínimo ${MEDIAN_FLOOR}`;
  if (base.source === 'history') return `_xMed = ${what} del reel / mediana de ${what} de las ${HISTORY_WEEKS} semanas anteriores (n=${base.n}).${env}_`;
  if (base.source === 'batch') return `_xMed = ${what} del reel / mediana de ${period} (${why}).${env}_`;
  return `_xMed: s/d, la mediana necesita al menos ${MEDIAN_FLOOR} reels con ${what} (hay ${base.n}).${env}_`;
}

// ── verdict (ig-audit: views but no follows = profile; no views = hook) ──────
/**
 * reels         scored reels of the period (multiple may be null)
 * reach         the period's reach (account total for the week, reel total for the month); null when unknown
 * prevReach     same for the previous period; null when unknown
 * newFollowers  new followers in the period; null when unknown
 * Returns { kind: 'floor' | 'perfil' | 'gancho' | 'ok' | 'none', ...facts } for formatVerdict.
 */
function verdict({ reels, reach, prevReach, newFollowers }) {
  const withReach = reels.filter((p) => positive(p.reach));
  if (withReach.length < VERDICT_FLOOR) return { kind: 'floor', n: withReach.length };
  const multiples = reels.map((p) => p.multiple).filter((x) => x != null);
  const best = multiples.length ? Math.max(...multiples) : null;
  const hit = best != null && best >= HIT_MULTIPLE;
  const followsPer1k = Number.isFinite(newFollowers) && positive(reach) ? Math.round((newFollowers / reach) * 10000) / 10 : null;
  const deltaPct = positive(prevReach) && Number.isFinite(reach) ? Math.round(((reach - prevReach) / prevReach) * 100) : null;
  const facts = { best, followsPer1k, deltaPct, newFollowers: Number.isFinite(newFollowers) ? newFollowers : null };
  if (hit && followsPer1k != null && followsPer1k < LOW_FOLLOWS_PER_1K) return { kind: 'perfil', ...facts };
  if (hit) return { kind: 'ok', ...facts };
  if (deltaPct != null && deltaPct <= FLAT_REACH_PCT) return { kind: 'gancho', ...facts };
  if (best != null) return { kind: 'gancho', ...facts };            // multiples known, none travelled
  if (deltaPct != null) return { kind: 'ok', ...facts };              // reach grew, no multiples to judge
  return { kind: 'none', ...facts };
}

const fmtPct = (d) => (d == null ? 's/d' : `${d > 0 ? '+' : ''}${d}%`);

/** The one-line verdict, Spanish, no em dash. `prevWord` names the previous period ("la semana anterior"). */
function formatVerdict(v, prevWord = 'el periodo anterior') {
  if (!v) return null;
  const follows = v.followsPer1k == null ? 's/d' : `${fmtPer1k(v.followsPer1k)} por 1,000 alcanzados`;
  switch (v.kind) {
    case 'floor': return `_Veredicto: s/d (hacen falta ${VERDICT_FLOOR} reels con alcance; hay ${v.n})._`;
    case 'perfil': return `*Veredicto:* problema de perfil, no de gancho. El mejor reel hizo ${fmtMultiple(v.best)} la mediana y la cuenta sumó pocos seguidores (${follows}).`;
    case 'gancho': return `*Veredicto:* problema de gancho. Alcance ${v.deltaPct == null ? 'sin despegue' : `plano (${fmtPct(v.deltaPct)} vs ${prevWord})`} y ningún reel pasó de ${HIT_MULTIPLE}x la mediana (mejor: ${fmtMultiple(v.best)}).`;
    case 'ok': return `*Veredicto:* sin alarma. Mejor reel ${fmtMultiple(v.best)} la mediana, alcance ${fmtPct(v.deltaPct)} vs ${prevWord}, seguidores nuevos ${follows}.`;
    default: return `_Veredicto: s/d (sin alcance de ${prevWord} ni mediana para comparar)._`;
  }
}

module.exports = {
  HISTORY_WEEKS, MEDIAN_FLOOR, VERDICT_FLOOR, HIT_MULTIPLE, LOW_FOLLOWS_PER_1K, FLAT_REACH_PCT,
  isoWeekOf, trailingWeeks, weeksStartingIn, median, medianBase, multiple, sendsPer1k, fmtMultiple, fmtPer1k,
  score, rank, sortKey, HEADER_FOR_KEY, COLUMN_FOR_KEY, footnote, verdict, formatVerdict,
};
