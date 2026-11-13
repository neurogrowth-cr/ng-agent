// Setter Scorecard (Ron, 2026-11-12): with one setter, a ranking says nothing.
// The Tue + Sat board compares Sebastián with his pay targets and his own last
// week instead of with other setters. Pure: no I/O, so test/setter-scorecard.test.js
// can drive it directly. index.js (runSetterScorecard) gathers the rows.
//
// Pay rules, from the Playbook ("Flywheel - Sales Team Kit" > Setter Material):
//   AQC = held call with a complete FICHA DE TRIAGE, whoever booked it (2026-11-12).
//   Monthly tiers at 15 / 25 / 40 AQCs; show rate of at least 60% or no bonus;
//   AQCs closing at 25%+ multiply the bonus by 1.25, at 30%+ by 1.5.
//   Condition since 2026-11-12: Max's signal cards answered within 15 minutes,
//   08:00 to 20:00 CR.
// Dollar amounts are never printed: the board posts to a team channel.

const TIERS = [15, 25, 40];
const SHOW_GATE = 0.6;
const MULTIPLIERS = [{ at: 0.30, label: 'x1.5' }, { at: 0.25, label: 'x1.25' }];
const SIGNAL_SLA_MIN = 15;
const DAY_START_H = 8;
const DAY_END_H = 20;
const CR_OFFSET_MS = 6 * 3600 * 1000; // Costa Rica is UTC-6, no DST
const MESES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];

const pct = (n, d) => (d > 0 ? Math.round((n / d) * 100) : null);

function median(xs) {
  const s = (xs || []).filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  if (!s.length) return null;
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : Math.round((s[m - 1] + s[m]) / 2);
}

// Where the 15-minute clock starts for a card: its creation time inside working
// hours, otherwise the next 08:00 CR (a card at 21:30 is due 08:15 next day).
function slaClockStart(createdMs) {
  const cr = new Date(createdMs - CR_OFFSET_MS);
  const h = cr.getUTCHours();
  if (h >= DAY_START_H && h < DAY_END_H) return createdMs;
  const day = Date.UTC(cr.getUTCFullYear(), cr.getUTCMonth(), cr.getUTCDate()) + CR_OFFSET_MS;
  return day + (h >= DAY_END_H ? 24 : 0) * 3600 * 1000 + DAY_START_H * 3600 * 1000;
}

// Signal cards (kind 'signal', shown to Sebastián in draft mode). Answered = he
// marked it handled (✅); the guardian stamps approved_at then. A card still
// inside its 15 minutes is not judged yet.
function signalSla(rows, now) {
  let onTime = 0, judged = 0;
  const minutes = [];
  for (const r of rows || []) {
    if (r.kind !== 'signal' || r.mode !== 'draft') continue;
    const created = Date.parse(r.created_at);
    if (!Number.isFinite(created)) continue;
    const start = slaClockStart(created);
    const deadline = start + SIGNAL_SLA_MIN * 60000;
    const handledAt = r.status === 'handled' && r.approved_at ? Date.parse(r.approved_at) : null;
    if (handledAt == null && now < deadline) continue;
    judged++;
    if (handledAt != null) {
      const m = Math.max(0, Math.round((handledAt - start) / 60000));
      minutes.push(m);
      if (handledAt <= deadline) onTime++;
    }
  }
  return { onTime, judged, medianMinutes: median(minutes) };
}

// Reply and strike drafts Sebastián saw (draft mode only; shadow rows went to Ron).
function draftStats(rows) {
  const out = { asIs: 0, edited: 0, expired: 0, rejected: 0, waiting: 0 };
  for (const r of rows || []) {
    if (r.mode !== 'draft' || !(r.kind === 'reply' || r.kind === 'strike')) continue;
    if (r.status === 'sent') { if (r.edited) out.edited++; else out.asIs++; }
    else if (r.status === 'expired') out.expired++;
    else if (r.status === 'rejected') out.rejected++;
    else if (r.status === 'drafted') out.waiting++;
  }
  return out;
}

// Highest tier reached (0 = none) and the gap to the next one.
function tierProgress(aqc) {
  let tier = 0;
  for (const t of TIERS) if (aqc >= t) tier++;
  const next = TIERS[tier] ?? null;
  return { tier, next, toGo: next == null ? 0 : next - aqc };
}

function multiplierFor(closeRate) {
  if (closeRate == null) return null;
  const m = MULTIPLIERS.find((x) => closeRate >= x.at);
  return m ? m.label : null;
}

function showRateOf(s) {
  const decided = (s.attended || 0) + (s.no_shows || 0);
  return { rate: decided ? s.attended / decided : null, decided };
}

function vsLine(label, cur, prev, fmt = (x) => String(x)) {
  return `${label} ${cur == null ? '-' : fmt(cur)} (antes ${prev == null ? '-' : fmt(prev)})`;
}

// data: { now, mtd, week, prevWeek, sla, drafts, backlog }
//   mtd       setter slot for the month: held_with_ficha, won_with_ficha, attended,
//             no_shows, pending, ficha_incomplete, ficha_unchecked
//   week      slot for the last 7 days; prevWeek the 7 before (no ficha needed)
//   sla       signalSla() result; drafts draftStats() result; null = unreadable
//   backlog   { tagged, replied, booked, struckOut } or null when unreadable
function formatScorecard({ name = 'Sebastián', now, mtd, week, prevWeek, sla, drafts, backlog }) {
  const cr = new Date(now - CR_OFFSET_MS);
  const day = cr.getUTCDate();
  const daysInMonth = new Date(Date.UTC(cr.getUTCFullYear(), cr.getUTCMonth() + 1, 0)).getUTCDate();
  const lines = [`*SCORECARD DE ${name.toUpperCase()}* · ${MESES[cr.getUTCMonth()]}, día ${day} de ${daysInMonth}`];

  const aqc = mtd.held_with_ficha || 0;
  const { tier, next, toGo } = tierProgress(aqc);
  const show = showRateOf(mtd);
  const closeRate = aqc ? (mtd.won_with_ficha || 0) / aqc : null;
  const mult = multiplierFor(closeRate);
  lines.push('', '*Bono del mes*');
  const tierStr = next == null
    ? `nivel ${tier} de ${TIERS.length}, el más alto`
    : `faltan ${toGo} para el nivel ${tier + 1} (${next})${tier ? `, ya en nivel ${tier}` : ''}`;
  lines.push(`AQC (llamada realizada con ficha completa): *${aqc}*, ${tierStr}`);
  if (show.rate == null) {
    lines.push(`Show rate: sin datos aún${mtd.pending ? ` (${mtd.pending} llamada(s) esperan el resultado del closer)` : ''}`);
  } else {
    const ok = show.rate >= SHOW_GATE;
    lines.push(`Show rate: ${pct(mtd.attended, show.decided)}% (${mtd.attended} de ${show.decided}). Mínimo 60%: ${ok ? '✅' : '❌ sin bono por ahora'}`);
  }
  if (closeRate != null) {
    lines.push(`Cierre de tus AQC: ${mtd.won_with_ficha || 0} de ${aqc} (${pct(mtd.won_with_ficha || 0, aqc)}%). ${mult ? `Multiplicador ${mult}` : 'Multiplicador desde 25%'}`);
  }
  const fichaGaps = [
    mtd.ficha_incomplete ? `${mtd.ficha_incomplete} con un claro en blanco` : '',
    mtd.ficha_unchecked ? `${mtd.ficha_unchecked} sin revisar (GHL no respondió)` : '',
  ].filter(Boolean);
  if (fichaGaps.length) lines.push(`Fichas que no cuentan: ${fichaGaps.join(', ')}`);

  lines.push('', '*Señales de Max contestadas en 15 min* (8:00 a 20:00)');
  if (sla === null) lines.push('No se pudo leer hoy');
  else if (!sla || !sla.judged) lines.push('Ninguna señal este mes todavía');
  else lines.push(`${sla.onTime} de ${sla.judged} (${pct(sla.onTime, sla.judged)}%)${sla.medianMinutes != null ? `, mediana ${sla.medianMinutes} min` : ''}`);

  if (drafts === null) lines.push('', '*Borradores de Max este mes*', 'No se pudo leer hoy');
  else if (drafts) {
    const total = drafts.asIs + drafts.edited + drafts.expired + drafts.rejected;
    lines.push('', '*Borradores de Max este mes*');
    lines.push(total
      ? `Enviados tal cual ${drafts.asIs}, editados ${drafts.edited}, vencidos ${drafts.expired}, descartados ${drafts.rejected}${drafts.waiting ? `, esperando ${drafts.waiting}` : ''}`
      : 'Ninguno todavía');
  }

  lines.push('', '*Reactivación del backlog este mes*');
  if (!backlog) lines.push('No se pudo leer hoy');
  else if (!backlog.tagged) lines.push('Nadie contactado todavía');
  else lines.push(`Contactados ${backlog.tagged}, respondieron ${backlog.replied} (${pct(backlog.replied, backlog.tagged)}%), llamadas agendadas ${backlog.booked}, cerrados a Lost ${backlog.struckOut}`);

  const wShow = showRateOf(week), pShow = showRateOf(prevWeek);
  const asPct = (r) => `${Math.round(r * 100)}%`;
  lines.push('', '*Últimos 7 días vs los 7 anteriores*');
  lines.push([
    vsLine('Leads tomados', week.leads_claimed || 0, prevWeek.leads_claimed || 0),
    vsLine('llamadas agendadas', week.calls_booked || 0, prevWeek.calls_booked || 0),
    vsLine('show rate', wShow.rate, pShow.rate, asPct),
  ].join(' · '));
  return lines.join('\n');
}

module.exports = { TIERS, SHOW_GATE, SIGNAL_SLA_MIN, slaClockStart, signalSla, draftStats, tierProgress, multiplierFor, formatScorecard };
