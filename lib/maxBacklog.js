'use strict';
// Max self-improvement backlog (Ron, 2026-10-24). Max writes down what he
// could not do so Ron can ship the fixes once a month with /max-backlog.
// Recipe: ~/automations/ops/recipes/max-backlog-digest.md.
//
// Sources (observed only, no speculative ideas): Max's own replies that say he
// lacks a tool, access or permission (regex gate, then Haiku decides), tools
// that threw, BLOCKED tool results, failed crons, saved corrections. Rows live
// in max_backlog (migration 023), deduped by slug.
//
// Pure (no Slack, no network, no Supabase), tested in test/max-backlog.test.js.
// The I/O half is in index.js: recordBacklogItem, captureGapFromReply,
// syncMaxBacklogFromActivity, runMaxBacklogDigest.

const KINDS = ['capability_gap', 'policy_block', 'tool_error', 'check_broken', 'correction'];
const SLUG_PREFIX = {
  capability_gap: 'gap',
  policy_block: 'blocked',
  tool_error: 'tool_error',
  check_broken: 'check_broken',
  correction: 'correction',
};
const KIND_LABEL_ES = {
  capability_gap: 'me falta la herramienta',
  policy_block: 'bloqueado por regla',
  tool_error: 'herramienta falló',
  check_broken: 'chequeo falló',
  correction: 'corrección',
};
const DIGEST_TOP = 5;
const DIGEST_QUEUE_WARN = 25;   // open items above this = the queue grows faster than it closes
const TITLE_MAX = 110;
const SUMMARY_MAX = 220;
const CR_OFFSET_MS = 6 * 60 * 60 * 1000;   // Costa Rica is UTC-6, no DST
const MONTHS_ES = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO',
  'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'];
const MONTHS_EN_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

// ── Gate ───────────────────────────────────────────────────────────────────
// Cheap first pass on Max's own reply. Broad on purpose: in the 30 days before
// 2026-10-26 only 3 of 1,173 outbound replies used any of these phrasings, so a
// false positive costs one Haiku call, and Haiku makes the real decision.
const GAP_HINT_RE = new RegExp([
  'no tengo (la |una |el )?(herramienta|acceso|autorizaci[oó]n|forma|permiso|manera|capacidad)',
  'no puedo (eliminar|borrar|remover|modificar|crear|enviar|acceder|cambiar|editar|mover|leer|ver|sobrescribir|actualizar|agendar|programar|subir|descargar|publicar|marcar|asignar)',
  'no cuento con',
  'fuera de mi alcance',
  'no (me )?es posible',
  'no est[aá] (entre mis|a mi alcance|en mis herramientas)',
  'sin acceso a',
  'no tengo c[oó]mo',
  "i (can'?t|cannot|am unable to|am not able to) (delete|remove|edit|access|send|create|update|change|read|see|overwrite|schedule|upload|post|assign|confirm)",
  "i don'?t have (a tool|access|permission|the ability|a way)",
  'outside (of )?my (tools|access|scope)',
].join('|'), 'i');

function looksLikeGap(replyText) {
  return GAP_HINT_RE.test(String(replyText || ''));
}

// ── Text hygiene ───────────────────────────────────────────────────────────
// Rows land in #ng-pm-agent (Ron + ops). Strip what could identify a lead and
// any long number (amounts, phones, ids); a capability description needs none.
function scrub(text, max) {
  let t = String(text == null ? '' : text)
    .replace(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g, '[email]')
    .replace(/\+?\d[\d\s().-]{5,}\d/g, '[número]')
    .replace(/[\u2014\u2013]/g, ',')          // no em or en dashes (Ron's rule)
    .replace(/[`*_~]/g, '')                   // no Slack formatting inside fields
    .replace(/\s+/g, ' ')
    .trim();
  if (max && t.length > max) t = t.slice(0, max - 1).trimEnd() + '…';
  return t;
}

function slugify(kind, topic) {
  const base = String(topic || '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase().replace(/[^a-z0-9:_]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 60);
  const prefix = SLUG_PREFIX[kind] || 'gap';
  return base ? `${prefix}:${base}` : null;
}

// ── Haiku verdict ──────────────────────────────────────────────────────────
function buildGapPrompt({ userText, replyText, openItems = [] }) {
  const known = openItems.slice(0, 30)
    .map(o => `- ${o.slug}: ${scrub(o.title, 90)}`).join('\n') || '(none)';
  return `Max is an internal Slack ops agent. A teammate asked him something and Max replied.

Teammate:
${String(userText || '').slice(0, 800)}

Max replied:
${String(replyText || '').slice(0, 1500)}

Did Max say he could NOT do what was asked because he lacks a tool, data access, or permission?
- capability_gap: no tool or no access exists for it (he would need new code).
- policy_block: a deliberate rule stops him (Ron-only, first-writer-wins, approval required).
- none: he did it, he only asked a clarifying question, the data simply was not there, or the refusal was about something else.

Already in the backlog (reuse the exact slug when it is the same capability):
${known}

Respond with JSON only, no markdown fences:
{"kind":"capability_gap|policy_block|none","slug":"<an existing slug from the list, or empty>","topic":"<2-5 word kebab-case verb-object-system, e.g. eliminar-contacto-ghl>","title":"<Spanish, under 90 chars, the capability, e.g. Eliminar contactos de GHL>","summary":"<Spanish, one sentence: what was asked and why Max could not>"}
Never include names of leads or clients, emails, phone numbers, amounts, or financial figures in topic, title or summary.`;
}

function parseGapJson(raw, knownSlugs = []) {
  let parsed;
  try {
    parsed = JSON.parse(String(raw || '').trim().replace(/^```(?:json)?\s*|\s*```$/g, ''));
  } catch (_) { return null; }
  if (!parsed || typeof parsed !== 'object') return null;
  const kind = parsed.kind;
  if (kind !== 'capability_gap' && kind !== 'policy_block') return null;
  const title = scrub(parsed.title, TITLE_MAX);
  if (!title) return null;
  const reuse = typeof parsed.slug === 'string' && knownSlugs.includes(parsed.slug) ? parsed.slug : null;
  const slug = reuse || slugify(kind, parsed.topic || title);
  if (!slug) return null;
  return { kind, slug, title, summary: scrub(parsed.summary, SUMMARY_MAX) || null };
}

// ── Merge ──────────────────────────────────────────────────────────────────
// Every item carries `times` (ISO strings of the events it stands for). Only
// events newer than the row's last_seen count, so the weekly sync can re-read
// a window it already read without double counting.
function newRow(item, nowIso) {
  const times = (item.times && item.times.length ? item.times : [nowIso]).slice().sort();
  return {
    slug: item.slug,
    kind: item.kind,
    title: scrub(item.title, TITLE_MAX),
    summary: item.summary ? scrub(item.summary, SUMMARY_MAX) : null,
    requested_by: item.requested_by || null,
    channel: item.channel || null,
    permalink: item.permalink || null,
    occurrences: times.length,
    first_seen: times[0],
    last_seen: times[times.length - 1],
    status: 'open',
    correlation_id: item.correlation_id || null,
    updated_at: nowIso,
  };
}

function mergeOccurrence(existing, item, nowIso) {
  const lastSeen = Date.parse(existing.last_seen) || 0;
  const times = (item.times && item.times.length ? item.times : [nowIso])
    .filter(t => Date.parse(t) > lastSeen).sort();
  if (!times.length) return null;
  const patch = {
    occurrences: (Number(existing.occurrences) || 0) + times.length,
    last_seen: times[times.length - 1],
    updated_at: nowIso,
  };
  if (!existing.permalink && item.permalink) patch.permalink = item.permalink;
  if (!existing.summary && item.summary) patch.summary = scrub(item.summary, SUMMARY_MAX);
  if (existing.status === 'done') {
    // The fix did not hold. Reopen and say so; a dropped row stays dropped.
    patch.status = 'open';
    patch.notes = `${existing.notes ? `${existing.notes} | ` : ''}reabierto ${nowIso.slice(0, 10)}`;
  }
  return patch;
}

// ── Mappers for the weekly sync ────────────────────────────────────────────
// agent_activity rows (portal project), event_type cron_run or audit with
// status error or degraded, grouped by action.
function activityRowsToItems(rows) {
  const byAction = new Map();
  for (const r of rows || []) {
    if (!r || !r.action || !r.created_at) continue;
    if (r.status !== 'error' && r.status !== 'degraded') continue;
    if (!byAction.has(r.action)) byAction.set(r.action, []);
    byAction.get(r.action).push(r);
  }
  const items = [];
  for (const [action, list] of byAction) {
    list.sort((a, b) => String(a.created_at).localeCompare(String(b.created_at)));
    const latest = list[list.length - 1];
    items.push({
      kind: 'check_broken',
      slug: slugify('check_broken', action),
      title: scrub(`${action} falló`, TITLE_MAX),
      summary: latest.error_message ? scrub(latest.error_message, SUMMARY_MAX) : `estado ${latest.status}`,
      times: list.map(r => new Date(r.created_at).toISOString()),
    });
  }
  return items;
}

// agent_knowledge rows with source correction or report-feedback. Keys are
// correction:<topic>:<date> and report_lesson:<scope>:<date>.
function lessonRowsToItems(rows) {
  const items = [];
  for (const r of rows || []) {
    if (!r || !r.key || !r.updated_at) continue;
    const parts = String(r.key).split(':');
    let topic = null;
    if (parts[0] === 'correction' && parts[1]) topic = parts[1];
    else if (parts[0] === 'report_lesson' && parts.length >= 3) topic = `reporte-${parts.slice(1, -1).join('-')}`;
    if (!topic) continue;
    const firstSentence = String(r.value || '').replace(/^#+\s*\w*\s*/m, '').split(/(?<=[.!?])\s/)[0];
    items.push({
      kind: 'correction',
      slug: slugify('correction', topic),
      title: scrub(`Corrección: ${topic.replace(/-/g, ' ')}`, TITLE_MAX),
      summary: scrub(firstSentence, SUMMARY_MAX) || null,
      requested_by: r.user_id || null,
      times: [new Date(r.updated_at).toISOString()],
    });
  }
  return items;
}

// ── Ranking and dates ──────────────────────────────────────────────────────
function rankOpen(rows, limit = DIGEST_TOP) {
  return (rows || [])
    .filter(r => r && r.status !== 'done' && r.status !== 'dropped')
    .slice()
    .sort((a, b) => (Number(b.occurrences) || 0) - (Number(a.occurrences) || 0)
      || String(b.last_seen || '').localeCompare(String(a.last_seen || '')))
    .slice(0, limit);
}

function crDate(date) {
  return new Date(new Date(date).getTime() - CR_OFFSET_MS);
}

function isFirstMondayCR(date) {
  const d = crDate(date);
  return d.getUTCDay() === 1 && d.getUTCDate() <= 7;
}

function monthLabelCR(date) {
  const d = crDate(date);
  return `${MONTHS_ES[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
}

// The next first Monday strictly after the CR calendar day of `date`.
function nextFirstMondayCR(date) {
  const d = crDate(date);
  const day = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  for (let i = 1; i <= 40; i++) {
    const c = new Date(day.getTime() + i * 86400000);
    if (c.getUTCDay() === 1 && c.getUTCDate() <= 7) return c;
  }
  return null;
}

// ── Formatting ─────────────────────────────────────────────────────────────
function formatDigest({ rows, openTotal, monthLabel, ronId }) {
  const top = rankOpen(rows, DIGEST_TOP);
  const total = Number.isFinite(openTotal) ? openTotal : top.length;
  const lines = [
    `\`MAX BACKLOG: ${monthLabel}\`${ronId ? ` <@${ronId}>` : ''}`,
    `Lo que no pude hacer, ordenado por cuántas veces pasó. ${total} abierto${total === 1 ? '' : 's'} en total.`,
    '',
  ];
  top.forEach((r, i) => {
    const n = Number(r.occurrences) || 1;
    const bits = [`*${scrub(r.title, TITLE_MAX)}*`, KIND_LABEL_ES[r.kind] || r.kind, `${n} ${n === 1 ? 'vez' : 'veces'}`];
    if (r.requested_by) bits.push(`pidió <@${r.requested_by}>`);
    if (r.permalink) bits.push(`<${r.permalink}|hilo>`);
    lines.push(`${i + 1}. ${bits.join(' · ')}`);
    if (r.summary) lines.push(`    ${scrub(r.summary, SUMMARY_MAX)}`);
  });
  if (total > top.length) lines.push(`…y ${total - top.length} más.`);
  if (total > DIGEST_QUEUE_WARN) lines.push(`⚠️ La cola crece más rápido de lo que se cierra (${total} abiertos).`);
  lines.push('', 'Para planear el mes: /max-backlog en Claude Code.');
  return lines.join('\n');
}

// Recipe §4a. Checks the template, not the free text inside titles and
// summaries (an error message can legitimately contain the word "null").
const ITEM_LINE = /^\d+\. \*[^*]+\* · .+ · \d+ (vez|veces)( · .+)?$/;
function validateDigest(text) {
  const problems = [];
  const lines = String(text || '').split('\n');
  if (!/^`MAX BACKLOG: [A-ZÁÉÍÓÚÑ]+ \d{4}`/.test(lines[0] || '')) problems.push('header missing');
  const items = lines.filter(l => /^\d+\. /.test(l));
  if (!items.length) problems.push('no numbered items');
  if (items.length > DIGEST_TOP) problems.push(`more than ${DIGEST_TOP} items`);
  for (const l of items) {
    if (!ITEM_LINE.test(l)) problems.push(`malformed item line: ${l.slice(0, 60)}`);
    const frame = l.replace(/\*[^*]*\*/, '*T*');
    if (/\bundefined\b|\bnull\b|\bNaN\b|\[object Object\]/.test(frame)) problems.push(`placeholder in item line: ${l.slice(0, 60)}`);
  }
  if (/[\u2014]/.test(text)) problems.push('contains an em dash');
  return { ok: problems.length === 0, problems };
}

// One line for the Monday weekly brief (#ng-ops-management). Counts only: the
// titles stay in #ng-pm-agent.
function formatBriefLine({ newThisWeek, openTotal, nextDigest }) {
  const n = Number(newThisWeek) || 0;
  const open = Number(openTotal) || 0;
  const next = nextDigest ? ` Next digest ${MONTHS_EN_SHORT[nextDigest.getUTCMonth()]} ${nextDigest.getUTCDate()} in #ng-pm-agent.` : '';
  if (!n && !open) return `• Nothing logged, nothing open.${next}`;
  return `• ${n ? `${n} new gap${n === 1 ? '' : 's'} logged this week` : 'No new gaps this week'}, ${open} open.${next}`;
}

module.exports = {
  KINDS, KIND_LABEL_ES, DIGEST_TOP, DIGEST_QUEUE_WARN,
  looksLikeGap, scrub, slugify, buildGapPrompt, parseGapJson,
  newRow, mergeOccurrence, activityRowsToItems, lessonRowsToItems,
  rankOpen, isFirstMondayCR, monthLabelCR, nextFirstMondayCR,
  formatDigest, validateDigest, formatBriefLine,
};
