// Kai escalations digest. Recipe: ~/automations/ops/recipes/kai-escalations-digest.md
//
// Kai (ng-automation, the LinkedIn reply agent) hands a reply to a human when
// it cannot answer (status 'escalated') and parks a draft for the client's
// approval (status 'drafted'). On 2026-11-03 the audit found 365 escalations
// and 118 drafts never sent, 89 of the drafts older than 48 hours, and no
// Slack message for any escalated row: nobody owned the queue. From that date
// the fulfillment role (Valeria) owns it, and this module gives her the
// backlog per client every weekday morning. Pure logic lives here so it is
// testable without booting the Slack app; index.js owns the cron and the DM.
//
// Read-only: every query runs on portalPg (role max_readonly). Nothing here
// sends, approves, edits or expires a reply.
//
// Only what still needs a person (Ron, 2026-11-04): a prospect's no is never a
// human's job (Kai closes those, ng-automation lib/decline-close.js) and a thread
// the client already answered is done. Older rows still carry 'escalated' on both.

const { notDemoWorkspace } = require('./demoClient');

const CR_TZ = 'America/Costa_Rica';
// Rows before this date are the pre-owner backlog: reported as one number,
// never listed per client, so the first digest is not 365 lines long.
const DEFAULT_SINCE = '2026-10-01';
const STALE_HOURS = 48;
const DAY_MS = 24 * 3600 * 1000;


const STILL_NEEDS_A_PERSON = `
       AND COALESCE(q.classified_intent, '') <> 'unsubscribe'
       AND q.client_answered_at IS NULL
       AND q.customer_handled_at IS NULL
       AND COALESCE(q.thread_state, '') <> 'answered'`;

const SQL = {
  // One row per workspace and status: items never sent, created on or after
  // the cutoff, with how many have waited longer than the stale window.
  backlog: `
    SELECT q.workspace_id,
           COALESCE(NULLIF(trim(w.client_name), ''), 'UNKNOWN CLIENT') AS client_name,
           COALESCE(w.paused_by_customer, false) AS paused,
           COALESCE(w.auto_send_enabled, false) AS auto_send,
           q.status,
           count(*)::int AS n,
           count(*) FILTER (WHERE q.created_at < $1::timestamptz - make_interval(hours => $3::int))::int AS stale,
           min(q.created_at) AS oldest
      FROM prosp_reply_queue q
      LEFT JOIN workspaces w ON w.id = q.workspace_id
     WHERE q.status IN ('drafted', 'escalated')
       AND q.sent_at IS NULL
       AND q.created_at >= $2::timestamptz
       AND q.created_at <= $1::timestamptz${STILL_NEEDS_A_PERSON}
       AND ${notDemoWorkspace('q.workspace_id')}
     GROUP BY 1, 2, 3, 4, 5`,
  // Everything older than the cutoff, as one number.
  legacy: `
    SELECT count(*)::int AS n,
           count(DISTINCT q.workspace_id)::int AS clients
      FROM prosp_reply_queue q
     WHERE q.status IN ('drafted', 'escalated')
       AND q.sent_at IS NULL
       AND q.created_at < $1::timestamptz${STILL_NEEDS_A_PERSON}
       AND ${notDemoWorkspace('q.workspace_id')}`,
};

async function readKaiEscalations(pg, now, { since = DEFAULT_SINCE, staleHours = STALE_HOURS } = {}) {
  const [backlog, legacy] = await Promise.all([
    pg.query(SQL.backlog, [now.toISOString(), since, staleHours]),
    pg.query(SQL.legacy, [since]),
  ]);
  return { now, since, staleHours, rows: backlog.rows, legacy: legacy.rows[0] || { n: 0, clients: 0 } };
}

function daysAgo(ts, now) {
  if (!ts) return null;
  const t = new Date(ts).getTime();
  if (Number.isNaN(t)) return null;
  return Math.max(0, Math.floor((now.getTime() - t) / DAY_MS));
}

function crDateLabel(date) {
  return new Intl.DateTimeFormat('es-CR', { timeZone: CR_TZ, weekday: 'long', day: 'numeric', month: 'long' }).format(date);
}

// Groups the SQL rows per client and keeps only what a human can act on:
// escalated and drafted counts, how many are stale, the oldest age.
function summarizeKaiEscalations(data) {
  const now = data.now instanceof Date ? data.now : new Date(data.now);
  const byWs = new Map();
  for (const r of data.rows || []) {
    const key = r.workspace_id || r.client_name;
    if (!byWs.has(key)) {
      byWs.set(key, {
        workspace_id: r.workspace_id, client_name: String(r.client_name || 'UNKNOWN CLIENT').trim().toUpperCase(),
        paused: !!r.paused, auto_send: !!r.auto_send,
        escalated: { n: 0, stale: 0, oldestDays: null },
        drafted: { n: 0, stale: 0, oldestDays: null },
      });
    }
    const c = byWs.get(key);
    const bucket = r.status === 'escalated' ? c.escalated : c.drafted;
    bucket.n += Number(r.n) || 0;
    bucket.stale += Number(r.stale) || 0;
    const age = daysAgo(r.oldest, now);
    if (age !== null && (bucket.oldestDays === null || age > bucket.oldestDays)) bucket.oldestDays = age;
  }
  const clients = [...byWs.values()]
    .filter((c) => c.escalated.stale > 0 || c.drafted.stale > 0)
    .sort((a, b) => (b.escalated.stale - a.escalated.stale) || (b.drafted.stale - a.drafted.stale) || a.client_name.localeCompare(b.client_name));
  const totals = clients.reduce((t, c) => ({
    staleEscalated: t.staleEscalated + c.escalated.stale,
    staleDrafted: t.staleDrafted + c.drafted.stale,
  }), { staleEscalated: 0, staleDrafted: 0 });
  return {
    now, since: data.since || DEFAULT_SINCE, staleHours: data.staleHours || STALE_HOURS,
    clients, totals,
    legacy: { n: Number(data.legacy?.n) || 0, clients: Number(data.legacy?.clients) || 0 },
  };
}

function plural(n, one, many) {
  return `${n} ${n === 1 ? one : many}`;
}

function clientLine(c) {
  const parts = [];
  if (c.escalated.stale > 0) {
    parts.push(`${plural(c.escalated.stale, 'escalado sin responder', 'escalados sin responder')}${c.escalated.oldestDays !== null ? ` (el más viejo ${c.escalated.oldestDays} d)` : ''}`);
  }
  if (c.drafted.stale > 0) {
    parts.push(`${plural(c.drafted.stale, 'borrador sin revisar', 'borradores sin revisar')}${c.drafted.oldestDays !== null ? ` (${c.drafted.oldestDays} d)` : ''}`);
  }
  const flags = [];
  if (c.paused) flags.push('⏸️ cliente en pausa');
  if (c.auto_send) flags.push('envío automático activo');
  return `• ${c.client_name}: ${parts.join(' · ')}${flags.length ? ` · ${flags.join(' · ')}` : ''}`;
}

// Returns null when nothing has waited past the stale window: no post, by
// Ron's grooming bar (a no-change day is no message). Weekly since 2026-11-10
// (Ron): one report in the fulfillment channel instead of a daily DM, with
// last week's totals so the channel sees the trend.
function formatKaiEscalationsDigest(summary, { maxLines = 10, weekly = false, previous = null } = {}) {
  const { totals, clients, legacy, now, staleHours } = summary;
  if (totals.staleEscalated === 0 && totals.staleDrafted === 0) return null;
  const lines = [];
  lines.push(`\`KAI: ESCALADOS SIN ATENDER (${weekly ? 'semana del ' : ''}${crDateLabel(now)})\``);
  lines.push('');
  lines.push(`⚠️ ${plural(totals.staleEscalated, 'escalado', 'escalados')} y ${plural(totals.staleDrafted, 'borrador', 'borradores')} con más de ${staleHours} h en ${plural(clients.length, 'cliente', 'clientes')}.`);
  if (previous && Number.isFinite(Number(previous.staleEscalated)) && Number.isFinite(Number(previous.staleDrafted))) {
    lines.push(`Semana pasada: ${plural(Number(previous.staleEscalated), 'escalado', 'escalados')} y ${plural(Number(previous.staleDrafted), 'borrador', 'borradores')}.`);
  }
  lines.push('');
  for (const c of clients.slice(0, maxLines)) lines.push(clientLine(c));
  if (clients.length > maxLines) lines.push(`• y ${plural(clients.length - maxLines, 'cliente más', 'clientes más')}`);
  lines.push('');
  lines.push('`QUÉ HACER`');
  lines.push('• Escalado = Kai no pudo responder: alguien contesta en LinkedIn (Prosp) como el cliente, o se le avisa al cliente para que lo haga.');
  lines.push('• Borradores sin revisar = el cliente no entra a su bandeja: una llamada corta, o proponerle el envío automático.');
  if (legacy.n > 0) {
    lines.push('');
    lines.push(`• Pendiente anterior al ${summary.since}: ${legacy.n} en ${plural(legacy.clients, 'cliente', 'clientes')} (revisar una vez y cerrar; no vuelve a listarse por cliente).`);
  }
  return lines.join('\n');
}

module.exports = {
  SQL, DEFAULT_SINCE, STALE_HOURS,
  readKaiEscalations, summarizeKaiEscalations, formatKaiEscalationsDigest,
  daysAgo,
};
