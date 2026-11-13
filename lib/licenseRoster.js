// Licencia Unificada roster. Recipe: ~/automations/ops/recipes/license-roster.md
//
// Ron, 2026-11-13: twice a week (Mon and Thu) the fulfillment channel gets the
// up to date list of clients on a license SKU, and which of them do not have
// Kai working yet, so the team can turn Kai on where it is paid and offer the
// free trial (or the $200 upgrade) where it is not.
//
// The license SKUs are legacy only (pre-ladder Build & Release clients, see
// the wiki business/offer-catalog.md). Names and prices come from the portal's
// catalog_rungs, never from this file.
//
// "Kai activo" here means Kai can actually work for the client today, the same
// test ng-automation runs before it drafts or sends (lib/entitlement.js there):
//   1. the plan includes Kai: the $200 Kai + Licencia Unificada, Sales
//      Navigator + KAI, or the $140 Licencia Unificada only while a Kai test
//      drive runs on it (source 'kai_trial', an end date, not closed);
//   2. the client has a Kai workspace with the switch on
//      (workspaces.prosp_reply_agent_enabled);
//   3. the client has not paused it (workspaces.paused_by_customer).
// A $140 row with the switch on but no running trial is NOT active: Kai runs
// in 'license_only' mode there and sends nothing.
//
// Read-only: one query on portalPg (role max_readonly). Pure logic lives here
// so it is testable without booting the Slack app; index.js owns the cron.

const { notDemoDashboard } = require('./demoClient');

const CR_TZ = 'America/Costa_Rica';
const DAY_MS = 24 * 3600 * 1000;
const TRIAL_ENDING_DAYS = 7;

// The license family in catalog_rungs (strategy plan-of-record §1). A new
// license SKU has to be added here, or it will not show in the roster.
const LICENSE_RUNG_CODES = ['licencia_unificada', 'kai_licencia_unificada', 'kai_sales_nav', 'sales_nav'];
const KAI_INCLUDED = new Set(['kai_licencia_unificada', 'kai_sales_nav']);
const KAI_ONLY_ON_TRIAL = 'licencia_unificada';
const NO_KAI = new Set(['sales_nav']);
const LIVE_STATUSES = ['active', 'trialing', 'past_due'];

const SQL = {
  roster: `
    SELECT s.customer_id,
           COALESCE(NULLIF(trim(d.client_name), ''), 'UNKNOWN CLIENT') AS client_name,
           s.rung_code, s.status, s.source, s.trial_ends_at, s.trial_closed_at,
           r.name AS rung_name, r.price_amount,
           w.id AS workspace_id,
           w.prosp_reply_agent_enabled AS kai_switch,
           w.paused_by_customer AS paused,
           w.auto_send_enabled AS auto_send
      FROM automation_subscriptions s
      JOIN client_dashboards d ON d.id = s.customer_id
      LEFT JOIN catalog_rungs r ON r.code = s.rung_code
      LEFT JOIN LATERAL (
        SELECT ws.id, ws.prosp_reply_agent_enabled, ws.paused_by_customer, ws.auto_send_enabled
          FROM workspaces ws
         WHERE ws.dashboard_id = s.customer_id
           AND COALESCE(ws.is_demo, false) = false
         ORDER BY ws.updated_at DESC NULLS LAST
         LIMIT 1
      ) w ON true
     WHERE s.rung_code = ANY($1::text[])
       AND s.status = ANY($2::text[])
       AND ${notDemoDashboard('d')}`,
};

async function readLicenseRoster(pg, now) {
  const { rows } = await pg.query(SQL.roster, [LICENSE_RUNG_CODES, LIVE_STATUSES]);
  return { now, rows };
}

function crDateLabel(date) {
  return new Intl.DateTimeFormat('es-CR', { timeZone: CR_TZ, weekday: 'long', day: 'numeric', month: 'long' }).format(date);
}

function shortDate(ts) {
  const t = new Date(ts);
  if (Number.isNaN(t.getTime())) return null;
  return new Intl.DateTimeFormat('es-CR', { timeZone: CR_TZ, day: 'numeric', month: 'short' }).format(t).replace('.', '');
}

function daysUntil(ts, now) {
  const t = new Date(ts).getTime();
  if (Number.isNaN(t)) return null;
  return Math.ceil((t - now.getTime()) / DAY_MS);
}

function planLabel(row) {
  const name = String(row.rung_name || row.rung_code || 'Licencia').trim();
  const price = Number(row.price_amount);
  return Number.isFinite(price) && price > 0 ? `${name} $${Math.round(price)}` : name;
}

function isRunningTrial(row) {
  return row.source === 'kai_trial' && Boolean(row.trial_ends_at) && !row.trial_closed_at && LIVE_STATUSES.includes(row.status);
}

// One client's Kai reading. `on` is the answer to "does Kai work for them
// today"; `paysKai` marks the urgent case (they pay for Kai and it is off).
function kaiStatus(row, now) {
  const code = row.rung_code;
  if (NO_KAI.has(code)) return { on: false, paysKai: false, rank: 3, note: 'su plan no incluye Kai' };

  const trial = code === KAI_ONLY_ON_TRIAL && isRunningTrial(row);
  const included = KAI_INCLUDED.has(code) || trial;
  if (!included) {
    const ended = code === KAI_ONLY_ON_TRIAL && row.trial_closed_at ? shortDate(row.trial_closed_at) : null;
    return { on: false, paysKai: false, rank: 2, note: ended ? `su prueba de Kai terminó el ${ended}` : 'nunca activó la prueba gratis de Kai' };
  }

  const paysKai = KAI_INCLUDED.has(code);
  const who = paysKai ? 'paga Kai' : 'tiene la prueba de Kai';
  if (!row.workspace_id) return { on: false, paysKai, rank: paysKai ? 0 : 1, note: `${who} pero Kai no está configurado` };
  if (!row.kai_switch) return { on: false, paysKai, rank: paysKai ? 0 : 1, note: `${who} pero Kai está apagado` };
  if (row.paused) return { on: false, paysKai, rank: paysKai ? 0 : 1, note: `${who} pero el cliente lo pausó` };

  const mode = row.auto_send ? 'envío automático' : 'con aprobación';
  if (trial) {
    const left = daysUntil(row.trial_ends_at, now);
    const until = shortDate(row.trial_ends_at);
    const ending = left !== null && left <= TRIAL_ENDING_DAYS;
    return { on: true, trial: true, ending, daysLeft: left, note: `prueba gratis hasta el ${until}${ending ? ' ⏰' : ''} · ${mode}` };
  }
  return { on: true, trial: false, note: mode };
}

function summarizeLicenseRoster(data) {
  const now = data.now instanceof Date ? data.now : new Date(data.now);
  const clients = (data.rows || []).map((r) => {
    const k = kaiStatus(r, now);
    return {
      client_name: String(r.client_name || 'UNKNOWN CLIENT').trim().toUpperCase(),
      plan: planLabel(r),
      past_due: r.status === 'past_due',
      ...k,
    };
  });
  const byName = (a, b) => a.client_name.localeCompare(b.client_name);
  const off = clients.filter((c) => !c.on).sort((a, b) => (a.rank - b.rank) || byName(a, b));
  const on = clients.filter((c) => c.on).sort((a, b) =>
    (Number(b.trial && b.ending) - Number(a.trial && a.ending))
    || (Number(a.trial) - Number(b.trial))
    || ((a.daysLeft ?? 0) - (b.daysLeft ?? 0))
    || byName(a, b));
  return {
    now, clients, on, off,
    totals: {
      clients: clients.length,
      on: on.length,
      off: off.length,
      paysKaiOff: off.filter((c) => c.paysKai).length,
      trialsEnding: on.filter((c) => c.trial && c.ending).length,
    },
  };
}

function plural(n, one, many) {
  return `${n} ${n === 1 ? one : many}`;
}

function line(c) {
  return `• ${c.client_name} · ${c.plan} · ${c.paysKai ? '⚠️ ' : ''}${c.note}${c.past_due ? ' · pago atrasado' : ''}`;
}

// Always posts when there is at least one license (Ron wants the list on the
// day even when nothing changed). Zero rows is treated as a broken read by
// the caller, never posted.
function formatLicenseRoster(summary) {
  const { totals, on, off, now } = summary;
  if (totals.clients === 0) return null;
  const lines = [];
  lines.push(`\`LICENCIA UNIFICADA: LISTA AL DÍA (${crDateLabel(now)})\``);
  lines.push('');
  lines.push(`${plural(totals.clients, 'cliente', 'clientes')} con licencia · ${totals.on} con Kai activo · ${totals.off} sin Kai activo`);
  lines.push('');
  lines.push(`\`SIN KAI ACTIVO (${off.length})\``);
  if (off.length) for (const c of off) lines.push(line(c));
  else lines.push('• Ninguno: todos tienen Kai trabajando.');
  lines.push('');
  lines.push(`\`CON KAI ACTIVO (${on.length})\``);
  if (on.length) for (const c of on) lines.push(line(c));
  else lines.push('• Ninguno todavía.');

  const todo = [];
  if (totals.paysKaiOff > 0) todo.push('• ⚠️ Paga Kai y no está activo: encender o terminar de configurar Kai hoy, el cliente ya lo está pagando.');
  if (off.some((c) => c.rank === 1)) todo.push('• Prueba registrada pero Kai apagado o en pausa: revisar con el cliente y encenderlo, los días de prueba corren igual.');
  if (off.some((c) => c.rank === 2)) todo.push('• Licencia $140 sin Kai: ofrecerle la prueba gratis de Kai desde su dashboard, o pasarlo a KAI + Licencia Unificada.');
  if (totals.trialsEnding > 0) todo.push(`• ⏰ Prueba que vence en ${TRIAL_ENDING_DAYS} días o menos: hablar con el cliente para que siga con Kai (pasar a KAI + Licencia Unificada).`);
  if (todo.length) {
    lines.push('');
    lines.push('`QUÉ HACER`');
    lines.push(...todo);
  }
  return lines.join('\n');
}

// Recipe §4a: what a post must hold before it reaches the channel. Returns the
// list of broken rules (empty = good).
function checkLicenseRoster(text, summary) {
  const problems = [];
  if (!text) return ['empty text'];
  if (!/^`LICENCIA UNIFICADA: LISTA AL DÍA \(/.test(text)) problems.push('header missing');
  const listPart = text.split('`QUÉ HACER`')[0];
  const bullets = (listPart.match(/^• (?!Ninguno)/gm) || []).length;
  if (bullets !== summary.totals.clients) problems.push(`client lines ${bullets} != clients ${summary.totals.clients}`);
  if (summary.totals.on + summary.totals.off !== summary.totals.clients) problems.push('on + off != clients');
  if (/undefined|\bnull\b|NaN|\[object Object\]/.test(text)) problems.push('undefined/null/NaN in text');
  if (/—/.test(text)) problems.push('em dash in text');
  return problems;
}

module.exports = {
  SQL, LICENSE_RUNG_CODES, LIVE_STATUSES, TRIAL_ENDING_DAYS,
  readLicenseRoster, summarizeLicenseRoster, formatLicenseRoster, checkLicenseRoster, kaiStatus,
};
