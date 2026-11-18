'use strict';
// Agency Prosp waiting list: ROLEX and PATEK clients still not on
// NeuroGrowth's Prosp 3+ days after their held activation call (dash
// plan-of-record UPDATE 2026-11-17 later). Connecting the client's LinkedIn in
// the agency workspace on the activation call is Valeria's duty (Ron,
// 2026-11-17), so the dash's daily prosp-key-check calls
// POST /webhook/agency-move-overdue with the list and Max DMs her. Auth is
// the shared CLIENT_ATTENTION_FEED_SECRET, as for /webhook/portal-request.
//
// Everything here is pure (no Slack, no network) and tested in
// test/agency-move-overdue.test.js.

const MODES = new Set(['live', 'dry_run']);
const MAX_CLIENTS = 50;
const RUNGS = /^[A-Z0-9_]{1,40}$/;
const DAY = /^\d{4}-\d{2}-\d{2}$/;

/** Slack mrkdwn treats &, < and > as control characters. */
function escapeSlack(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function oneLine(s) {
  return String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
}

/** Validates the dash payload. Fails closed on anything unexpected. */
function checkPayload(body) {
  if (!body || typeof body !== 'object') return { ok: false, problem: 'body is not an object' };
  const day = oneLine(body.day);
  if (!DAY.test(day)) return { ok: false, problem: 'day missing' };
  if (!Array.isArray(body.clients) || body.clients.length === 0) return { ok: false, problem: 'clients missing' };
  if (body.clients.length > MAX_CLIENTS) return { ok: false, problem: 'too many clients' };
  const clients = [];
  for (const c of body.clients) {
    const name = oneLine(c && c.name).slice(0, 120);
    const rung = oneLine(c && c.rung).toUpperCase();
    const days = Number(c && c.daysSinceCall);
    if (!name) return { ok: false, problem: 'client name missing' };
    if (!RUNGS.test(rung)) return { ok: false, problem: `bad rung for ${name}` };
    if (!Number.isInteger(days) || days < 0 || days > 3650) return { ok: false, problem: `bad daysSinceCall for ${name}` };
    clients.push({ name, rung, daysSinceCall: days });
  }
  // A missing mode is a dry run: only an explicit "live" reaches Valeria.
  const mode = MODES.has(body.mode) ? body.mode : 'dry_run';
  return { ok: true, payload: { day, clients, mode } };
}

/** The DM text, in Spanish for Valeria. */
function formatDm({ clients }) {
  const lines = clients.map((c) => {
    const d = c.daysSinceCall === 1 ? 'día' : 'días';
    return `• *${escapeSlack(c.name)}* (${escapeSlack(c.rung)}): ${c.daysSinceCall} ${d} desde la llamada de activación`;
  });
  return [
    `🔌 ${clients.length === 1 ? 'Un cliente ROLEX o PATEK sigue' : `${clients.length} clientes ROLEX o PATEK siguen`} sin su LinkedIn en el Prosp de NeuroGrowth, 3 o más días después de la llamada de activación:`,
    ...lines,
    'Conecta su LinkedIn en el workspace de agencia de NeuroGrowth y revisa sus LinkedIn handles en Admin; el dashboard los cambia solo en menos de una hora.',
  ].join('\n');
}

module.exports = { checkPayload, formatDm, escapeSlack, MAX_CLIENTS };
