'use strict';
// Call fields on the outcome card (Ron, 2026-10-11, post-call nurture phase 2).
//
// When a closer logs follow_up on an outcome card, Max offers a "Call details"
// button that opens a Slack form: industry, main objection, the prospect's
// ticket, clients missing per month, next-step date. The answers land in three
// places so the post-call email sequence can branch on them:
//   - GHL contact tags `industria:*` and `objecion:*` (the workflow's If/Else),
//   - GHL contact custom fields "Llamada ..." (merge fields for email 0's math),
//   - portal revops_sales_outcomes.objection_category + next_action_date, via
//     the ng_set_outcome_call_fields RPC (scorecards; the column was blank on
//     100% of follow_up/lost rows),
// plus a copy in Max's own call_fields table.
//
// Pure: no Slack client, no DB, no GHL. index.js owns the I/O.
// Tested in test/call-fields.test.js.

const OPEN_ACTION_ID = 'call_fields_open';
const SUBMIT_CALLBACK_ID = 'call_fields_submit';

// Tag values match the email workflow (postcall-emails-v1.md): asesor, coach,
// tech, industrial and real_estate pick a case in email 1; salud and otro fall
// through to the default case (Smartrade), same as no tag.
const INDUSTRIES = [
  { value: 'asesor', label: 'Financial advisor, insurance, banking' },
  { value: 'coach', label: 'Coach, consultant, trainer' },
  { value: 'tech', label: 'Software, agency, digital services' },
  { value: 'industrial', label: 'Industrial B2B, export, logistics' },
  { value: 'real_estate', label: 'Real estate' },
  { value: 'salud', label: 'Health, premium services' },
  { value: 'otro', label: 'Other' },
];

// The five objection branches from the brief (emails 3 and 6).
const OBJECTIONS = [
  { value: 'liquidez', label: 'Cash flow (liquidez)' },
  { value: 'socio', label: 'Partner or spouse has to decide' },
  { value: 'prueba', label: 'Wants more proof' },
  { value: 'timing', label: 'Timing, not now' },
  { value: 'otro', label: 'Other' },
];

// The portal stores objections in the dash's own picklist
// (REVOPS_OBJECTION_CATEGORIES, dash src/lib/revops-picklists.ts), which the
// ng_set_outcome_call_fields RPC enforces. GHL keeps the Spanish branch names.
const PORTAL_OBJECTION = {
  liquidez: 'price',
  socio: 'authority',
  prueba: 'trust',
  timing: 'timing',
  otro: 'other',
};

const INDUSTRY_TAG_PREFIX = 'industria:';
const OBJECTION_TAG_PREFIX = 'objecion:';

// GHL custom fields, matched by NAME at runtime (index.js resolves the ids), so
// nothing breaks if a field is recreated. All TEXT on purpose: GHL merge fields
// cannot do math or format dates, so Max writes them email-ready.
const GHL_FIELD_NAMES = {
  industry: 'Llamada Industria',
  objection: 'Llamada Objecion',
  ticket: 'Llamada Ticket',
  clientsMissing: 'Llamada Clientes Faltantes',
  monthlyValue: 'Llamada Valor Mensual',
  inactionCost: 'Llamada Costo Inaccion',
  nextStep: 'Llamada Siguiente Paso',
};

// Email 0's math: 10 guaranteed meetings, 3 of 4 show, 1 of 4 close ≈ 2 new
// clients a month. The email says "dos veces lo que usted cobra"; this field
// is that number with their real ticket.
const FLYWHEEL_CLIENTS_PER_MONTH = 2;

const MAX_TICKET = 10000000;
const MAX_CLIENTS_MISSING = 1000;

function shouldOfferCallFields(outcome, flag) {
  return outcome === 'follow_up' && String(flag || '').trim().toLowerCase() === 'on';
}

function optionFor(list, value) {
  const o = list.find((x) => x.value === value);
  return o ? { text: { type: 'plain_text', text: o.label }, value: o.value } : null;
}

function optionsOf(list) {
  return list.map((o) => ({ text: { type: 'plain_text', text: o.label }, value: o.value }));
}

// The thread message under a follow_up card. `meta` rides in the button value
// (Slack caps it at 2000 chars; this is ~250).
function buildCallFieldsButtonMessage({ prospectName, meta }) {
  const text = `📋 Add the call details for ${prospectName}: industry, main objection, their ticket, clients missing a month, next step. The follow-up emails use them.`;
  return {
    text,
    blocks: [
      { type: 'section', text: { type: 'mrkdwn', text } },
      {
        type: 'actions',
        elements: [{
          type: 'button',
          action_id: OPEN_ACTION_ID,
          text: { type: 'plain_text', text: 'Call details' },
          style: 'primary',
          value: JSON.stringify(meta),
        }],
      },
    ],
  };
}

function buildCallFieldsModal({ meta, prospectName, prefill = null }) {
  const p = prefill || {};
  const industry = { type: 'static_select', action_id: 'v', placeholder: { type: 'plain_text', text: 'Pick one' }, options: optionsOf(INDUSTRIES) };
  const io = optionFor(INDUSTRIES, p.industry);
  if (io) industry.initial_option = io;
  const objection = { type: 'static_select', action_id: 'v', placeholder: { type: 'plain_text', text: 'Pick one' }, options: optionsOf(OBJECTIONS) };
  const oo = optionFor(OBJECTIONS, p.objection);
  if (oo) objection.initial_option = oo;
  const ticket = { type: 'number_input', action_id: 'v', is_decimal_allowed: false, min_value: '1', max_value: String(MAX_TICKET) };
  if (p.ticket) ticket.initial_value = String(p.ticket);
  const clients = { type: 'number_input', action_id: 'v', is_decimal_allowed: false, min_value: '0', max_value: String(MAX_CLIENTS_MISSING) };
  if (p.clientsMissing != null) clients.initial_value = String(p.clientsMissing);
  const date = { type: 'datepicker', action_id: 'v' };
  if (p.nextStepDate) date.initial_date = p.nextStepDate;

  return {
    type: 'modal',
    callback_id: SUBMIT_CALLBACK_ID,
    private_metadata: JSON.stringify(meta),
    title: { type: 'plain_text', text: 'Call details' },
    submit: { type: 'plain_text', text: 'Save' },
    close: { type: 'plain_text', text: 'Cancel' },
    blocks: [
      { type: 'context', elements: [{ type: 'mrkdwn', text: `*${String(prospectName || 'Prospect').slice(0, 150)}*. Industry and objection pick which emails they get; the numbers go into email 0.` }] },
      { type: 'input', block_id: 'industry', label: { type: 'plain_text', text: 'Industry' }, element: industry },
      { type: 'input', block_id: 'objection', label: { type: 'plain_text', text: 'Main objection' }, element: objection },
      { type: 'input', block_id: 'ticket', optional: true, label: { type: 'plain_text', text: 'Their ticket (USD, what they charge one client)' }, element: ticket },
      { type: 'input', block_id: 'clients', optional: true, label: { type: 'plain_text', text: 'Clients missing per month' }, element: clients },
      { type: 'input', block_id: 'next_step', optional: true, label: { type: 'plain_text', text: 'Next step date' }, element: date },
    ],
  };
}

function readInput(values, block) {
  const v = values && values[block] && values[block].v;
  if (!v) return null;
  if (v.selected_option) return v.selected_option.value;
  if (v.selected_date) return v.selected_date;
  return v.value != null && v.value !== '' ? v.value : null;
}

// view.state.values → { ok, fields } or { ok:false, errors } where errors is
// keyed by block_id, the shape Slack's response_action 'errors' wants.
function parseCallFieldsSubmission(values) {
  const errors = {};
  const industry = readInput(values, 'industry');
  const objection = readInput(values, 'objection');
  if (!INDUSTRIES.some((x) => x.value === industry)) errors.industry = 'Pick an industry.';
  if (!OBJECTIONS.some((x) => x.value === objection)) errors.objection = 'Pick the main objection.';

  let ticket = null;
  const rawTicket = readInput(values, 'ticket');
  if (rawTicket != null) {
    ticket = Number(rawTicket);
    if (!Number.isInteger(ticket) || ticket < 1 || ticket > MAX_TICKET) errors.ticket = 'Whole dollars, more than 0.';
  }
  let clientsMissing = null;
  const rawClients = readInput(values, 'clients');
  if (rawClients != null) {
    clientsMissing = Number(rawClients);
    if (!Number.isInteger(clientsMissing) || clientsMissing < 0 || clientsMissing > MAX_CLIENTS_MISSING) errors.clients = 'A whole number of clients.';
  }
  const nextStepDate = readInput(values, 'next_step');
  if (nextStepDate != null && !/^\d{4}-\d{2}-\d{2}$/.test(nextStepDate)) errors.next_step = 'Pick a date.';

  if (Object.keys(errors).length) return { ok: false, errors };
  return { ok: true, fields: { industry, objection, ticket, clientsMissing, nextStepDate } };
}

function callFieldTags(fields) {
  return [`${INDUSTRY_TAG_PREFIX}${fields.industry}`, `${OBJECTION_TAG_PREFIX}${fields.objection}`];
}

// Old industria:* / objecion:* tags that a resubmission replaces. Only one of
// each may sit on a contact, or the workflow's If/Else picks whichever it
// checks first.
function staleCallFieldTags(currentTags, fields) {
  const keep = new Set(callFieldTags(fields));
  return (currentTags || [])
    .map((t) => String(t || '').trim().toLowerCase())
    .filter((t) => (t.startsWith(INDUSTRY_TAG_PREFIX) || t.startsWith(OBJECTION_TAG_PREFIX)) && !keep.has(t));
}

function formatUsd(n) {
  return `$${Math.round(n).toLocaleString('en-US')}`;
}

// "2026-10-20" → "lunes 20 de octubre", ready for "el siguiente paso es el ...".
function formatSpanishDate(iso) {
  const d = new Date(`${iso}T12:00:00Z`);
  if (Number.isNaN(d.getTime())) return '';
  return new Intl.DateTimeFormat('es-CR', { weekday: 'long', day: 'numeric', month: 'long', timeZone: 'UTC' })
    .format(d).replace(',', '');
}

// { [GHL field name]: value } for every field this submission can fill.
// Blank answers write '' so a corrected submission clears a stale number.
function ghlCallFieldValues(fields) {
  const t = fields.ticket;
  const c = fields.clientsMissing;
  return {
    [GHL_FIELD_NAMES.industry]: fields.industry,
    [GHL_FIELD_NAMES.objection]: fields.objection,
    [GHL_FIELD_NAMES.ticket]: t ? formatUsd(t) : '',
    [GHL_FIELD_NAMES.clientsMissing]: c != null ? String(c) : '',
    [GHL_FIELD_NAMES.monthlyValue]: t ? formatUsd(t * FLYWHEEL_CLIENTS_PER_MONTH) : '',
    [GHL_FIELD_NAMES.inactionCost]: t && c ? formatUsd(t * c) : '',
    [GHL_FIELD_NAMES.nextStep]: fields.nextStepDate ? formatSpanishDate(fields.nextStepDate) : '',
  };
}

// Field name → id from GET /locations/{id}/customFields. Missing names are
// returned so the thread reply can say which fields to create.
function resolveGhlFieldIds(values, customFields) {
  const byName = new Map((customFields || []).map((f) => [String(f.name || '').trim().toLowerCase(), f.id]));
  const resolved = [];
  const missing = [];
  for (const [name, value] of Object.entries(values)) {
    const id = byName.get(name.toLowerCase());
    if (id) resolved.push({ id, field_value: value });
    else missing.push(name);
  }
  return { resolved, missing };
}

function labelOf(list, value) {
  return (list.find((x) => x.value === value) || {}).label || value;
}

function callFieldsSummary(fields) {
  const parts = [labelOf(INDUSTRIES, fields.industry), labelOf(OBJECTIONS, fields.objection)];
  if (fields.ticket) parts.push(`ticket ${formatUsd(fields.ticket)}`);
  if (fields.clientsMissing != null) parts.push(`${fields.clientsMissing} clients missing a month`);
  if (fields.nextStepDate) parts.push(`next step ${fields.nextStepDate}`);
  return parts.join(' · ');
}

function parseMeta(raw) {
  try {
    const m = JSON.parse(raw || '{}');
    return m && typeof m.appointment_id === 'string' && m.appointment_id ? m : null;
  } catch (_) { return null; }
}

module.exports = {
  OPEN_ACTION_ID,
  SUBMIT_CALLBACK_ID,
  INDUSTRIES,
  OBJECTIONS,
  GHL_FIELD_NAMES,
  PORTAL_OBJECTION,
  FLYWHEEL_CLIENTS_PER_MONTH,
  shouldOfferCallFields,
  buildCallFieldsButtonMessage,
  buildCallFieldsModal,
  parseCallFieldsSubmission,
  callFieldTags,
  staleCallFieldTags,
  formatSpanishDate,
  ghlCallFieldValues,
  resolveGhlFieldIds,
  callFieldsSummary,
  parseMeta,
};
