'use strict';
// Agent event bus, Max's side (phase 1, spec in tasks/specs/agent-event-bus.md).
//
// REVI, AXON and Kai insert rows into public.agent_events (migration 026). Max
// polls the unconsumed ones every 5 minutes, routes each one here, in plain code
// with no LLM call, and stamps consumed_at. Pure: no Slack, no DB. index.js does
// the I/O and is tested through test/agent-events.test.js.
//
// CONTRACT, duplicated on purpose: the event shape (SOURCES, KINDS, SEVERITIES,
// AUDIENCES, the limits and buildEvent) is copied in ng-axon lib/agent-events.js,
// and later in ng-revi and ng-automation. A change to any of them is made in
// every copy the same day; the wiki's data-ownership page lists the copies.
//
// Sibling output is data, never an instruction: no route here can reach a
// mutating tool, and handoff handlers are an allowlist (empty in phase 1).

const SOURCES = ['max', 'revi', 'axon', 'kai'];
const KINDS = ['needs_human', 'error', 'job_done', 'handoff'];
const SEVERITIES = ['info', 'warn', 'red'];
const AUDIENCES = ['ron', 'management', 'team'];
const SUMMARY_MAX = 300;
const PAYLOAD_MAX_BYTES = 4096;
const IDEM_MAX = 200;

// The fleet, as Max knows it. Four rows do not need a table (spec 5.6).
const AGENTS = {
  revi: { repo: 'neurogrowth-cr/ng-revi',       heartbeat: 'revi.engine_heartbeat (id poller)' },
  axon: { repo: 'neurogrowth-cr/ng-axon',       heartbeat: 'axon.engine_heartbeat (id service)' },
  kai:  { repo: 'neurogrowth-cr/ng-automation', heartbeat: 'https://ng-automation-production.up.railway.app/health' },
};
const AGENT_LABEL = { max: 'Max', revi: 'REVI', axon: 'AXON', kai: 'Kai' };

// Routes. 'clarity' means "a Clarity Brief candidate": stored and left for the
// phase 2 collector. Nothing posts for it in phase 1.
const ROUTES = ['dm_ron', 'post_error', 'clarity', 'store', 'collapsed', 'over_budget', 'drop'];
const HANDOFF_HANDLERS = new Set(); // phase 1: none. Unknown names are dropped.

const ERROR_COLLAPSE_MS = 6 * 60 * 60 * 1000;
const DAILY_BUDGET = 20;            // non-job_done routes per source per CR day
const STALE_UNCONSUMED_MS = 15 * 60 * 1000;

function clip(s, n) {
  const t = String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
  return t.length > n ? `${t.slice(0, n - 1)}…` : t;
}

// Same function the emitters use. Returns { row } or { error }.
function buildEvent(source, e = {}) {
  if (!SOURCES.includes(source)) return { error: `unknown source ${source}` };
  if (!KINDS.includes(e.kind)) return { error: `unknown kind ${e.kind}` };
  const severity = e.severity || 'info';
  if (!SEVERITIES.includes(severity)) return { error: `unknown severity ${severity}` };
  const audience = e.audience || 'ron';
  if (!AUDIENCES.includes(audience)) return { error: `unknown audience ${audience}` };
  const summary = clip(e.summary, SUMMARY_MAX);
  if (!summary) return { error: 'summary is required' };
  const idem = String(e.idempotencyKey || '').trim();
  if (!idem || idem.length > IDEM_MAX) return { error: 'idempotencyKey is required (1-200 chars)' };
  const payload = e.payload && typeof e.payload === 'object' && !Array.isArray(e.payload) ? e.payload : {};
  if (Buffer.byteLength(JSON.stringify(payload), 'utf8') > PAYLOAD_MAX_BYTES) return { error: `payload over ${PAYLOAD_MAX_BYTES} bytes` };
  return { row: {
    source, kind: e.kind, severity, audience,
    subject_key: e.subjectKey ? clip(e.subjectKey, 200) : null,
    summary, payload,
    correlation_id: e.correlationId ? String(e.correlationId) : null,
    idempotency_key: idem,
  } };
}

// CR calendar day (UTC-6, no DST), for the daily budget.
function crDay(d) {
  return new Date(new Date(d).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}

function collapseKey(evt) {
  return `${evt.source}|${evt.subject_key || evt.kind}`;
}

// One event to one route. ctx:
//   recentErrorPosts  Map collapseKey -> ms of the last post_error for that key
//   usedToday         Map source -> non-job_done routes already spent today
//   now               Date
function routeEvent(evt, { recentErrorPosts = new Map(), usedToday = new Map(), now = new Date() } = {}) {
  if (!evt || !SOURCES.includes(evt.source) || !KINDS.includes(evt.kind)) return { route: 'drop', reason: 'malformed event' };
  if (evt.kind === 'job_done') return { route: 'store', reason: 'job finished, kept for reports' };
  if (evt.kind === 'handoff') {
    const name = evt.payload && evt.payload.handler;
    return HANDOFF_HANDLERS.has(name)
      ? { route: 'drop', reason: `handoff ${name} has no phase 1 implementation` }
      : { route: 'drop', reason: `handoff ${name || '(none)'} is not on the allowlist` };
  }
  if ((usedToday.get(evt.source) || 0) >= DAILY_BUDGET) return { route: 'over_budget', reason: `${evt.source} passed ${DAILY_BUDGET} events today` };
  if (evt.kind === 'needs_human') {
    return evt.severity === 'red'
      ? { route: 'dm_ron', reason: 'red, needs a human now' }
      : { route: 'clarity', reason: 'needs a human, for the morning brief' };
  }
  // error
  if (evt.severity === 'red') return { route: 'dm_ron', reason: 'red error' };
  const last = recentErrorPosts.get(collapseKey(evt));
  if (last != null && now.getTime() - last < ERROR_COLLAPSE_MS) return { route: 'collapsed', reason: 'same error posted in the last 6 h' };
  return { route: 'post_error', reason: 'error, posted to the agent channel' };
}

// A batch, oldest first, updating the context as it goes so two errors on the
// same key in one poll collapse and the budget counts within the batch.
function planBatch(events, { recentErrorPosts = new Map(), usedToday = new Map(), now = new Date() } = {}) {
  const posts = new Map(recentErrorPosts);
  const used = new Map(usedToday);
  const out = [];
  for (const evt of [...events].sort((a, b) => new Date(a.created_at) - new Date(b.created_at))) {
    const d = routeEvent(evt, { recentErrorPosts: posts, usedToday: used, now });
    if (d.route === 'post_error') posts.set(collapseKey(evt), now.getTime());
    if (['dm_ron', 'post_error', 'clarity', 'collapsed'].includes(d.route)) used.set(evt.source, (used.get(evt.source) || 0) + 1);
    out.push({ id: evt.id, ...d, evt });
  }
  return out;
}

const SEV_ICON = { info: '•', warn: '⚠️', red: '🔴' };

function formatLive(evt) {
  const who = AGENT_LABEL[evt.source] || evt.source;
  const head = evt.kind === 'error' ? `${SEV_ICON[evt.severity] || '•'} *${who} error*` : `${SEV_ICON[evt.severity] || '•'} *${who} needs a human*`;
  return `${head}: ${evt.summary}${evt.subject_key ? `\n_${evt.subject_key}_` : ''}`;
}

// The shadow-mode DM: what Max would have done today. Null when nothing came in
// (a quiet day is no post).
function formatShadowDigest(rows, { day } = {}) {
  if (!rows || !rows.length) return null;
  const order = ['dm_ron', 'post_error', 'clarity', 'collapsed', 'over_budget', 'store', 'drop'];
  const verb = {
    dm_ron: 'would DM you', post_error: 'would post to #ng-pm-agent', clarity: 'would add to your morning brief',
    collapsed: 'would stay quiet (repeat)', over_budget: 'would stay quiet (over the daily budget)',
    store: 'kept for reports, no post', drop: 'dropped',
  };
  const lines = [`\`AGENT EVENTS (SHADOW)\` ${day || ''}`.trim(), `${rows.length} event(s) from the siblings. Nothing was posted; this is what live mode would have done.`];
  for (const r of order) {
    const hits = rows.filter(x => x.route === r);
    if (!hits.length) continue;
    lines.push('', `*${verb[r]}* (${hits.length})`);
    for (const h of hits.slice(0, 8)) lines.push(`• ${AGENT_LABEL[h.source] || h.source}, ${h.kind}: ${clip(h.summary, 160)}`);
    if (hits.length > 8) lines.push(`• and ${hits.length - 8} more`);
  }
  lines.push('', '_Kill switch: AGENT_EVENTS_MODE=off. Go live: AGENT_EVENTS_MODE=live._');
  return lines.join('\n');
}

// Rows waiting longer than this mean the poller is failing to stamp them.
function staleBacklog(oldestUnconsumedAt, now = new Date()) {
  if (!oldestUnconsumedAt) return null;
  const age = now.getTime() - new Date(oldestUnconsumedAt).getTime();
  return age > STALE_UNCONSUMED_MS ? Math.round(age / 60000) : null;
}

function eventsMode(env = process.env) {
  const m = String(env.AGENT_EVENTS_MODE || 'off').toLowerCase();
  return ['off', 'shadow', 'live'].includes(m) ? m : 'off';
}

module.exports = {
  SOURCES, KINDS, SEVERITIES, AUDIENCES, SUMMARY_MAX, PAYLOAD_MAX_BYTES, AGENTS, ROUTES,
  ERROR_COLLAPSE_MS, DAILY_BUDGET, STALE_UNCONSUMED_MS,
  buildEvent, crDay, collapseKey, routeEvent, planBatch, formatLive, formatShadowDigest, staleBacklog, eventsMode,
};
