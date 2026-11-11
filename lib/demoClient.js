// The portal's permanent fake DEMO client (Ron, 2026-11-11): "Consultora
// Ejemplo", demo@example.com. The dash marks it with
// client_dashboards.is_demo and workspaces.is_demo (boolean not null default
// false). Max must never report on, alert on, count, learn about or act on it,
// so every reader that loops over portal clients goes through one of these.
// Single-client lookups by id or email stay as they are.

// supabase-js: add `.eq('is_demo', false)` to a client_dashboards or
// workspaces query builder. Returns the builder so it chains.
function excludeDemo(query) {
  return query.eq('is_demo', false);
}

// A row (or a webhook record) that is the demo client.
function isDemoRecord(row) {
  return row?.is_demo === true;
}

// Raw SQL on portalPg: `${notDemoDashboard('d')}` in a WHERE clause.
function notDemoDashboard(alias) {
  return `COALESCE(${alias}.is_demo, false) = false`;
}

// Raw SQL on portalPg for any row keyed by a workspace id column, joined to
// workspaces or not. Keeps rows whose workspace is missing (LEFT JOIN nulls).
function notDemoWorkspace(workspaceIdColumn) {
  return `NOT EXISTS (SELECT 1 FROM workspaces demo_w WHERE demo_w.id = ${workspaceIdColumn} AND demo_w.is_demo)`;
}

// Prompt note for query_portal_db and the portal data map.
const DEMO_PROMPT_NOTE = 'client_dashboards.is_demo and workspaces.is_demo mark the fake DEMO client (Consultora Ejemplo, demo@example.com). Always add is_demo = false (client_dashboards or workspaces) to any query that lists, counts or aggregates clients, and never mention the demo in an answer.';

module.exports = { excludeDemo, isDemoRecord, notDemoDashboard, notDemoWorkspace, DEMO_PROMPT_NOTE };
