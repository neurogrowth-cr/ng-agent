const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

const client = createClient(
  process.env.PORTAL_SUPABASE_URL,
  process.env.PORTAL_SUPABASE_ANON_KEY
);

const ENABLED = process.env.AGENT_ACTIVITY_LOG !== 'false';

function newCorrelationId() {
  return crypto.randomUUID();
}

// supabase-js sits on plain fetch, which has no timeout of its own — an insert
// that never gets a response would hang forever, silently, with nothing to show
// for it: the exact "vanished" failure mode the cron watchdog exists to catch
// (see the CRON_DEFAULT_TIMEOUT_MS comment in index.js), except here it's the
// watchdog's OWN terminal-status write that can go dark. abortSignal bounds it.
const LOG_TIMEOUT_MS = 15_000;

function logActivity(row) {
  if (!ENABLED) return;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), LOG_TIMEOUT_MS);
  // fire-and-forget — never block the caller
  client.from('agent_activity').insert({ agent: 'max', status: 'ok', ...row })
    .abortSignal(controller.signal)
    .then(({ error }) => { if (error) console.error('[activityLog] insert failed:', error.message); })
    .catch(e => console.error('[activityLog] threw:', e.message))
    .finally(() => clearTimeout(timer));
}

module.exports = { logActivity, newCorrelationId };
