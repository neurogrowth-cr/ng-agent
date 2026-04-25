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

function logActivity(row) {
  if (!ENABLED) return;
  // fire-and-forget — never block the caller
  client.from('agent_activity').insert({ agent: 'max', status: 'ok', ...row })
    .then(({ error }) => { if (error) console.error('[activityLog] insert failed:', error.message); })
    .catch(e => console.error('[activityLog] threw:', e.message));
}

module.exports = { logActivity, newCorrelationId };
