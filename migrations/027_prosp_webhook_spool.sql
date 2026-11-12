-- 027_prosp_webhook_spool.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Applied 2026-11-12 (Ron's OK the same day), before the dash code that writes to it.
--
-- Parking table for Prosp webhook events the portal database could not take
-- (Ron, 2026-11-12: Prosp webhooks are never off). On 2026-11-12 the portal
-- database was overloaded for five hours, the dash answered Prosp with errors,
-- and Prosp switched our webhooks off without telling anyone. Since then the
-- dash endpoint answers Prosp 200, writes the raw event here when its own store
-- fails or is too slow, and a dash scheduled route
-- (/api/scheduled/prosp-spool-drain) replays it into the portal and stamps
-- stored_at. It lives in this project on purpose: separate compute from the
-- portal. Max watches it (lib/kaiHealth.js, spool_waiting).
--
-- Rows hold a raw Prosp event only until it is replayed: the replay blanks
-- raw_body and headers, so what stays is ids, times and counts. The size caps
-- are enforced by the database so a careless writer cannot fill the table.

CREATE TABLE IF NOT EXISTS prosp_webhook_spool (
  id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  target          text        NOT NULL DEFAULT 'dash',
  received_at     timestamptz NOT NULL DEFAULT now(),
  raw_body        text        NOT NULL,
  headers         jsonb       NOT NULL DEFAULT '{}'::jsonb,
  workspace_id    text,                        -- Prosp workspaceId, for the alert text
  event_type      text,
  reason          text,                        -- why it was parked
  attempts        integer     NOT NULL DEFAULT 0,
  last_error      text,
  last_attempt_at timestamptz,
  stored_at       timestamptz,                 -- set by the replay; null = still waiting
  portal_log_id   text,                        -- the prosp_webhook_logs row it became
  CONSTRAINT prosp_webhook_spool_target_check CHECK (target IN ('dash','kai')),
  CONSTRAINT prosp_webhook_spool_body_size    CHECK (octet_length(raw_body) <= 262144),
  CONSTRAINT prosp_webhook_spool_headers_size CHECK (octet_length(headers::text) <= 4096)
);
CREATE INDEX IF NOT EXISTS prosp_webhook_spool_pending_idx ON prosp_webhook_spool (received_at) WHERE stored_at IS NULL;

-- RLS on, mirroring 026: every grant is explicit in this project. Read, insert
-- and update, never delete.
ALTER TABLE prosp_webhook_spool ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS prosp_webhook_spool_read   ON prosp_webhook_spool;
DROP POLICY IF EXISTS prosp_webhook_spool_insert ON prosp_webhook_spool;
DROP POLICY IF EXISTS prosp_webhook_spool_update ON prosp_webhook_spool;
CREATE POLICY prosp_webhook_spool_read   ON prosp_webhook_spool FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY prosp_webhook_spool_insert ON prosp_webhook_spool FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY prosp_webhook_spool_update ON prosp_webhook_spool FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE ON prosp_webhook_spool TO anon, authenticated, service_role;
