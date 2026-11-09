-- 026_agent_events.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE setting AGENT_EVENTS_MODE=shadow in Max or deploying AXON's
-- emitter. With the mode off (the default) Max never reads this table.
--
-- Agent event bus, phase 1 (Ron, 2026-11-08; spec in tasks/specs/agent-event-bus.md).
-- REVI, AXON and Kai report things that happened (needs a human, an error, a job
-- finished) by inserting one row here. Max polls unconsumed rows, routes each in
-- plain code (no LLM) and stamps consumed_at. Heartbeats do NOT go here: they stay
-- in revi.engine_heartbeat and axon.engine_heartbeat.
--
-- Rows hold ids, counts and one readable sentence. Never a transcript, a draft,
-- a financial figure or lead/client personal data. The payload cap below is
-- enforced by the database so a careless emitter cannot store a token bomb.

CREATE TABLE IF NOT EXISTS agent_events (
  id              bigserial   PRIMARY KEY,
  created_at      timestamptz NOT NULL DEFAULT now(),
  source          text        NOT NULL,
  kind            text        NOT NULL,
  severity        text        NOT NULL DEFAULT 'info',
  audience        text        NOT NULL DEFAULT 'ron',
  subject_key     text,                        -- client:<id>, campaign:<id>, run:<id>
  summary         text        NOT NULL,
  payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
  correlation_id  text,
  idempotency_key text        NOT NULL,
  -- Max's bookkeeping, set by the poller only.
  consumed_at     timestamptz,
  route           text,                        -- what Max did or, in shadow, would have done
  route_mode      text,                        -- shadow | live
  acked_at        timestamptz,
  acked_by        text,
  CONSTRAINT agent_events_source_check   CHECK (source   IN ('max','revi','axon','kai')),
  CONSTRAINT agent_events_kind_check     CHECK (kind     IN ('needs_human','error','job_done','handoff')),
  CONSTRAINT agent_events_severity_check CHECK (severity IN ('info','warn','red')),
  CONSTRAINT agent_events_audience_check CHECK (audience IN ('ron','management','team')),
  CONSTRAINT agent_events_summary_len    CHECK (char_length(summary) BETWEEN 1 AND 300),
  CONSTRAINT agent_events_payload_size   CHECK (octet_length(payload::text) <= 4096),
  CONSTRAINT agent_events_idem_len       CHECK (char_length(idempotency_key) BETWEEN 1 AND 200)
);
-- A retry of the same event is a no-op, never a second row.
CREATE UNIQUE INDEX IF NOT EXISTS agent_events_idem_idx ON agent_events (source, idempotency_key);
CREATE INDEX IF NOT EXISTS agent_events_unconsumed_idx ON agent_events (created_at) WHERE consumed_at IS NULL;
CREATE INDEX IF NOT EXISTS agent_events_subject_idx ON agent_events (source, subject_key, consumed_at DESC);

-- RLS on, mirroring 023 and 025. Max and REVI use the ANON key, so the policies
-- name it: read, insert and update, never delete. AXON writes with the service
-- role, which skips RLS but still needs table grants: this project revokes the
-- default grants, so every grant is explicit.
ALTER TABLE agent_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agent_events_read   ON agent_events;
DROP POLICY IF EXISTS agent_events_insert ON agent_events;
DROP POLICY IF EXISTS agent_events_update ON agent_events;
CREATE POLICY agent_events_read   ON agent_events FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY agent_events_insert ON agent_events FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY agent_events_update ON agent_events FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE ON agent_events TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON SEQUENCE agent_events_id_seq TO anon, authenticated, service_role;
