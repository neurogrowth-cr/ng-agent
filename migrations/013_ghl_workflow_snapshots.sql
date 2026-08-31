-- 013_ghl_workflow_snapshots.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
--
-- Daily snapshot of the GHL workflow inventory, for the drift watchdog
-- (runGhlWorkflowDriftCheck). The endpoint is list-only — seven fields per
-- workflow, no steps or triggers — so this stores identity + status + version
-- and nothing more.
--
-- Not stored in agent_knowledge on purpose: upsertKnowledge truncates `value`
-- at 2000 chars, and a 22-workflow payload is ~3-4KB, so it would be silently
-- clipped into a corrupt diff with no error raised.

CREATE TABLE IF NOT EXISTS ghl_workflow_snapshots (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  captured_at    timestamptz NOT NULL DEFAULT now(),
  location_id    text NOT NULL,
  -- A zero-workflow read is indistinguishable from a failed read. The watchdog
  -- refuses to write one; this CHECK makes that a database guarantee rather
  -- than an application convention, so no future caller can poison the diff
  -- with an empty snapshot that would read as "every workflow was deleted".
  workflow_count integer NOT NULL CHECK (workflow_count > 0),
  workflows      jsonb NOT NULL,
  correlation_id text
);

CREATE INDEX IF NOT EXISTS ghl_workflow_snapshots_captured_idx
  ON ghl_workflow_snapshots (captured_at DESC);

-- index.js builds its Supabase client with the ANON key, so the watchdog can
-- only reach this table if anon is granted. CREATE TABLE via a migration does
-- NOT inherit Supabase's default dashboard grants — the first live run failed
-- with "permission denied for table ghl_workflow_snapshots" for exactly this
-- reason. Posture matches metric_observations (Max-owned, append-only, RLS off,
-- anon granted), with one deliberate difference: no DELETE. These snapshots are
-- the watchdog's memory; nothing should be able to erase them.
GRANT SELECT, INSERT ON ghl_workflow_snapshots TO anon, authenticated;
