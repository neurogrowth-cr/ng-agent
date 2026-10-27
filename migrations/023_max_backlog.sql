-- 023_max_backlog.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes this table (recordBacklogItem).
-- Without it every capture logs a read error and nothing is recorded (fail closed:
-- replies still post, only the ledger stays empty).
--
-- Max self-improvement backlog (Ron, 2026-10-24). One row per thing Max could not
-- do, deduped by slug: a capability he lacks or a rule that blocked him (from his
-- own replies, judged by Haiku), a tool that threw, a cron that failed, a saved
-- correction. occurrences counts repeats. The monthly digest in #ng-pm-agent
-- ranks open rows; the /max-backlog Claude Code skill moves them to planned,
-- done or dropped. A done row that happens again reopens.
--
-- Rows hold a capability description, never a lead's message: titles and
-- summaries are scrubbed of emails and long digit runs before insert.

CREATE TABLE IF NOT EXISTS max_backlog (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug            text NOT NULL,                      -- dedupe key, e.g. gap:eliminar-contacto-ghl
  kind            text NOT NULL
                  CHECK (kind IN ('capability_gap','policy_block','tool_error','check_broken','correction')),
  title           text NOT NULL,
  summary         text,
  requested_by    text,                               -- Slack user id, null for crons
  channel         text,                               -- Slack channel id of the triggering message
  permalink       text,                               -- Slack permalink when known
  occurrences     integer NOT NULL DEFAULT 1,
  first_seen      timestamptz NOT NULL DEFAULT now(),
  last_seen       timestamptz NOT NULL DEFAULT now(),
  status          text NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open','planned','done','dropped')),
  notes           text,                               -- set by /max-backlog: plan line, PR number, drop reason
  correlation_id  text,
  day             date NOT NULL DEFAULT ((now() AT TIME ZONE 'America/Costa_Rica')::date),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS max_backlog_slug_uq   ON max_backlog (slug);
CREATE INDEX IF NOT EXISTS max_backlog_status_seen_idx  ON max_backlog (status, last_seen DESC);

-- RLS on, mirroring 022_guardian_actions.sql. index.js uses the ANON key, so the
-- policies name it: read, insert and update, never delete. Grants are explicit
-- because migration-created tables do not inherit dashboard grants (and after
-- 2026-10-30 new tables get none).
ALTER TABLE max_backlog ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS max_backlog_read   ON max_backlog;
DROP POLICY IF EXISTS max_backlog_insert ON max_backlog;
DROP POLICY IF EXISTS max_backlog_update ON max_backlog;
CREATE POLICY max_backlog_read   ON max_backlog FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY max_backlog_insert ON max_backlog FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY max_backlog_update ON max_backlog FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE ON max_backlog TO anon, authenticated;
