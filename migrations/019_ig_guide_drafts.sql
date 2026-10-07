-- 019_ig_guide_drafts.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes this table (runIgGuideCheck).
-- Without it every run fails its first read and nothing is drafted (fail closed).
--
-- Instagram guide replies, phase A (plan-of-record §1 UPDATE 2026-10-07): one row
-- per lead message Max drafted an answer for. The send to the lead is claimed
-- atomically on `status` (pending → sending), so a double ✅ never sends twice.
-- This table is also the evidence for phase B: approve rate and edit rate by intent.

CREATE TABLE IF NOT EXISTS ig_guide_drafts (
  message_id       text PRIMARY KEY,                -- GHL id of the newest lead message answered
  conversation_id  text NOT NULL,
  contact_id       text NOT NULL,
  contact_name     text,
  lead_message     text,                            -- the burst being answered
  lead_message_at  timestamptz NOT NULL,            -- Meta's 24 h window starts here
  intent           text NOT NULL DEFAULT 'other',
  stage            text NOT NULL DEFAULT 'discover',
  collected        jsonb NOT NULL DEFAULT '{}'::jsonb,
  draft            text NOT NULL DEFAULT '',
  problems         text,                            -- validation problems left after one retry
  status           text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','sending','sent','skipped','superseded','expired','archived')),
  slack_channel    text,
  slack_ts         text,
  sent_text        text,
  edited           boolean,
  ghl_message_id   text,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  decided_at       timestamptz,
  sent_at          timestamptz
);

CREATE INDEX IF NOT EXISTS ig_guide_drafts_convo_idx ON ig_guide_drafts (conversation_id, status);
CREATE INDEX IF NOT EXISTS ig_guide_drafts_created_idx ON ig_guide_drafts (created_at);

-- index.js uses the ANON key, and CREATE TABLE via a migration does not inherit
-- dashboard grants. Max-owned, RLS off like lead_reply_signals. No DELETE.
GRANT SELECT, INSERT, UPDATE ON ig_guide_drafts TO anon, authenticated;
