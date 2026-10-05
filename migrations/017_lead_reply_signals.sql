-- 017_lead_reply_signals.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes these tables (runHotReplyCheck).
-- A missing table only costs the reporting row (alerts still go out), but the
-- history from the first runs would be lost.
--
-- Reporting data for hot reply alerts and the social intake gap check (PR #256).
-- agent_knowledge only holds dedupe keys; these tables answer:
--   which channel brings buying intent, what leads ask for, how fast each setter
--   answers a hot reply, and whether a hot reply turns into a booked call
--   (join contact_id to revops_appointments / lead_posts).
--
-- lead_reply_signals: one row per unanswered run of lead messages, keyed on the
-- run's first message (the alert ladder's anchor). A lead who writes three
-- messages in a row is one row; if they write again after the team answered,
-- that is a new row. The verdict is the latest classification of the run;
-- ever_hot stays true once any classification said hot.
CREATE TABLE IF NOT EXISTS lead_reply_signals (
  anchor_message_id text PRIMARY KEY,
  newest_message_id text NOT NULL,
  conversation_id   text NOT NULL,
  contact_id        text NOT NULL,
  contact_name      text,
  channel           text NOT NULL CHECK (channel IN ('whatsapp', 'instagram', 'messenger')),
  owner_ghl_id      text,
  owner_label       text,          -- Sebastian / Oscar / William / Unassigned
  call_booked       boolean NOT NULL DEFAULT false,
  verdict           text NOT NULL CHECK (verdict IN ('hot', 'normal', 'noise')),
  ever_hot          boolean NOT NULL DEFAULT false,
  reason_es         text,          -- why the classifier chose the verdict
  summary_es        text,          -- what the lead wants, max ~12 words
  message_count     integer NOT NULL DEFAULT 1,
  lead_message_at   timestamptz NOT NULL,   -- the run's first lead message
  classified_at     timestamptz NOT NULL DEFAULT now(),
  poke1_at          timestamptz,
  poke2_at          timestamptz,
  answered_at       timestamptz,            -- first real team reply after lead_message_at
  mode              text NOT NULL DEFAULT 'dry_run',  -- alert mode when the row was written
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS lead_reply_signals_lead_at_idx ON lead_reply_signals (lead_message_at DESC);
CREATE INDEX IF NOT EXISTS lead_reply_signals_contact_idx ON lead_reply_signals (contact_id);
CREATE INDEX IF NOT EXISTS lead_reply_signals_unanswered_idx ON lead_reply_signals (lead_message_at) WHERE answered_at IS NULL;

-- social_intake_gaps: one row per Instagram/Messenger contact the intake check
-- flagged (no card, or card but no Slack lead post).
CREATE TABLE IF NOT EXISTS social_intake_gaps (
  contact_id       text PRIMARY KEY,
  conversation_id  text,
  contact_name     text,
  channel          text NOT NULL CHECK (channel IN ('instagram', 'messenger')),
  code             text NOT NULL CHECK (code IN ('no_card', 'no_post')),
  first_inbound_at timestamptz,
  detected_at      timestamptz NOT NULL DEFAULT now(),
  mode             text NOT NULL DEFAULT 'dry_run'
);

-- Response-time view for reports: minutes from the lead's message to the first
-- team reply, null while unanswered.
CREATE OR REPLACE VIEW lead_reply_signals_report AS
SELECT s.*,
       CASE WHEN s.answered_at IS NOT NULL
            THEN round(extract(epoch FROM (s.answered_at - s.lead_message_at)) / 60.0)::integer
       END AS minutes_to_answer
  FROM lead_reply_signals s;

-- index.js uses the ANON key, and CREATE TABLE via a migration does not inherit
-- dashboard grants (013's first live run failed on exactly this). Max-owned,
-- RLS off like ghl_workflow_snapshots. UPDATE is needed: the same row gets its
-- pokes and its answered_at later. No DELETE.
GRANT SELECT, INSERT, UPDATE ON lead_reply_signals TO anon, authenticated;
GRANT SELECT, INSERT ON social_intake_gaps TO anon, authenticated;
GRANT SELECT ON lead_reply_signals_report TO anon, authenticated;
