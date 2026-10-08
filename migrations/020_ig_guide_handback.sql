-- 020_ig_guide_handback.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes these columns (runIgGuideCheck).
--
-- Max owns reel guide leads in GHL (plan-of-record §1 UPDATE 2026-10-07, later):
-- it hands a conversation back to #ng-sales-goats after 12 h without Ron's
-- approval or when the lead needs a person. These columns record that, and a
-- handed-back conversation is never drafted again (the setters own it).
ALTER TABLE ig_guide_drafts ADD COLUMN IF NOT EXISTS handed_back_at timestamptz;
ALTER TABLE ig_guide_drafts ADD COLUMN IF NOT EXISTS handback_reason text;
CREATE INDEX IF NOT EXISTS ig_guide_drafts_handback_idx ON ig_guide_drafts (conversation_id) WHERE handed_back_at IS NOT NULL;
