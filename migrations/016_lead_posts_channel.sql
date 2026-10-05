-- 016_lead_posts_channel.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes `channel`: the intake webhook
-- upserts it on every lead_posts row, and a missing column makes that upsert
-- fail, which loses the row (the Slack post still goes out, the row does not).
--
-- lead_posts.source lumps WhatsApp, Instagram and Messenger together as
-- "Social media", so a silence on one of them is invisible while the others
-- keep flowing (the 2026-10-02/03 Instagram intake gap). `channel` is derived
-- at intake from the GHL contact's attributionSource.medium by
-- lib/leadVolume.js leadChannel():
--   fb_form | whatsapp | instagram | messenger | other
-- Null on rows written before this column existed; the watchdog falls back to
-- `source` for those (which only recovers fb_form).

ALTER TABLE public.lead_posts
  ADD COLUMN IF NOT EXISTS channel text;
-- No new index: the hourly 14-day read uses the existing lead_posts_posted_at_idx.
