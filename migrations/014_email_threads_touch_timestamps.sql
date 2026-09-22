-- 014_email_threads_touch_timestamps.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes these columns: executeEmailSend
-- inserts last_outbound_at inside its try block, so a missing column would
-- report "Send failed" to the setter for an email that actually went out.
--
-- Lets the auto strike mover count emails setters send through Max (email
-- proxy, Gmail) as setter touches. last_message_at is not usable for that: it
-- advances on both our sends and the lead's replies, so direction is lost.
--   last_outbound_at  newest email Max sent on this thread (insert + reply paths)
--   last_inbound_at   newest lead reply the hourly poller saw
-- Backfill last_outbound_at from created_at only: a row exists only because an
-- email was sent. last_inbound_at stays null for history; only the last 24h of
-- inbound matter to the silence rule, so the gap closes itself within a day.

ALTER TABLE public.email_threads
  ADD COLUMN IF NOT EXISTS last_outbound_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_inbound_at  timestamptz;

UPDATE public.email_threads
   SET last_outbound_at = created_at
 WHERE last_outbound_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_email_threads_last_outbound
  ON public.email_threads (last_outbound_at DESC);
