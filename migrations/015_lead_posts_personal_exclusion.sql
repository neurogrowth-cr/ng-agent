-- 015_lead_posts_personal_exclusion.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that reads personal_excluded_at: the lead
-- count and nag queries filter on it, and a missing column makes them error.
--
-- Ron's brand accounts (Messenger, Instagram) also get DMs from friends. GHL
-- turns every first DM into a contact + New Lead card, and the intake webhook
-- posts it to Slack and writes a lead_posts row. Tagging the GHL contact
-- `personal` takes it out of the sales flow; this is the lead_posts half:
--   personal_excluded_at   set when the contact was tagged personal. Every
--                          lead count and stale-lead nag skips flagged rows.
-- Soft flag, not a delete: reversible, and the row stays as an audit trail.
--
-- anon (Max's key) only has insert + select on lead_posts. Instead of a broad
-- update policy, one SECURITY DEFINER function that can only set this flag,
-- only for one contact, and returns the Slack posts it touched so Max can
-- replace them with a one-line note.

ALTER TABLE public.lead_posts
  ADD COLUMN IF NOT EXISTS personal_excluded_at timestamptz;

CREATE OR REPLACE FUNCTION public.mark_lead_posts_personal(p_contact_id text)
RETURNS TABLE (slack_message_ts text, slack_channel_id text)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  UPDATE public.lead_posts AS lp
     SET personal_excluded_at = now()
   WHERE lp.contact_id = p_contact_id
     AND lp.personal_excluded_at IS NULL
  RETURNING lp.slack_message_ts::text, lp.slack_channel_id::text;
$$;

REVOKE ALL ON FUNCTION public.mark_lead_posts_personal(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.mark_lead_posts_personal(text) TO anon;
