-- migrations/004_email_threads.sql
-- Tracks outbound email threads Max sent on behalf of sales team and
-- the watermark for reply-poll detection. One row per Gmail thread.
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).

CREATE TABLE IF NOT EXISTS public.email_threads (
  id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  gmail_thread_id          text NOT NULL UNIQUE,
  last_our_message_id      text NOT NULL,
  last_rfc822_message_id   text NOT NULL,
  rfc822_message_id_chain  text[] NOT NULL DEFAULT '{}',
  to_addresses             text[] NOT NULL,
  cc_addresses             text[] NOT NULL DEFAULT '{}',
  subject                  text NOT NULL,
  initiated_by_slack_id    text NOT NULL,
  last_message_at          timestamptz NOT NULL DEFAULT now(),
  active                   boolean NOT NULL DEFAULT true,
  created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_email_threads_active_setter
  ON public.email_threads (active, initiated_by_slack_id);

CREATE INDEX IF NOT EXISTS idx_email_threads_gmail_thread
  ON public.email_threads (gmail_thread_id);
