-- 022_guardian_actions.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes this table (runGuardianCheck).
-- Without it every run fails its first read and nothing is drafted (fail closed).
--
-- Pipeline guardian, phase A (tasks/specs/pipeline-guardian-phase-a.md, section 8;
-- plan-of-record UPDATE 2026-10-24 "the pipeline guardian replaces the second
-- setter seat"). One row per action Max proposed for an Appointment Setting
-- lead: a drafted reply or strike (waits for the setter's ✅), a signal (handoff,
-- nothing to send), or a stop. The send is claimed through guardian_claim_send
-- (approved → sending, first writer wins) so a double ✅ never sends twice.
-- Rows carry no message body other than the draft: the lead's quote stays in
-- the Slack card, never here.
--
-- Statuses beyond the spec list: `sending` (the claim state the RPC produces),
-- `stopped` (a stop action, so the card is marked), `handled` (a signal the
-- setter ✅'d; signals are never sent, so `sent` would lie to the watchdog).

CREATE TABLE IF NOT EXISTS guardian_actions (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  contact_id        text NOT NULL,
  conversation_id   text NOT NULL,
  opportunity_id    text,
  contact_name      text,
  channel           text NOT NULL,                      -- whatsapp | instagram | facebook | email
  step              text NOT NULL,                      -- reply | clarify | strike_1..strike_5 | renurture_1..2 | signal | stop
  kind              text NOT NULL DEFAULT 'reply',      -- reply | strike | signal | stop
  intent            text,
  verdict           text,                               -- hot | normal | noise (hot-reply classifier)
  stage_id          text,
  mode              text NOT NULL DEFAULT 'shadow',     -- GUARDIAN_MODE at draft time: shadow | draft
  draft             text NOT NULL DEFAULT '',           -- the reply, strike, or signal suggestion
  problems          text,                               -- validation problems left after one reprompt
  status            text NOT NULL DEFAULT 'drafted'
                    CHECK (status IN ('drafted','signal','approved','sending','sent','rejected','expired','send_failed','skipped','stopped','handled')),
  lead_message_at   timestamptz,                        -- newest lead message answered (replies); null for strikes
  our_last_message_at timestamptz,                      -- our newest message when the strike was drafted
  window_closes_at  timestamptz,                        -- Meta 24 h window end; null for email
  slack_channel_id  text,
  slack_message_ts  text,
  approved_by       text,
  approved_at       timestamptz,
  sent_at           timestamptz,
  sent_text         text,
  edited            boolean,
  message_id        text,                               -- GHL message id of the send
  last_error        text,
  correlation_id    text,
  day               date NOT NULL DEFAULT ((now() AT TIME ZONE 'America/Costa_Rica')::date),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz
);

-- One action per conversation, step and CR day: a strike is never drafted
-- twice in a day, and two overlapping runs cannot DM the same card twice.
CREATE UNIQUE INDEX IF NOT EXISTS guardian_actions_convo_step_day_uq
  ON guardian_actions (conversation_id, step, day);
CREATE INDEX IF NOT EXISTS guardian_actions_status_idx   ON guardian_actions (status, created_at);
CREATE INDEX IF NOT EXISTS guardian_actions_contact_idx  ON guardian_actions (contact_id, created_at);
CREATE INDEX IF NOT EXISTS guardian_actions_day_idx      ON guardian_actions (day);

-- RLS on (spec section 8). index.js uses the ANON key, so the policies name it:
-- read, insert and update, never delete. CREATE TABLE via a migration does not
-- inherit dashboard grants (and after 2026-10-30 new tables get none), so the
-- grants are explicit, mirroring 019_ig_guide_drafts.sql.
ALTER TABLE guardian_actions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS guardian_actions_read   ON guardian_actions;
DROP POLICY IF EXISTS guardian_actions_insert ON guardian_actions;
DROP POLICY IF EXISTS guardian_actions_update ON guardian_actions;
CREATE POLICY guardian_actions_read   ON guardian_actions FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY guardian_actions_insert ON guardian_actions FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY guardian_actions_update ON guardian_actions FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE ON guardian_actions TO anon, authenticated;

-- Atomic send claim: flips approved → sending for exactly one caller and returns
-- the row; a second ✅ (or a second process) gets zero rows and sends nothing.
CREATE OR REPLACE FUNCTION guardian_claim_send(p_id uuid, p_actor text)
RETURNS SETOF guardian_actions
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  UPDATE guardian_actions
     SET status = 'sending',
         approved_by = COALESCE(approved_by, p_actor),
         approved_at = COALESCE(approved_at, now()),
         updated_at = now()
   WHERE id = p_id AND status = 'approved'
  RETURNING *;
$$;
REVOKE ALL ON FUNCTION guardian_claim_send(uuid, text) FROM public;
GRANT EXECUTE ON FUNCTION guardian_claim_send(uuid, text) TO anon, authenticated;
