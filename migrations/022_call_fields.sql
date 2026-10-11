-- 022_call_fields.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE setting CALL_FIELDS_FORM=on. Without it the form still writes GHL
-- and the portal, and the thread reply says Max's copy was not saved.
--
-- Call details a closer enters on the outcome card's "Call details" form after
-- logging follow_up (Ron, 2026-10-11, post-call nurture phase 2). One row per
-- appointment; a resubmission overwrites it. Also prefills the form next time.
-- appointment_id is the portal revops_appointments.id (other database, no FK).

CREATE TABLE IF NOT EXISTS call_fields (
  appointment_id  uuid        PRIMARY KEY,
  industry        text        NOT NULL CHECK (industry IN ('asesor','coach','tech','industrial','real_estate','salud','otro')),
  objection       text        NOT NULL CHECK (objection IN ('liquidez','socio','prueba','timing','otro')),
  ticket          integer     CHECK (ticket > 0),
  clients_missing integer     CHECK (clients_missing >= 0),
  next_step_date  date,
  submitted_by    text        NOT NULL,                -- Slack user id
  submitted_at    timestamptz NOT NULL DEFAULT now()
);

-- index.js uses the ANON key, and CREATE TABLE via a migration does not inherit
-- dashboard grants (013's first live run failed on exactly this). Max-owned,
-- RLS off like video_views. No DELETE.
GRANT SELECT, INSERT, UPDATE ON call_fields TO anon, authenticated;
