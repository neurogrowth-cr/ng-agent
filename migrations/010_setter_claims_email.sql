-- migrations/010_setter_claims_email.sql
-- Capture the prospect's email on each setter claim so a claim can be matched to
-- an iClosed appointment by email with pure SQL (no live GHL lookup). This is the
-- go-forward join key feeding the setter_attributions reconciler — historical rows
-- (pre-launch) stay null and fall back to the GHL opp-owner resolver.
alter table public.setter_claims
    add column if not exists prospect_email text;

create index if not exists setter_claims_prospect_email_idx
    on public.setter_claims (lower(prospect_email));

comment on column public.setter_claims.prospect_email is
    'Prospect email captured at claim time (from lead-post metadata). Used to join setter -> iClosed appointment by email in the setter_attributions reconciler.';
