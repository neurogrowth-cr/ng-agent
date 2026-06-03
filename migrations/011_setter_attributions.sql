-- migrations/011_setter_attributions.sql
-- ng-agent-owned setter -> iClosed-call attribution map.
--
-- WHY: iClosed never sends a setter. revops_appointments.setter_id and
-- revops_prospects.setter_owner_id are NULL on 100% of appointments, so the
-- portal/iClosed world cannot tell us who booked a call. The only setter truth
-- lives in ng-agent (setter_claims + GHL opportunity owners). We can't reliably
-- write back to the portal project (separate Supabase, anon key + RLS), so the
-- setter->call link lives here and is JOINed into reports by iclosed_call_id.
--
-- Populated go-forward by runSetterAttributionReconcile():
--   resolved_via = 'claim_email' -> matched setter_claims.prospect_email = prospect.email
--   resolved_via = 'ghl_opp'     -> resolveSetterForContact() (live GHL opp owner)
--   resolved_via = 'unresolved'  -> no setter found (self-booked / unclaimed)
create extension if not exists pgcrypto;

create table if not exists public.setter_attributions (
    id              uuid primary key default gen_random_uuid(),
    iclosed_call_id text not null unique,
    prospect_email  text,
    ghl_contact_id  text,
    setter_name     text,
    resolved_via    text not null default 'unresolved',  -- claim_email | ghl_opp | unresolved
    confidence      text not null default 'low',         -- high | medium | low
    resolved_at     timestamptz,
    created_at      timestamptz not null default now()
);

create index if not exists setter_attributions_setter_idx on public.setter_attributions (setter_name);
create index if not exists setter_attributions_email_idx  on public.setter_attributions (lower(prospect_email));

alter table public.setter_attributions enable row level security;

-- ng-agent's Supabase clients use the ANON key. setter_claims established the
-- pattern: explicit anon insert + anon read; the reconciler also needs anon
-- update for its upsert path. Without these the writes fail silently.
create policy "anon insert setter_attributions"
    on public.setter_attributions for insert to anon with check (true);

create policy "anon update setter_attributions"
    on public.setter_attributions for update to anon using (true) with check (true);

create policy "anon read setter_attributions"
    on public.setter_attributions for select to anon using (true);

create policy "authenticated read setter_attributions"
    on public.setter_attributions for select to authenticated using (true);

comment on table public.setter_attributions is
    'Setter -> iClosed call attribution, owned by ng-agent because iClosed/portal never captures the setter. Keyed by iclosed_call_id; JOINed into setter leaderboard stats. Populated by runSetterAttributionReconcile.';
