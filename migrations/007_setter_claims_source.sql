-- migrations/007_setter_claims_source.sql
-- Add a column to distinguish whether a claim came from a Slack reaction
-- or from a direct GHL UI assignment (mirrored back via /webhook/ghl-claim).
-- Existing rows backfill to 'slack_reaction' (the only path before this).

alter table public.setter_claims
    add column if not exists claim_source text not null default 'slack_reaction';

create index if not exists setter_claims_source_idx on public.setter_claims (claim_source);

comment on column public.setter_claims.claim_source is 'Where the claim originated: slack_reaction (✋/✅ in #ng-sales-goats) or ghl_direct (assigned in GHL UI, mirrored via /webhook/ghl-claim).';
