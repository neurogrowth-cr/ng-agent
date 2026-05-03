-- migrations/003_setter_claims.sql
-- Audit trail for setter lead claims via #ng-sales-goats reactions.
-- Records who claimed which lead, when, and how long it took (seconds_to_claim).
-- Used for SLA mining (median/p90/breach analysis) and leaderboard MTD counts.
create extension if not exists pgcrypto;

create table if not exists public.setter_claims (
    id                       uuid primary key default gen_random_uuid(),
    ghl_contact_id           text not null,
    contact_name             text,
    slack_message_ts         text not null,
    slack_channel_id         text not null,
    claimed_by_slack_user_id text not null,
    claimed_by_setter_name   text,
    ghl_user_id              text not null,
    opps_reassigned          int  not null default 0,
    claimed_at               timestamptz not null default now(),
    seconds_to_claim         int,
    created_at               timestamptz not null default now()
);

create index if not exists setter_claims_contact_idx     on public.setter_claims (ghl_contact_id);
create index if not exists setter_claims_setter_user_idx on public.setter_claims (claimed_by_slack_user_id);
create index if not exists setter_claims_claimed_at_idx  on public.setter_claims (claimed_at desc);
create index if not exists setter_claims_msg_ts_idx      on public.setter_claims (slack_message_ts);

alter table public.setter_claims enable row level security;

create policy "anon insert setter_claims"
    on public.setter_claims
    for insert
    to anon
    with check (true);

create policy "authenticated read setter_claims"
    on public.setter_claims
    for select
    to authenticated
    using (true);

comment on table public.setter_claims is 'Audit trail of setter lead claims via Slack reaction. seconds_to_claim is generated from Slack message ts (post time) vs claimed_at.';
