-- migrations/005_campaign_sends.sql
-- Audit trail for messages sent via the recoverable-leads campaign flow.
-- Each row = one approved draft that Max sent to a prospect via GHL.
create extension if not exists pgcrypto;

create table if not exists public.campaign_sends (
    id                  uuid primary key default gen_random_uuid(),
    contact_id          text not null,
    contact_name        text,
    conversation_id     text,
    channel             text not null default 'WhatsApp',
    draft_text          text not null,
    sent_text           text,
    ghl_message_id      text,
    approved_by_slack_id text not null,
    approved_at         timestamptz not null default now(),
    sent_at             timestamptz,
    status              text not null default 'pending',
    error_message       text,
    correlation_id      uuid,
    created_at          timestamptz not null default now()
);

create index if not exists campaign_sends_contact_idx     on public.campaign_sends (contact_id);
create index if not exists campaign_sends_approved_at_idx on public.campaign_sends (approved_at desc);
create index if not exists campaign_sends_status_idx      on public.campaign_sends (status) where status <> 'sent';

alter table public.campaign_sends enable row level security;

create policy "anon insert campaign_sends"
    on public.campaign_sends
    for insert
    to anon
    with check (true);

create policy "anon update campaign_sends"
    on public.campaign_sends
    for update
    to anon
    using (true)
    with check (true);

create policy "authenticated read campaign_sends"
    on public.campaign_sends
    for select
    to authenticated
    using (true);

comment on table public.campaign_sends is 'Audit trail for approved campaign drafts sent via Max. status: pending|sent|failed|skipped.';
