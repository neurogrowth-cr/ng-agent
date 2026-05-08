-- migrations/008_prospect_followups.sql
-- Audit trail for auto-sent stalled-prospect re-engagement messages.
-- One row per attempt (sent or failed). Used by the Mon–Fri 11 AM CR cron to
-- enforce a 14-day per-contact cooldown and a 2-lifetime cap before sending again.
create extension if not exists pgcrypto;

create table if not exists public.prospect_followups (
    id                  uuid primary key default gen_random_uuid(),
    contact_id          text not null,
    contact_name        text,
    conversation_id     text,
    channel             text not null default 'WhatsApp',
    message             text not null,
    ghl_message_id      text,
    setter_slack_id     text not null,
    attempt_n           int  not null default 1,
    sent_at             timestamptz not null default now(),
    status              text not null default 'sent',
    error_message       text,
    correlation_id      uuid,
    created_at          timestamptz not null default now()
);

create index if not exists prospect_followups_contact_idx  on public.prospect_followups (contact_id, sent_at desc);
create index if not exists prospect_followups_sent_at_idx  on public.prospect_followups (sent_at desc);
create index if not exists prospect_followups_status_idx   on public.prospect_followups (status) where status <> 'sent';

alter table public.prospect_followups enable row level security;

create policy "anon insert prospect_followups"
    on public.prospect_followups
    for insert
    to anon
    with check (true);

create policy "anon update prospect_followups"
    on public.prospect_followups
    for update
    to anon
    using (true)
    with check (true);

create policy "authenticated read prospect_followups"
    on public.prospect_followups
    for select
    to authenticated
    using (true);

comment on table public.prospect_followups is 'Audit trail for auto-sent stalled-prospect WhatsApp re-engagement messages. status: sent|failed.';
