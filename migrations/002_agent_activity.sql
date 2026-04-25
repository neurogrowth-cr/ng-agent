-- migrations/002_agent_activity.sql
-- Audit log for every action Max takes. Written by the bot (anon key via RLS);
-- read by authenticated dashboards / ops review.
create extension if not exists pgcrypto;

create table if not exists public.agent_activity (
    id               uuid primary key default gen_random_uuid(),
    created_at       timestamptz not null default now(),
    agent            text        not null default 'max',
    actor_user_id    text,
    actor_name       text,
    channel_id       text,
    channel_name     text,
    thread_ts        text,
    event_type       text        not null,
    event_source     text,
    action           text        not null,
    status           text        not null default 'ok',
    duration_ms      integer,
    input            jsonb,
    output           jsonb,
    tool_name        text,
    model            text,
    tokens_in        integer,
    tokens_out       integer,
    error_message    text,
    error_stack      text,
    correlation_id   uuid,
    metadata         jsonb       not null default '{}'::jsonb
);

create index if not exists agent_activity_created_at_idx   on public.agent_activity (created_at desc);
create index if not exists agent_activity_actor_idx        on public.agent_activity (actor_user_id, created_at desc);
create index if not exists agent_activity_channel_idx      on public.agent_activity (channel_id, created_at desc);
create index if not exists agent_activity_event_type_idx   on public.agent_activity (event_type, created_at desc);
create index if not exists agent_activity_correlation_idx  on public.agent_activity (correlation_id);
create index if not exists agent_activity_status_idx       on public.agent_activity (status) where status <> 'ok';
create index if not exists agent_activity_metadata_gin     on public.agent_activity using gin (metadata);

create or replace function public.prune_agent_activity(retain_days int default 90)
returns bigint
language sql
as $$
    with deleted as (
        delete from public.agent_activity
        where created_at < now() - make_interval(days => retain_days)
        returning 1
    )
    select count(*) from deleted;
$$;

alter table public.agent_activity enable row level security;

create policy "anon insert agent_activity"
    on public.agent_activity
    for insert
    to anon
    with check (true);

create policy "authenticated read agent_activity"
    on public.agent_activity
    for select
    to authenticated
    using (true);

comment on table public.agent_activity is 'Audit trail of every action Max (the NeuroGrowth PM agent) performs.';
