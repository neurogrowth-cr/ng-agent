-- migrations/006_lead_posts.sql
-- Lookup table mapping a GHL contact_id to the Slack message ts of the
-- corresponding #ng-sales-goats lead post. Used by the reverse-mirror
-- /webhook/ghl-claim endpoint so when a setter assigns a lead in GHL
-- directly, Max can find and update the corresponding Slack message.
create extension if not exists pgcrypto;

create table if not exists public.lead_posts (
    id               uuid primary key default gen_random_uuid(),
    contact_id       text not null unique,
    slack_message_ts text not null,
    slack_channel_id text not null,
    posted_at        timestamptz not null default now()
);

create index if not exists lead_posts_msg_ts_idx on public.lead_posts (slack_message_ts);
create index if not exists lead_posts_posted_at_idx on public.lead_posts (posted_at desc);

alter table public.lead_posts enable row level security;

create policy "anon insert lead_posts"
    on public.lead_posts
    for insert
    to anon
    with check (true);

create policy "anon read lead_posts"
    on public.lead_posts
    for select
    to anon
    using (true);

create policy "authenticated read lead_posts"
    on public.lead_posts
    for select
    to authenticated
    using (true);

comment on table public.lead_posts is 'Maps GHL contact_id → Slack lead-post ts so Max can find and update the post when a contact is reassigned in GHL.';
