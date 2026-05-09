-- migrations/008_lead_posts_dedup.sql
-- Adds normalized phone + email columns to lead_posts so handleGHLWebhook can
-- skip posting a second Slack message when GHL spawns a duplicate contact for
-- the same human (typical pattern: FB Lead Form workflow + Paid Social workflow
-- both create a contact off the same Meta Lead Ads event, seconds apart).

alter table public.lead_posts
    add column if not exists phone_last10 text,
    add column if not exists email_lower  text;

create index if not exists lead_posts_phone_last10_posted_at_idx
    on public.lead_posts (phone_last10, posted_at desc)
    where phone_last10 is not null;

create index if not exists lead_posts_email_lower_posted_at_idx
    on public.lead_posts (email_lower, posted_at desc)
    where email_lower is not null;

comment on column public.lead_posts.phone_last10 is 'Last 10 digits of the phone — used for cross-contact dedup of GHL duplicate-contact spam.';
comment on column public.lead_posts.email_lower  is 'Lowercased email — used for cross-contact dedup of GHL duplicate-contact spam.';
