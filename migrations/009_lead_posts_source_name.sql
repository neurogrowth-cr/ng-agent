-- migrations/009_lead_posts_source_name.sql
-- Adds source + full_name columns to lead_posts so handleGHLWebhook can
-- detect the FB-Lead-Form → WhatsApp duplicate-contact signature:
--   first post: Source=Facebook, full name + email
--   second post (seconds-minutes later): Source=Paid Social (or non-FB), nickname only, no email
-- Phone+email dedup misses this case (the WA-spawned contact has a different phone
-- and no email), so we need name+source fuzzy match as a secondary check.

alter table public.lead_posts
    add column if not exists source        text,
    add column if not exists full_name     text,
    add column if not exists name_prefix3  text;

create index if not exists lead_posts_name_prefix3_posted_at_idx
    on public.lead_posts (name_prefix3, posted_at desc)
    where name_prefix3 is not null;

comment on column public.lead_posts.source       is 'Source field as posted to Slack — Facebook, Paid Social, etc.';
comment on column public.lead_posts.full_name    is 'Lead full name as posted to Slack.';
comment on column public.lead_posts.name_prefix3 is 'Lowercased first-3-chars of first name — fuzzy dedup key for FB→WA contact pairs.';
