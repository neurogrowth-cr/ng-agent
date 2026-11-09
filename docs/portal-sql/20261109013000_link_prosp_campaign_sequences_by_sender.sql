-- Copy for reference. Source of truth: dash.neurogrowth.io supabase/migrations/20261109013000_link_prosp_campaign_sequences_by_sender.sql (dash #432).
-- Recorded in the portal's Supabase migration history as version 20261109030145.
--
-- Link campaign sequences to their client by LinkedIn sender, in the database.
--
-- Applied to production by hand on 2026-11-08/09 (Cristina Mora incident: her
-- LinkedIn handler was saved after the day's sequence sync, so her campaigns
-- stayed without a client until the next day's read). This file records what
-- already runs there, so it only uses CREATE OR REPLACE and DROP ... IF EXISTS
-- and is safe to apply again.
--
-- The sequence sync (npm run prosp:sequences) still decides the owner on every
-- run. These triggers are the safety net between runs:
--   * trg_link_sequence_customer fills an empty customer_id when a row is
--     written.
--   * trg_relink_sequences_on_handlers links waiting campaigns the moment a
--     handler is saved on a dashboard.
-- Rules: never replaces an existing customer_id, links only when exactly one
-- active client owns the sender, skips rows not seen for 2 days, and swallows
-- its own errors so a write to either table can never fail because of it.
--
-- No new table, view or sequence, so no grants are needed. Trigger functions
-- cannot be called through the API.
--
-- Rollback:
--   drop trigger if exists trg_link_sequence_customer on public.prosp_campaign_sequences;
--   drop trigger if exists trg_relink_sequences_on_handlers on public.client_dashboards;
--   drop function if exists public.ng_link_sequence_customer();
--   drop function if exists public.ng_relink_sequences_for_dashboard();
--   drop function if exists public.ng_dashboard_handler_slugs(text);

create or replace function public.ng_dashboard_handler_slugs(handlers text)
returns text[]
language sql
immutable
as $function$
  select coalesce(array_agg(distinct s) filter (where s <> ''), '{}')
  from (
    select lower(regexp_replace(
             regexp_replace(regexp_replace(trim(h), '/+$', ''), '^.*/(in|company)/', ''),
             '^@', '')) as s
    from unnest(string_to_array(coalesce(handlers, ''), ',')) h
  ) x;
$function$;

create or replace function public.ng_link_sequence_customer()
returns trigger
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_customer uuid;
  v_n int;
begin
  if new.customer_id is not null
     or new.sender_keys is null or cardinality(new.sender_keys) = 0
     or new.last_seen_at is null or new.last_seen_at < now() - interval '2 days' then
    return new;
  end if;

  select count(distinct cd.id), min(cd.id::text)::uuid
    into v_n, v_customer
  from client_dashboards cd
  where cd.is_active
    and public.ng_dashboard_handler_slugs(cd.linkedin_handlers)
        && (select array_agg(lower(k)) from unnest(new.sender_keys) k);

  if v_n = 1 then
    new.customer_id := v_customer;
  end if;
  return new;
exception when others then
  return new;
end;
$function$;

create or replace function public.ng_relink_sequences_for_dashboard()
returns trigger
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_slugs text[];
begin
  if not new.is_active then
    return new;
  end if;
  v_slugs := public.ng_dashboard_handler_slugs(new.linkedin_handlers);
  if cardinality(v_slugs) = 0 then
    return new;
  end if;

  update prosp_campaign_sequences s
     set customer_id = new.id, updated_at = now()
   where s.customer_id is null
     and s.last_seen_at >= now() - interval '2 days'
     and (select array_agg(lower(k)) from unnest(s.sender_keys) k) && v_slugs
     and not exists (
       select 1 from client_dashboards o
       where o.is_active and o.id <> new.id
         and public.ng_dashboard_handler_slugs(o.linkedin_handlers) && v_slugs
     );
  return new;
exception when others then
  return new;
end;
$function$;

drop trigger if exists trg_link_sequence_customer on public.prosp_campaign_sequences;
create trigger trg_link_sequence_customer
  before insert or update on public.prosp_campaign_sequences
  for each row execute function public.ng_link_sequence_customer();

drop trigger if exists trg_relink_sequences_on_handlers on public.client_dashboards;
create trigger trg_relink_sequences_on_handlers
  after insert or update of linkedin_handlers, is_active on public.client_dashboards
  for each row execute function public.ng_relink_sequences_for_dashboard();
