-- migrations/012_strike_sweep_read.sql
-- Nightly learning aggregates auto-strike-mover sweeps out of agent_activity,
-- but portalSupabase (index.js) holds the anon key and agent_activity is
-- select-to-authenticated only. Scope: ONLY strike_sweep rows — the rest of
-- the audit log stays authenticated-only.
create policy "anon read strike sweeps"
    on public.agent_activity
    for select to anon
    using (event_type = 'strike_sweep');
