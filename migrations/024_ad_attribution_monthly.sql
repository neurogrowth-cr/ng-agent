-- 024_ad_attribution_monthly.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes this table (buildMonthlyAdBlock).
-- Without it the monthly block still posts; only the history write logs an error.
--
-- Monthly ad attribution history (Ron, 2026-10-28). One row per CR month per ad
-- (ad_key = Meta ad id, or 'organic' for people whose first touch had no ad):
-- leads created that month, people who booked, people whose call was held,
-- closes and revenue credited to the first-touch ad, and Meta spend for the ad
-- in the month. Written by the first Weekly Recap of each month (last month)
-- and by scripts/ad-attribution-history.js (rebuilt months, spend as Meta
-- reports it today). Read by get_ad_performance (history) and the month delta.
-- Counts and money per ad only; no person's name or contact id.

CREATE TABLE IF NOT EXISTS ad_attribution_monthly (
  month        date    NOT NULL,              -- first day of the CR month
  ad_key       text    NOT NULL,              -- Meta ad id or 'organic'
  ad_id        text,
  ad_name      text,
  campaign     text,
  ad_set       text,
  leads        integer NOT NULL DEFAULT 0,
  booked       integer NOT NULL DEFAULT 0,
  held         integer NOT NULL DEFAULT 0,
  wins         integer NOT NULL DEFAULT 0,
  revenue      numeric NOT NULL DEFAULT 0,
  spend        numeric NOT NULL DEFAULT 0,
  computed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (month, ad_key)
);

-- RLS on, mirroring 023_max_backlog.sql. index.js uses the ANON key, so the
-- policies name it: read, insert and update, never delete. Grants are explicit
-- because migration-created tables do not inherit dashboard grants (and after
-- 2026-10-30 new tables get none).
ALTER TABLE ad_attribution_monthly ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ad_attribution_monthly_read   ON ad_attribution_monthly;
DROP POLICY IF EXISTS ad_attribution_monthly_insert ON ad_attribution_monthly;
DROP POLICY IF EXISTS ad_attribution_monthly_update ON ad_attribution_monthly;
CREATE POLICY ad_attribution_monthly_read   ON ad_attribution_monthly FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY ad_attribution_monthly_insert ON ad_attribution_monthly FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY ad_attribution_monthly_update ON ad_attribution_monthly FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE ON ad_attribution_monthly TO anon, authenticated;
