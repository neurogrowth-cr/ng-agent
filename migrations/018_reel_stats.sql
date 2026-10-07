-- 018_reel_stats.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes this table (runReelsWeekly).
-- A missing table only costs the history row (the Slack post still goes out),
-- but the week-over-week deltas depend on last week's `account` row existing.
--
-- Weekly snapshot of Instagram reels for the Monday #ng-content post
-- (recipe ~/automations/ops/recipes/reels-weekly.md). One row per reel per
-- ISO week (post_id = the Instagram media id) plus one `account` row per week
-- with the GHL Social Planner totals. Later joins: comments that said
-- "LinkedIn" (lead_posts / lead_reply_signals by week) against reach here.

CREATE TABLE IF NOT EXISTS reel_stats (
  id            bigserial PRIMARY KEY,
  iso_week      text        NOT NULL,                 -- '2026-W41'
  post_id       text        NOT NULL,                 -- Instagram media id, or 'account'
  ghl_post_id   text,                                 -- GHL child post _id, for tracing
  kind          text        NOT NULL DEFAULT 'reel',  -- 'reel' | 'account'
  hook          text,                                 -- first sentence of the caption
  permalink     text,
  published_at  timestamptz,
  likes         integer     NOT NULL DEFAULT 0,
  comments      integer     NOT NULL DEFAULT 0,
  shares        integer     NOT NULL DEFAULT 0,
  saved         integer,                              -- Meta only
  reach         integer,                              -- Meta per reel; GHL total on the account row
  views         integer,                              -- Meta per reel; GHL impressions on the account row
  followers     integer,                              -- account row only: new followers in the week
  captured_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (iso_week, post_id)
);

CREATE INDEX IF NOT EXISTS reel_stats_week_idx ON reel_stats (iso_week);

-- index.js uses the ANON key, and CREATE TABLE via a migration does not inherit
-- dashboard grants (013's first live run failed on exactly this). Max-owned,
-- RLS off like ghl_workflow_snapshots. Re-runs upsert on (iso_week, post_id);
-- nothing deletes. No DELETE.
GRANT SELECT, INSERT, UPDATE ON reel_stats TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE reel_stats_id_seq TO anon, authenticated;
