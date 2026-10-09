-- 021_video_views.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE deploying the code that writes this table (runOrganicViewsSnapshot).
-- Without it the nightly snapshot fails its upsert and DMs Ron; the reels posts
-- then show "sin datos de vistas" instead of a number.
--
-- Organic views across Instagram, YouTube and TikTok (Ron, 2026-10-09). One row
-- per video per CR day with that video's LIFETIME views at ~23:50 CR. Views
-- gained in a week or month = growth of these totals (lib/organicViews.js).
-- Nothing deletes; re-runs on the same day upsert.

CREATE TABLE IF NOT EXISTS video_views (
  id             bigserial PRIMARY KEY,
  snapshot_date  date        NOT NULL,                 -- CR calendar day
  platform       text        NOT NULL CHECK (platform IN ('instagram','youtube','tiktok')),
  video_id       text        NOT NULL,                 -- IG media id, YouTube video id, TikTok video id
  views          bigint      NOT NULL,                 -- lifetime views at capture time
  published_at   timestamptz,
  title          text,                                 -- first caption line or video title
  captured_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (snapshot_date, platform, video_id)
);

CREATE INDEX IF NOT EXISTS video_views_date_idx ON video_views (snapshot_date);

-- index.js uses the ANON key, and CREATE TABLE via a migration does not inherit
-- dashboard grants (013's first live run failed on exactly this). Max-owned,
-- RLS off like reel_stats. No DELETE.
GRANT SELECT, INSERT, UPDATE ON video_views TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE video_views_id_seq TO anon, authenticated;
