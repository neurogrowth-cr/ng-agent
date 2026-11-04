-- 025_clarity_brief.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Apply BEFORE setting CLARITY_BRIEF_MODE=live. With the mode off nothing reads
-- these tables, so the code can merge first.
--
-- Clarity Brief (Ron, 2026-11-03; spec in tasks/specs/clarity-brief.md).
-- One DM from Max to Ron every weekday at 06:50 CR with the items that need him,
-- his projects in flight, and what was scanned and dropped. Three tables:
--   clarity_runs        one row per run (posted or failed); the 07:15 divergence
--                       check reads it, and the next run takes its window from it
--   clarity_items       one row per item shown, keyed by the Slack ts of its
--                       thread reply so the reaction handler can find it
--   clarity_dismissals  signatures Ron said "never again" to, with a count;
--                       muted after three in 30 days
-- Titles are short and may name a sender or a Slack channel; no email bodies,
-- no financial figures, no lead contact data are stored here.

CREATE TABLE IF NOT EXISTS clarity_runs (
  id            bigserial PRIMARY KEY,
  run_date      date        NOT NULL,            -- CR calendar day of the run
  started_at    timestamptz NOT NULL DEFAULT now(),
  posted_at     timestamptz,                     -- null until the DM is out
  window_from   timestamptz NOT NULL,
  window_to     timestamptz NOT NULL,
  parent_ts     text,                            -- Slack ts of the parent DM
  counts        jsonb       NOT NULL DEFAULT '{}'::jsonb, -- per source: scanned, kept, failed
  items_shown   integer     NOT NULL DEFAULT 0,
  projects      integer     NOT NULL DEFAULT 0,
  verdict       text,                            -- ✅ ALL GREEN: ... | ⚠️ ISSUES: ...
  error         text,
  cost_usd      numeric
);
CREATE INDEX IF NOT EXISTS clarity_runs_run_date_idx ON clarity_runs (run_date DESC);

CREATE TABLE IF NOT EXISTS clarity_items (
  id            bigserial PRIMARY KEY,
  run_id        bigint      NOT NULL REFERENCES clarity_runs(id) ON DELETE CASCADE,
  ts            text,                            -- Slack ts of the item's thread reply
  channel       text,                            -- Slack channel id the reply lives in (Ron's DM)
  dedupe_key    text        NOT NULL,            -- stable per underlying thing (message id, PR url, follow-up text hash)
  signature     text        NOT NULL,            -- source + sender or channel + topic key, for dismissals
  source        text        NOT NULL,            -- slack | max | gmail_ron | gmail_contact | calendar | github | wiki
  tier          smallint    NOT NULL,            -- 1 revenue now, 2 operation health, 3 hygiene
  title         text        NOT NULL,
  url           text,
  status        text        NOT NULL DEFAULT 'shown', -- shown | accepted | done | dismissed | snoozed | expired
  shown_at      timestamptz NOT NULL DEFAULT now(),
  accepted_at   timestamptz,
  done_at       timestamptz,
  snoozed_until timestamptz,
  reacted_by    text,
  CONSTRAINT clarity_items_status_check CHECK (status IN ('shown','accepted','done','dismissed','snoozed','expired'))
);
CREATE UNIQUE INDEX IF NOT EXISTS clarity_items_ts_idx ON clarity_items (ts) WHERE ts IS NOT NULL;
CREATE INDEX IF NOT EXISTS clarity_items_dedupe_idx ON clarity_items (dedupe_key, shown_at DESC);
CREATE INDEX IF NOT EXISTS clarity_items_status_idx ON clarity_items (status);

CREATE TABLE IF NOT EXISTS clarity_dismissals (
  signature     text        PRIMARY KEY,
  source        text        NOT NULL,
  count         integer     NOT NULL DEFAULT 1,
  muted         boolean     NOT NULL DEFAULT false,
  first_at      timestamptz NOT NULL DEFAULT now(),
  last_at       timestamptz NOT NULL DEFAULT now(),
  sample_title  text                              -- the last dismissed title, for the noise line
);

-- RLS on, mirroring 023_max_backlog.sql and 024. index.js uses the ANON key, so
-- the policies name it: read, insert and update, never delete. Grants are
-- explicit because migration-created tables get none after 2026-10-30.
ALTER TABLE clarity_runs        ENABLE ROW LEVEL SECURITY;
ALTER TABLE clarity_items       ENABLE ROW LEVEL SECURITY;
ALTER TABLE clarity_dismissals  ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS clarity_runs_read   ON clarity_runs;
DROP POLICY IF EXISTS clarity_runs_insert ON clarity_runs;
DROP POLICY IF EXISTS clarity_runs_update ON clarity_runs;
CREATE POLICY clarity_runs_read   ON clarity_runs FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY clarity_runs_insert ON clarity_runs FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY clarity_runs_update ON clarity_runs FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS clarity_items_read   ON clarity_items;
DROP POLICY IF EXISTS clarity_items_insert ON clarity_items;
DROP POLICY IF EXISTS clarity_items_update ON clarity_items;
CREATE POLICY clarity_items_read   ON clarity_items FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY clarity_items_insert ON clarity_items FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY clarity_items_update ON clarity_items FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS clarity_dismissals_read   ON clarity_dismissals;
DROP POLICY IF EXISTS clarity_dismissals_insert ON clarity_dismissals;
DROP POLICY IF EXISTS clarity_dismissals_update ON clarity_dismissals;
CREATE POLICY clarity_dismissals_read   ON clarity_dismissals FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY clarity_dismissals_insert ON clarity_dismissals FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICY clarity_dismissals_update ON clarity_dismissals FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE ON clarity_runs, clarity_items, clarity_dismissals TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE clarity_runs_id_seq, clarity_items_id_seq TO anon, authenticated;
