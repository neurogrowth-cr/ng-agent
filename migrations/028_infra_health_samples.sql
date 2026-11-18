-- 028_infra_health_samples.sql
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).
-- There is no migration runner in this repo; apply by hand or via the Supabase MCP.
-- Applied 2026-11-18 with Ron's OK.
--
-- Infrastructure history (Ron, 2026-11-18). The dash infra-health check
-- (dash src/lib/infra-health/history.ts, every 5 minutes) saves one reading per
-- database for the charts on dash Admin > Infrastructure. The same table lives
-- in the portal project (dash migration 20261118010000). Each database's
-- readings are written to the OTHER project first, so this table mostly holds
-- the PORTAL's readings (and Max's own only when the portal refuses them).
-- Max itself never reads or writes it. Server numbers only: no client, lead or
-- financial data. Kept 13 months; the check deletes older rows daily.
--
-- Reversible: DROP TABLE infra_health_samples;

CREATE TABLE IF NOT EXISTS infra_health_samples (
  sampled_at          timestamptz NOT NULL,
  target              text        NOT NULL CHECK (target IN ('portal', 'max')),
  answering           boolean     NOT NULL,
  probe_ms            integer,
  cpus                smallint,
  cpu_total_seconds   double precision,
  cpu_idle_seconds    double precision,
  cpu_iowait_seconds  double precision,
  cpu_steal_seconds   double precision,
  load15              real,
  mem_total_bytes     bigint,
  mem_available_bytes bigint,
  swap_used_bytes     bigint,
  disk_size_bytes     bigint,
  disk_avail_bytes    bigint,
  database_bytes      bigint,
  connections         integer,
  max_connections     integer,
  PRIMARY KEY (sampled_at, target)
);

COMMENT ON TABLE infra_health_samples IS
  'Server numbers per database every 5 minutes from the dash infra-health check, for the Infrastructure charts. CPU counters are cumulative since boot; busy % is the change between readings.';

-- The dash writes with this project's service role key (INFRA_HEALTH_MAX_SERVICE_KEY).
ALTER TABLE infra_health_samples ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, DELETE ON infra_health_samples TO service_role;
