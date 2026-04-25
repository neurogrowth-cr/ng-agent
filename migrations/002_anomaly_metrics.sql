-- Phase 1 anomaly detection: time-series observations + rolling baselines.
-- Run once against the primary Supabase project (SUPABASE_URL, ng-agent).

CREATE TABLE IF NOT EXISTS metric_observations (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric      text NOT NULL,
  domain      text NOT NULL CHECK (domain IN ('marketing','sales','fulfillment','client_success')),
  value       numeric NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT now(),
  source      text,
  meta        jsonb
);

CREATE INDEX IF NOT EXISTS idx_metric_obs_metric_time
  ON metric_observations (metric, observed_at DESC);

CREATE TABLE IF NOT EXISTS metric_baselines (
  metric        text PRIMARY KEY,
  domain        text NOT NULL,
  mean          numeric NOT NULL,
  std_dev       numeric NOT NULL,
  sample_size   integer NOT NULL,
  window_days   integer NOT NULL DEFAULT 28,
  last_computed timestamptz NOT NULL DEFAULT now()
);
