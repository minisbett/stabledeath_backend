-- no-transaction
-- Add up migration script here
DROP TABLE IF EXISTS daily;

-- I forgot to set chunk size so this sucks
CREATE TABLE measurements_new (
  time TIMESTAMPTZ NOT NULL PRIMARY KEY,
  stable BIGINT NOT NULL,
  lazer BIGINT NOT NULL
);

SELECT create_hypertable(
  'measurements_new',
  by_range('time', INTERVAL '1 day'),
  if_not_exists => TRUE
);

-- Copy data
INSERT INTO measurements_new
SELECT * FROM measurements;

-- Swap tables
DROP TABLE measurements;
ALTER TABLE measurements_new RENAME TO measurements;

-- daily peak
CREATE MATERIALIZED VIEW changelog_counts_daily_aggregate
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day_bucket,
    MAX(stable) as stable_peak,
    MAX(lazer) as lazer_peak,
    CAST(AVG(stable) AS BIGINT) as stable_avg,
    CAST(AVG(lazer) AS BIGINT) as lazer_avg,
    MIN(stable) as stable_min,
    MIN(lazer) as lazer_min
FROM measurements
GROUP BY day_bucket WITH NO DATA;

SELECT add_continuous_aggregate_policy('changelog_counts_daily_aggregate',
    start_offset => INTERVAL '50 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    initial_start => '2026-05-23 00:00:00',
    timezone => 'UTC');
