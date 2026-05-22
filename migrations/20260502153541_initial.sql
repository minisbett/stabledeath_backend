-- Add migration script here
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS measurements (
  "timestamp" TIMESTAMPTZ NOT NULL PRIMARY KEY,
  stable BIGINT NOT NULL,
  lazer BIGINT NOT NULL
);
SELECT create_hypertable('measurements', by_range('timestamp'), if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS daily (
  "date" TIMESTAMPTZ NOT NULL PRIMARY KEY,
  stable BIGINT NOT NULL,
  lazer BIGINT NOT NULL
);
SELECT create_hypertable('daily', by_range('date'), if_not_exists => TRUE);
