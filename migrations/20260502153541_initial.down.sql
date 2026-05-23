-- Add migration script here
DROP TABLE IF EXISTS daily;
DROP TABLE IF EXISTS measurements;

DROP EXTENSION IF EXISTS timescaledb;
