-- Add migration script here
CREATE TABLE `daily` (
	`date` integer PRIMARY KEY NOT NULL,
	`stable` integer NOT NULL,
	`lazer` integer NOT NULL
);
CREATE UNIQUE INDEX `idx_date` ON `daily` (`date`,`stable`,`lazer`);
CREATE TABLE "measurements"(
  timestamp,
  stable INT,
  lazer INT
);
CREATE UNIQUE INDEX idx_measurements_ts
ON "measurements" (timestamp);
CREATE UNIQUE INDEX `idx_timestamp` ON `measurements` (`timestamp`,`stable`,`lazer`);
