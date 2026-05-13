use chrono::{DateTime, Days, Utc};
use color_eyre::eyre::{Result, bail};
use sqlx::{Sqlite, query, query_as};

use crate::{
    database::{Database, models::MeasurementEntry},
    types::ratio,
};

impl Database {
    #[tracing::instrument(skip(self))]
    pub async fn insert_measurement(&mut self, entry: MeasurementEntry) -> Result<()> {
        let result = query!(
            r#"
INSERT INTO measurements ( timestamp, stable, lazer )
VALUES ( ?1, ?2, ?3 )
            "#,
            entry.timestamp,
            entry.stable,
            entry.lazer
        )
        .execute(&*self)
        .await;

        let _ = match result {
            Ok(_) => tracing::info!("Inserted measurement"),
            Err(e)
                if e.as_database_error()
                    .is_some_and(|error| error.is_unique_violation()) =>
            {
                tracing::debug!("Skipped duplicate measurement")
            }
            Err(e) => bail!("Database error: {e}"),
        };
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_user_count_peak(&self) -> Result<MeasurementEntry> {
        tracing::debug!("Fetching peak lazer user count measurement");
        let result = query_as!(
            MeasurementEntry,
            r#"
SELECT *
FROM measurements
WHERE lazer = (SELECT MAX(lazer) FROM measurements)
            "#
        )
        .fetch_one(&*self)
        .await?;
        tracing::info!(
            stable = result.stable,
            lazer = result.lazer,
            "Found user peak at {}",
            result.timestamp
        );

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_user_ratio_peak(&self) -> Result<MeasurementEntry> {
        tracing::debug!("Fetching peak lazer ratio measurement");
        let result = query_as!(
            MeasurementEntry,
            r#"
SELECT *
FROM measurements
WHERE (CAST(lazer AS REAL) / (stable + lazer)) = (
    SELECT MAX(CAST(lazer AS REAL) / (stable + lazer))
    FROM measurements
    WHERE (stable + lazer) > 0
)
            "#
        )
        .fetch_one(&*self)
        .await?;
        tracing::info!(
            stable = result.stable,
            lazer = result.lazer,
            ratio = ratio(result.stable, result.lazer),
            "Found lazer% peak at {}",
            result.timestamp
        );

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_user_highest_percentile_peak(&self) -> Result<MeasurementEntry> {
        tracing::debug!("Fetching highest user count within peak ratio percentile");
        let max_percentage: f64 = sqlx::query_scalar::<Sqlite, f64>(
            r#"
            SELECT MAX(CAST(lazer AS REAL) / (stable + lazer))
            FROM measurements
            WHERE (stable + lazer) > 0
            "#,
        )
        .fetch_one(&*self)
        .await?;

        tracing::debug!(max_percentage, "Found maximum lazer ratio");

        let peak: MeasurementEntry = sqlx::query_as(
            r#"
            SELECT timestamp, stable, lazer
            FROM measurements
            WHERE (CAST(lazer AS REAL) / (stable + lazer)) >= ?
            ORDER BY lazer DESC
            LIMIT 1
            "#,
        )
        .bind(max_percentage - 0.015)
        .fetch_one(&*self)
        .await?;

        tracing::info!(
            timestamp = peak.timestamp,
            stable = peak.stable,
            lazer = peak.lazer,
            ratio = ratio(peak.stable, peak.lazer),
            "Found percentile-adjusted lazer ratio peak"
        );

        Ok(peak)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_history_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MeasurementEntry>> {
        let timestamp_start = start.timestamp();
        let timestamp_end = end.timestamp();
        tracing::debug!("Fetching measurement history range");

        let result = query_as!(
            MeasurementEntry,
            r#"
SELECT *
FROM measurements
WHERE timestamp >= ?1 AND timestamp < ?2
ORDER BY timestamp ASC
            "#,
            timestamp_start,
            timestamp_end
        )
        .fetch_all(&*self)
        .await?;

        tracing::info!(len = result.len(), "Fetched history data");
        Ok(result)
    }

    pub async fn get_past_day(&self) -> Result<Vec<MeasurementEntry>> {
        let now = Utc::now();
        let Some(start) = now.checked_sub_days(Days::new(1)) else {
            tracing::error!(
                timestamp = now.timestamp(),
                "Failed to calculate past-day start time"
            );
            bail!("Failed to calculate past-day start time");
        };

        self.get_history_range(start, now).await
    }
}
