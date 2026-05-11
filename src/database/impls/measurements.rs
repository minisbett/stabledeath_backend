use chrono::{Days, NaiveDateTime, Utc};
use color_eyre::eyre::{Result, bail};
use sqlx::{Sqlite, query, query_as};

use crate::{
    database::{Database, models::MeasurementEntry},
    types::ratio,
};

impl Database {
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
            Ok(_) => (),
            Err(e) if e.as_database_error().unwrap().is_unique_violation() => (),
            Err(e) => bail!("Database error: {e}"),
        };
        tracing::info!(
            stable = entry.stable,
            lazer = entry.lazer,
            "Inserted measurement at {}",
            entry.timestamp
        );
        Ok(())
    }

    pub async fn get_user_count_peak(&self) -> Result<MeasurementEntry> {
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

    pub async fn get_user_ratio_peak(&self) -> Result<MeasurementEntry> {
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

    pub async fn get_user_highest_percentile_peak(&self) -> Result<MeasurementEntry> {
        let max_percentage: f64 = sqlx::query_scalar::<Sqlite, f64>(
            r#"
            SELECT MAX(CAST(lazer AS REAL) / (stable + lazer))
            FROM measurements
            WHERE (stable + lazer) > 0
            "#,
        )
        .fetch_one(&*self)
        .await?;

        let peak = sqlx::query_as::<_, _>(
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

        Ok(peak)
    }

    pub async fn get_history_range(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
    ) -> Result<Vec<MeasurementEntry>> {
        let result = query_as!(
            MeasurementEntry,
            r#"
SELECT *
FROM measurements
WHERE timestamp >= ?1 AND timestamp < ?2
ORDER BY timestamp DESC
            "#,
            start,
            end
        )
        .fetch_all(&*self)
        .await?;

        Ok(result)
    }

    pub fn get_past_day(&self) -> impl Future<Output = Result<Vec<MeasurementEntry>>> {
        self.get_history_range(
            Utc::now()
                .naive_local()
                .checked_sub_days(Days::new(1))
                .unwrap(),
            Utc::now().naive_local(),
        )
    }
}
