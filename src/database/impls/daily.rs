use color_eyre::eyre::{Result, bail};
use sqlx::query_as;

use crate::{
    database::{
        Database,
        models::{DailyEntry, RatioRegression},
    },
    types::{BucketSize, ratio},
};

const SECONDS_PER_DAY: f64 = 86_400.0;

impl Database {
    #[tracing::instrument(skip(self))]
    pub async fn get_history(&self, bucket_size: BucketSize) -> Result<Vec<DailyEntry>> {
        tracing::debug!("Fetching daily history rows");
        let rows = match bucket_size {
            BucketSize::Day => query_as::<_, DailyEntry>(
                r#"
                SELECT
                    -- Coalesce to remove nullchecks
                    COALESCE(EXTRACT(EPOCH FROM day_bucket)::BIGINT, 1702252800) AS date,
                    COALESCE(stable_avg, 0) AS stable,
                    COALESCE(lazer_avg, 0) AS lazer
                FROM changelog_counts_daily_aggregate
                ORDER BY day_bucket ASC
                    "#,
            ),
            BucketSize::Week => query_as::<_, DailyEntry>(
                r#"
                SELECT
                    -- Coalesce to remove nullchecks
                    COALESCE(EXTRACT(EPOCH FROM time_bucket('1 week', day_bucket))::BIGINT, 1702252800) AS date,
                    COALESCE(AVG(stable_avg)::BIGINT, 0) AS stable,
                    COALESCE(AVG(lazer_avg)::BIGINT, 0) AS lazer
                FROM changelog_counts_daily_aggregate
                GROUP BY time_bucket('1 week', day_bucket)
                ORDER BY date ASC
                    "#,
            ),
            BucketSize::Month => todo!(),
        };
        let rows = rows.fetch_all(&*self).await?;

        tracing::info!(rows = rows.len(), "Fetched daily history rows");

        Ok(rows)
    }

    #[tracing::instrument(skip(self))]
    pub async fn estimate_ratio_percentage(
        &self,
        target_percentage: f64,
    ) -> Result<RatioRegression> {
        tracing::debug!(
            target_percentage,
            "Estimating ratio target from daily history"
        );
        let entries = self.get_history(BucketSize::Day).await?;
        let regression = calculate_ratio_regression(entries, target_percentage)?;

        tracing::info!(
            target_percentage,
            estimated_timestamp = regression.estimated_timestamp,
            "Estimated ratio target from daily regression"
        );

        Ok(regression)
    }
}

fn calculate_ratio_regression(
    entries: Vec<DailyEntry>,
    target_percentage: f64,
) -> Result<RatioRegression> {
    let target_ratio = target_percentage / 100.0;

    if let Some(entry) = entries
        .iter()
        .filter(|entry| entry.stable + entry.lazer > 0)
        .find(|entry| ratio(entry.stable, entry.lazer) >= target_ratio)
    {
        tracing::info!(
            target_ratio,
            estimated_timestamp = entry.date,
            "Found observed daily ratio target crossing"
        );

        return Ok(RatioRegression {
            target_ratio,
            was_reached: true,
            estimated_timestamp: entry.date,
        });
    }

    let valid_entries: Vec<&DailyEntry> = entries
        .iter()
        .filter(|e| e.stable + e.lazer > 0)
        .collect();

    if valid_entries.is_empty() {
        bail!("Cannot estimate ratio target without valid entries");
    }
    let len = valid_entries.len();
    let start = len.saturating_sub(30);
    let valid_entries = &valid_entries[start..];

    if valid_entries.len() < 2 {
        bail!("Cannot estimate ratio target with fewer than two valid entries");
    }

    tracing::info!(
        used_points = valid_entries.len(),
        "Using last 30 days for logistic regression"
    );

    let first_timestamp = valid_entries[0].date;
    let mut points = Vec::with_capacity(valid_entries.len());

    for entry in valid_entries {
        let p = ratio(entry.stable, entry.lazer);

        let p = p.clamp(EPS, 1.0 - EPS);
        let t = (entry.date - first_timestamp) as f64 / SECONDS_PER_DAY;

        let logit = (p / (1.0 - p)).ln();

        points.push((t, logit));
    }

    let n = points.len() as f64;

    let sum_x: f64 = points.iter().map(|(x, _)| *x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| *y).sum();
    let sum_xx: f64 = points.iter().map(|(x, _)| x * x).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();

    let denominator = n * sum_xx - sum_x * sum_x;

    if denominator.abs() <= f64::EPSILON {
        bail!("Cannot estimate ratio target because daily timestamps have no variance");
    }

    let k = (n * sum_xy - sum_x * sum_y) / denominator;
    let c = (sum_y - k * sum_x) / n;

    if k.abs() <= f64::EPSILON {
        bail!("Cannot estimate ratio target because the ratio trend is flat");
    }

    let t_50 = -c / k;

    let estimated_timestamp =
        first_timestamp as f64 + t_50 * SECONDS_PER_DAY;

    if !estimated_timestamp.is_finite()
        || estimated_timestamp < i64::MIN as f64
        || estimated_timestamp > i64::MAX as f64
    {
        bail!("Estimated ratio target timestamp is outside the supported range");
    }

    Ok(RatioRegression {
        target_ratio,
        was_reached: false,
        estimated_timestamp: estimated_timestamp.round() as i64,
    })
}
