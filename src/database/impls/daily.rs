use color_eyre::eyre::Result;
use sqlx::query_as;

use crate::{
    database::{Database, models::DailyEntry},
    types::{PointLineResponse, ratio},
};

impl Database {
    pub async fn get_history(&self) -> Result<Vec<DailyEntry>> {
        let rows = query_as!(
            DailyEntry,
            r#"
SELECT date, stable, lazer
FROM daily
ORDER BY date ASC
            "#
        )
        .fetch_all(&*self)
        .await?;

        Ok(rows)
    }
}
