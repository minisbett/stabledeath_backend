use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::database::models::MeasurementEntry;

/// Calculate ratio between stable and lazer user counts
pub fn ratio(stable: i64, lazer: i64) -> f64 {
    let (stable, lazer) = (stable as f64, lazer as f64);
    lazer / (stable + lazer)
}

#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub enum BucketSize {
    #[default]
    Day,
    Week,
    Month,
}

/// Changelog API entry, simplified
#[derive(Serialize, Debug, Clone)]
pub struct SinglePointResponse {
    pub timestamp: i64,
    pub stable: i64,
    pub lazer: i64,
    pub sum: i64,
    pub ratio: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct PointLineResponse {
    pub timestamp: Vec<i64>,
    pub stable: Vec<i64>,
    pub lazer: Vec<i64>,
    pub sum: Vec<i64>,
    pub ratio: Vec<f64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct RatioRegressionResponse {
    pub target_ratio: f64,
    pub was_reached: bool,
    pub estimated_timestamp: i64,
}

pub fn to_response(value: MeasurementEntry) -> axum::Json<SinglePointResponse> {
    axum::Json(value.into())
}

pub struct ApplicationCache {
    pub(crate) last_update: chrono::DateTime<Utc>,
    pub(crate) changelog_entries: BarEntries,
    pub(crate) graph_entries: GraphEntries,
}

impl ApplicationCache {
    pub fn is_stale(&self) -> bool {
        self.last_update.signed_duration_since(Utc::now()) > TimeDelta::minutes(5)
    }

    pub fn latest(&self) -> SinglePointResponse {
        self.changelog_entries.latest.clone()
    }
    pub fn peak_user_count(&self) -> SinglePointResponse {
        self.changelog_entries.peak_user_count.clone()
    }
    pub fn peak_user_percentage(&self) -> SinglePointResponse {
        self.changelog_entries.peak_user_percentage.clone()
    }
    pub fn peak_percentile_percentage(&self) -> SinglePointResponse {
        self.changelog_entries.peak_percentile_percentage.clone()
    }

    pub fn daily_user_graph(&self) -> PointLineResponse {
        self.graph_entries.day_users.clone()
    }
    pub fn historical_user_graph(&self) -> PointLineResponse {
        self.graph_entries.history_users.clone()
    }
}

pub struct BarEntries {
    /// Latest fetched changelog entry
    pub latest: SinglePointResponse,
    /// Peak lazer user count
    pub peak_user_count: SinglePointResponse,
    /// Peak lazer:stable user ratio
    pub peak_user_percentage: SinglePointResponse,
    /// Peak lazer:stable ratio withing 15% of highest user count
    pub peak_percentile_percentage: SinglePointResponse,
}

pub struct GraphEntries {
    /// Condensed collected data
    // pub history_users: Vec<ChangelogEntry>,
    /// 24h users
    pub day_users: PointLineResponse,
    /// Overall daily average history
    pub history_users: PointLineResponse,
}
