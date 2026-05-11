use crate::types::{PointLineResponse, SinglePointResponse, ratio};

#[derive(Debug, sqlx::FromRow)]
pub struct MeasurementEntry {
    pub timestamp: i64,
    pub stable: i64,
    pub lazer: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct DailyEntry {
    pub date: i64,
    pub stable: i64,
    pub lazer: i64,
}

impl From<MeasurementEntry> for SinglePointResponse {
    fn from(value: MeasurementEntry) -> Self {
        Self {
            timestamp: chrono::DateTime::from_timestamp_secs(value.timestamp)
                .unwrap()
                .into(),
            stable: value.stable,
            lazer: value.lazer,
            ratio: ratio(value.stable, value.lazer),
            sum: value.stable + value.lazer,
        }
    }
}

impl From<SinglePointResponse> for MeasurementEntry {
    fn from(value: SinglePointResponse) -> Self {
        Self {
            timestamp: value.timestamp.timestamp(),
            stable: value.stable,
            lazer: value.lazer,
        }
    }
}

impl From<Vec<MeasurementEntry>> for PointLineResponse {
    fn from(value: Vec<MeasurementEntry>) -> Self {
        let mut response = Self {
            timestamps: Vec::with_capacity(value.len()),
            stable: Vec::with_capacity(value.len()),
            lazer: Vec::with_capacity(value.len()),
            sum: Vec::with_capacity(value.len()),
            ratio: Vec::with_capacity(value.len()),
        };

        for entry in value {
            response.timestamps.push(
                chrono::DateTime::from_timestamp_secs(entry.timestamp)
                    .unwrap()
                    .into(),
            );
            response.stable.push(entry.stable);
            response.lazer.push(entry.lazer);
            response.sum.push(entry.stable + entry.lazer);
            response.ratio.push(ratio(entry.stable, entry.lazer));
        }

        response
    }
}

impl From<Vec<DailyEntry>> for PointLineResponse {
    fn from(value: Vec<DailyEntry>) -> Self {
        let mut response = Self {
            timestamps: Vec::with_capacity(value.len()),
            stable: Vec::with_capacity(value.len()),
            lazer: Vec::with_capacity(value.len()),
            sum: Vec::with_capacity(value.len()),
            ratio: Vec::with_capacity(value.len()),
        };
        for entry in value {
            response.timestamps.push(
                chrono::DateTime::from_timestamp_secs(entry.date)
                    .unwrap()
                    .into(),
            );
            response.stable.push(entry.stable);
            response.lazer.push(entry.lazer);
            response.sum.push(entry.stable + entry.lazer);
            response.ratio.push(ratio(entry.stable, entry.lazer));
        }
        response
    }
}
