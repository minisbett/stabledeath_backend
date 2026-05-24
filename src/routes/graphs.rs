use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use chrono::DateTime;
use serde::Deserialize;

use crate::{
    server::ServerState,
    types::{BucketSize, PointLineResponse, RatioRegressionResponse},
};

pub async fn user_count_graph(State(state): State<ServerState>) -> Json<PointLineResponse> {
    let response: Json<PointLineResponse> = state.lock().await.cache().daily_user_graph().into();
    tracing::info!(
        points = response.0.timestamp.len(),
        "Served daily graph data"
    );

    response
}

#[derive(Deserialize, Default)]
pub struct HistoryQuery {
    from: Option<i64>,
    to: Option<i64>,
    #[serde(default)]
    bucket_size: BucketSize,
}

pub async fn history_user_graph(
    State(state): State<ServerState>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<PointLineResponse>, StatusCode> {
    let response: PointLineResponse;
    match (query.from, query.to) {
        (None, None) => {
            if let BucketSize::Day = query.bucket_size {
                response = state.lock().await.cache().historical_user_graph().into();
            } else {
                response = state
                    .lock()
                    .await
                    .database()
                    .get_history(query.bucket_size)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    .into();
            }
        }
        (None, Some(_)) | (Some(_), None) => return Err(StatusCode::BAD_REQUEST),
        (Some(from), Some(to)) => {
            response = state
                .lock()
                .await
                .database()
                .get_history_range(
                    DateTime::from_timestamp(from, 0).ok_or(StatusCode::BAD_REQUEST)?,
                    DateTime::from_timestamp(to, 0).ok_or(StatusCode::BAD_REQUEST)?,
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .into()
        }
    }
    tracing::info!(
        points = response.timestamp.len(),
        "Served history graph data"
    );

    Ok(Json(response))
}

pub async fn ratio_estimate(
    State(state): State<ServerState>,
    Path(percentage): Path<f64>,
) -> Result<Json<RatioRegressionResponse>, StatusCode> {
    if !percentage.is_finite() || !(0.0..=100.0).contains(&percentage) {
        tracing::warn!(percentage, "Rejected invalid ratio estimate target");
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut state = state.lock().await;
    let estimate = state
        .database()
        .estimate_ratio_percentage(percentage)
        .await
        .inspect_err(|error| tracing::warn!(%error, percentage, "Failed to estimate ratio target"))
        .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

    Ok(Json(estimate.into()))
}

pub fn router() -> Router<ServerState> {
    tracing::debug!("Building graphs router");
    Router::new()
        .route("/day", get(user_count_graph))
        .route("/history", get(history_user_graph))
        .route("/ratio_estimate/{percentage}", get(ratio_estimate))
}
