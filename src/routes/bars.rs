//! Data for bars of stable vs lazer info

use apply::Apply;
use axum::{Json, Router, extract::State, http::StatusCode, routing::get};

use crate::{
    database::models::MeasurementEntry,
    server::ServerState,
    types::{SinglePointResponse, to_response},
};

async fn get_current(
    State(state): State<ServerState>,
) -> Result<Json<SinglePointResponse>, StatusCode> {
    let mut state = state.lock().await;
    let changelog = state
        .get_latest_changelog()
        .await
        .inspect_err(|e| tracing::warn!("Error on current data: {e}"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(axum::Json(changelog))
}

async fn get_highest_user_count(State(state): State<ServerState>) -> Json<SinglePointResponse> {
    state
        .lock()
        .await
        .cache()
        .peak_user_count()
        .apply(MeasurementEntry::from)
        .apply(to_response)
}

async fn get_highest_user_percentage(
    State(state): State<ServerState>,
) -> Json<SinglePointResponse> {
    state
        .lock()
        .await
        .cache()
        .peak_user_percentage()
        .apply(MeasurementEntry::from)
        .apply(to_response)
}

async fn get_highest_user_count_within_85th_percentile(
    State(state): State<ServerState>,
) -> Json<SinglePointResponse> {
    state
        .lock()
        .await
        .cache()
        .peak_percentile_percentage()
        .apply(MeasurementEntry::from)
        .apply(to_response)
}

pub fn router() -> Router<ServerState> {
    Router::new()
        .route("/current", get(get_current))
        .route("/peak_users", get(get_highest_user_count))
        .route("/peak_ratio", get(get_highest_user_percentage))
        .route(
            "/peak_percentile",
            get(get_highest_user_count_within_85th_percentile),
        )
}
