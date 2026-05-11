use axum::{Json, Router, extract::State, routing::get, serve::Serve};

use crate::{server::ServerState, types::PointLineResponse};

pub async fn user_count_graph(State(state): State<ServerState>) -> Json<PointLineResponse> {
    state.lock().await.cache().daily_user_graph().into()
}

pub async fn history_user_graph(State(state): State<ServerState>) -> Json<PointLineResponse> {
    state.lock().await.cache().historical_user_graph().into()
}

pub fn router() -> Router<ServerState> {
    Router::new()
        .route("/day", get(user_count_graph))
        .route("/history", get(history_user_graph))
}
