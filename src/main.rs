use std::{sync::Arc, time::Duration};

use axum::{Router, routing::get};
use axum_prometheus::PrometheusMetricLayer;
use rosu_v2::Osu;
use rustls::crypto::{CryptoProvider, ring};
use tokio::time::{Instant, Interval};

mod database;
mod routes;
mod server;
mod types;

#[tokio::main]
async fn main() {
    CryptoProvider::install_default(ring::default_provider()).unwrap();
    tracing_subscriber::fmt().init();
    let server = server::Server::init().await.unwrap();

    let (layer, metric_handler) = PrometheusMetricLayer::pair();

    let server_state = Arc::new(tokio::sync::Mutex::new(server));

    let cloned_state = Arc::clone(&server_state);
    tokio::task::spawn(async move {
        tracing::info!("Initializing cache update loop");
        let mut interval = tokio::time::interval_at(
            Instant::now() + Duration::from_mins(5),
            Duration::from_mins(5),
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            cloned_state.lock().await.update_cache().await.unwrap();
            tracing::info!("Updated cache");
        }
    });

    let app = Router::new()
        .nest("/api/bars", routes::bars::router())
        .nest("/api/graphs", routes::graphs::router())
        .route("/metrics", get(|| async move { metric_handler.render() }))
        .layer(layer)
        .with_state(server_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:6727").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}
