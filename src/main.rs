mod api_doc;
mod consumer;
mod hls;
mod kafka;
mod loader_rustfs;
mod pipeline;
mod progress;
mod storage;

use std::sync::Arc;

use api_doc::ApiDoc;
use axum::{routing::get, Router};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cfg = loader_rustfs::Config::from_env().expect("RustFS config: set RUSTFS_* env variables");
    let client = loader_rustfs::create_client(&cfg)
        .await
        .expect("Failed to create RustFS client");
    let storage: Arc<dyn storage::StorageBackend> = Arc::new(client);

    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let kafka_producer =
        kafka::new_producer(&kafka_brokers).expect("Failed to create Kafka producer");

    let progress_map = progress::new_progress_map();
    let progress_config = progress::ProgressConfig::from_env();

    let state = progress::WorkerState {
        progress: progress_map.clone(),
        config: progress_config,
    };

    let consumer_storage = storage.clone();
    let consumer_brokers = kafka_brokers.clone();
    let consumer_kafka = kafka_producer.clone();
    let consumer_progress = progress_map.clone();
    tokio::spawn(async move {
        if let Err(e) = consumer::run_media_consumer(
            &consumer_brokers,
            consumer_storage,
            consumer_kafka,
            consumer_progress,
        )
        .await
        {
            error!("Kafka consumer crashed: {}", e);
        }
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route(
            "/api/media/worker/progress/:file_id",
            get(progress::progress_sse),
        )
        .layer(cors)
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8082".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    info!("media_worker listening on http://{}", addr);
    info!("Swagger UI: http://localhost:{}/swagger-ui/", port);

    axum::serve(listener, app).await.expect("Server error");
}
