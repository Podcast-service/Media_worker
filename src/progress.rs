use std::{convert::Infallible, pin::Pin, sync::Arc, time::Duration};

use axum::{
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
};
use dashmap::DashMap;
use serde::Serialize;
use tracing::warn;
use utoipa::ToSchema;
use uuid::Uuid;

const PROGRESS_POLL_INTERVAL_MS_ENV: &str = "PROGRESS_POLL_INTERVAL_MS";
const PROGRESS_WAIT_TIMEOUT_MS_ENV: &str = "PROGRESS_WAIT_TIMEOUT_MS";
const DEFAULT_PROGRESS_POLL_INTERVAL_MS: u64 = 300;
const DEFAULT_PROGRESS_WAIT_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStage {
    Queued,
    Normalizing,
    Converting,
    Uploading,
    Done,
    Error,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct WorkerProgress {
    pub stage: WorkerStage,
    pub percent: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

pub type ProgressMap = Arc<DashMap<Uuid, WorkerProgress>>;

pub fn new_progress_map() -> ProgressMap {
    Arc::new(DashMap::new())
}

#[derive(Clone)]
pub struct ProgressConfig {
    pub poll_interval: Duration,
    pub wait_timeout: Duration,
}

impl ProgressConfig {
    pub fn from_env() -> Self {
        Self {
            poll_interval: load_duration_from_env(
                PROGRESS_POLL_INTERVAL_MS_ENV,
                DEFAULT_PROGRESS_POLL_INTERVAL_MS,
            ),
            wait_timeout: load_duration_from_env(
                PROGRESS_WAIT_TIMEOUT_MS_ENV,
                DEFAULT_PROGRESS_WAIT_TIMEOUT_MS,
            ),
        }
    }
}

#[derive(Clone)]
pub struct WorkerState {
    pub progress: ProgressMap,
    pub config: ProgressConfig,
}

type SseStream = Pin<Box<dyn futures_core::Stream<Item = Result<Event, Infallible>> + Send>>;

#[utoipa::path(
    get,
    path = "/api/media/worker/progress/{file_id}",
    params(
        ("file_id" = Uuid, Path, description = "File id from media.uploaded event")
    ),
    responses(
        (status = 200, description = "SSE progress stream", content_type = "text/event-stream", body = WorkerProgress)
    ),
    tag = "worker"
)]
pub async fn progress_sse(
    State(state): State<WorkerState>,
    Path(file_id): Path<Uuid>,
) -> Sse<SseStream> {
    let progress_map = state.progress.clone();
    let config = state.config.clone();

    let stream: SseStream = Box::pin(async_stream::stream! {
        let mut interval = tokio::time::interval(config.poll_interval);
        if !wait_for_progress_registration(&progress_map, file_id, &config).await {
            yield Ok(build_missing_file_event());
            return;
        }

        loop {
            interval.tick().await;
            let Some(progress) = load_progress_snapshot(&progress_map, file_id) else {
                break;
            };

            yield Ok(progress_to_sse_event(&progress));

            if is_terminal_stage(&progress.stage) {
                break;
            }
        }

        progress_map.remove(&file_id);
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn wait_for_progress_registration(
    progress_map: &ProgressMap,
    file_id: Uuid,
    config: &ProgressConfig,
) -> bool {
    let wait_deadline = tokio::time::Instant::now() + config.wait_timeout;

    while !progress_map.contains_key(&file_id) {
        if tokio::time::Instant::now() >= wait_deadline {
            return false;
        }

        tokio::time::sleep(config.poll_interval).await;
    }

    true
}

fn build_missing_file_event() -> Event {
    Event::default()
        .event("error")
        .data(r#"{"error":"file_id not found in worker"}"#)
}

fn load_progress_snapshot(progress_map: &ProgressMap, file_id: Uuid) -> Option<WorkerProgress> {
    let entry = progress_map.get(&file_id)?;
    let progress = entry.clone();
    drop(entry);
    Some(progress)
}

fn progress_to_sse_event(progress: &WorkerProgress) -> Event {
    let json = serde_json::to_string(progress).unwrap_or_default();
    Event::default().data(json)
}

fn is_terminal_stage(stage: &WorkerStage) -> bool {
    matches!(stage, WorkerStage::Done | WorkerStage::Error)
}

fn load_duration_from_env(env_name: &str, default_ms: u64) -> Duration {
    match std::env::var(env_name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(0) => {
                warn!(
                    "{} must be greater than 0, using default {}ms",
                    env_name, default_ms
                );
                Duration::from_millis(default_ms)
            }
            Ok(milliseconds) => Duration::from_millis(milliseconds),
            Err(e) => {
                warn!(
                    "Failed to parse {}='{}': {}, using default {}ms",
                    env_name, value, e, default_ms
                );
                Duration::from_millis(default_ms)
            }
        },
        Err(_) => Duration::from_millis(default_ms),
    }
}
