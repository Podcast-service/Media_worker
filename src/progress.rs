use axum::{
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
};
use dashmap::DashMap;
use serde::Serialize;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use utoipa::ToSchema;
use uuid::Uuid;

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
pub struct WorkerState {
    pub progress: ProgressMap,
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

    let stream: SseStream = Box::pin(async_stream::stream! {
        let mut interval = tokio::time::interval(Duration::from_millis(300));

        let mut waited = 0;
        while !progress_map.contains_key(&file_id) {
            if waited >= 100 {
                yield Ok(Event::default()
                    .event("error")
                    .data(r#"{"error":"file_id not found in worker"}"#));
                return;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            waited += 1;
        }

        loop {
            interval.tick().await;

            let entry = progress_map.get(&file_id);

            match entry {
                Some(p) => {
                    let progress = p.clone();
                    drop(p);

                    let json = serde_json::to_string(&progress).unwrap_or_default();
                    yield Ok(Event::default().data(json));

                    if progress.stage == WorkerStage::Done || progress.stage == WorkerStage::Error {
                        break;
                    }
                }
                None => {
                    break;
                }
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
