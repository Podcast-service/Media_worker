use utoipa::OpenApi;

use crate::kafka::MediaWorkerEvent;
use crate::progress::{WorkerProgress, WorkerStage};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::progress::progress_sse,
    ),
    components(
        schemas(
            WorkerProgress,
            WorkerStage,
            MediaWorkerEvent,
        )
    ),
    tags(
        (name = "worker", description = "Media Worker — обработка аудио, прогресс")
    )
)]
pub struct ApiDoc;
