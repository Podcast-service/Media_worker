use utoipa::OpenApi;

use crate::kafka::{MediaWorkerConvertedEvent, MediaWorkerDeletedEvent, MediaWorkerErrorEvent};
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
            MediaWorkerConvertedEvent,
            MediaWorkerErrorEvent,
            MediaWorkerDeletedEvent,
        )
    ),
    tags(
        (name = "worker", description = "Media Worker — обработка аудио, прогресс")
    )
)]
pub struct ApiDoc;
