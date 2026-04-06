use anyhow::{Context, Result};
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

/// Входящее событие из media_api — файл загружен
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaUploadedEvent {
    pub file_id: String,
    pub author_id: String,
    pub size_bytes: usize,
    pub original_format: String,
    pub temp_path: String,
    pub uploaded_at: String,
}

/// Входящее событие — запрос на удаление медиа
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaDeletedEvent {
    pub file_id: String,
    pub deleted_at: String,
}

/// media.worker.converted — файл успешно сконвертирован и загружен в хранилище
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MediaWorkerConvertedEvent {
    pub file_id: String,
    pub path: String,
    pub duration: f64,
    pub bitrates: Vec<u32>,
    pub converted_at: String,
}

/// media.worker.error — ошибка на стадии обработки
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MediaWorkerErrorEvent {
    pub file_id: String,
    pub stage: String,
    pub error_message: String,
    pub timestamp: String,
}

/// media.worker.deleted — файлы успешно удалены из хранилища
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MediaWorkerDeletedEvent {
    pub file_id: String,
    pub deleted_objects: u32,
    pub deleted_at: String,
}


const TOPIC_MEDIA: &str = "media";
const TOPIC_MEDIA_WORKER: &str = "media.worker";


pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>()
            .context("Failed to create Kafka producer")?;

        Ok(Self { producer })
    }

    /// Публикует media.worker.converted
    pub async fn send_converted(
        &self,
        file_id: Uuid,
        hls_path: &str,
        duration: f64,
        bitrates: Vec<u32>,
    ) -> Result<()> {
        let event = MediaWorkerConvertedEvent {
            file_id: file_id.to_string(),
            path: hls_path.to_string(),
            duration,
            bitrates,
            converted_at: Utc::now().to_rfc3339(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER)
            .key(&event.file_id)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| {
                anyhow::anyhow!("Failed to send media.worker.converted: {}", err)
            })?;

        info!(
            "Published media.worker.converted (file_id={}, path={})",
            file_id,
            hls_path,
        );

        Ok(())
    }

    /// Публикует media.worker.error
    pub async fn send_worker_error(
        &self,
        file_id: Uuid,
        stage: &str,
        error_message: &str,
    ) -> Result<()> {
        let event = MediaWorkerErrorEvent {
            file_id: file_id.to_string(),
            stage: stage.to_string(),
            error_message: error_message.to_string(),
            timestamp: Utc::now().to_rfc3339(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER)
            .key(&event.file_id)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send media.worker.error: {}", err))?;

        info!(
            "Published media.worker.error (file_id={}, stage={})",
            file_id,
            stage,
        );

        Ok(())
    }

    /// Публикует media.worker.deleted
    pub async fn send_deleted(&self, file_id: Uuid, deleted_objects: u32) -> Result<()> {
        let event = MediaWorkerDeletedEvent {
            file_id: file_id.to_string(),
            deleted_objects,
            deleted_at: Utc::now().to_rfc3339(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER)
            .key(&event.file_id)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| {
                anyhow::anyhow!("Failed to send media.worker.deleted: {}", err)
            })?;

        info!(
            "Published media.worker.deleted (file_id={}, objects={})",
            file_id,
            deleted_objects,
        );

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(10))
            .context("Failed to flush Kafka producer")?;
        Ok(())
    }
}

pub type SharedKafkaProducer = Arc<KafkaProducer>;

pub fn new_producer(brokers: &str) -> Result<SharedKafkaProducer> {
    let producer = KafkaProducer::new(brokers)?;
    Ok(Arc::new(producer))
}
