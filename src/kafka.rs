use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

/// Входящие события из media_api.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaEvent {
    Uploaded {
        file_id: String,
        author_id: String,
        size_bytes: usize,
        original_format: String,
        temp_path: String,
        uploaded_at: DateTime<Utc>,
    },
    Deleted {
        file_id: String,
        deleted_at: DateTime<Utc>,
    },
}

/// Исходящие события media.worker.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaWorkerEvent {
    Converted {
        file_id: String,
        path: String,
        duration: f64,
        bitrates: Vec<u32>,
        converted_at: DateTime<Utc>,
    },
    Error {
        file_id: String,
        stage: String,
        error_message: String,
        timestamp: DateTime<Utc>,
    },
    Deleted {
        file_id: String,
        deleted_objects: u32,
        deleted_at: DateTime<Utc>,
    },
}

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
        let file_id_key = file_id.to_string();
        let event = MediaWorkerEvent::Converted {
            file_id: file_id_key.clone(),
            path: hls_path.to_string(),
            duration,
            bitrates,
            converted_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER)
            .key(&file_id_key)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| {
                anyhow::anyhow!("Failed to send media.worker.converted: {}", err)
            })?;

        info!(
            "Published media.worker.converted (file_id={}, path={})",
            file_id, hls_path,
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
        let file_id_key = file_id.to_string();
        let event = MediaWorkerEvent::Error {
            file_id: file_id_key.clone(),
            stage: stage.to_string(),
            error_message: error_message.to_string(),
            timestamp: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER)
            .key(&file_id_key)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send media.worker.error: {}", err))?;

        info!(
            "Published media.worker.error (file_id={}, stage={})",
            file_id, stage,
        );

        Ok(())
    }

    /// Публикует media.worker.deleted
    pub async fn send_deleted(&self, file_id: Uuid, deleted_objects: u32) -> Result<()> {
        let file_id_key = file_id.to_string();
        let event = MediaWorkerEvent::Deleted {
            file_id: file_id_key.clone(),
            deleted_objects,
            deleted_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER)
            .key(&file_id_key)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| {
                anyhow::anyhow!("Failed to send media.worker.deleted: {}", err)
            })?;

        info!(
            "Published media.worker.deleted (file_id={}, objects={})",
            file_id, deleted_objects,
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
