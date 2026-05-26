use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use serde::{Deserialize, Serialize};
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

/// Входящие события из media_api.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MediaObjectType {
    PodcastFile,
    Avatar,
    PodcastCover,
    Playlists,
}

impl MediaObjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PodcastFile => "podcast_file",
            Self::Avatar => "avatar",
            Self::PodcastCover => "podcast_cover",
            Self::Playlists => "playlists",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaEvent {
    StartUpload {
        #[serde(rename = "type")]
        media_type: MediaObjectType,
        object_id: String,
        started_at: DateTime<Utc>,
    },
    Uploaded {
        #[serde(rename = "type")]
        media_type: MediaObjectType,
        object_id: String,
        url: String,
        size: usize,
        content_type: String,
        uploaded_at: DateTime<Utc>,
    },
    Error {
        #[serde(rename = "type")]
        media_type: MediaObjectType,
        object_id: String,
        error_message: String,
        timestamp: DateTime<Utc>,
    },
    Deleted {
        object_id: String,
        deleted_at: DateTime<Utc>,
    },
}

/// Исходящие события media.worker.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaWorkerEvent {
    Converted {
        file_id: String,
        podcast_id: String,
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
const TOPIC_SUBTITLE: &str = "media.subtitle";

#[derive(Debug, Clone, Serialize)]
pub struct SubtitleRequestedEvent {
    pub file_id: String,
    pub source_bucket: String,
    pub source_object_key: String,
    pub language: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_speakers: Option<u32>,
    pub requested_at: DateTime<Utc>,
}

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("message.send.max.retries", "10")
            .set("retry.backoff.ms", "500")
            .set("reconnect.backoff.ms", "500")
            .set("reconnect.backoff.max.ms", "10000")
            .set("socket.keepalive.enable", "true")
            .create::<FutureProducer>()
            .context("Failed to create Kafka producer")?;

        Ok(Self { producer })
    }

    /// Публикует media.worker.converted
    pub async fn send_converted(
        &self,
        file_id: Uuid,
        podcast_id: &str,
        hls_path: &str,
        duration: f64,
        bitrates: Vec<u32>,
    ) -> Result<()> {
        let file_id_key = file_id.to_string();
        let event = MediaWorkerEvent::Converted {
            file_id: file_id_key.clone(),
            podcast_id: podcast_id.to_string(),
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
            "Published media.worker.converted (file_id={}, podcast_id={}, path={})",
            file_id, podcast_id, hls_path,
        );

        Ok(())
    }

    /// Публикует запрос на генерацию субтитров в media.subtitle
    pub async fn send_subtitle_requested(
        &self,
        file_id: Uuid,
        source_bucket: &str,
        source_object_key: &str,
        language: &str,
        num_speakers: Option<u32>,
    ) -> Result<()> {
        let file_id_key = file_id.to_string();
        let event = SubtitleRequestedEvent {
            file_id: file_id_key.clone(),
            source_bucket: source_bucket.to_string(),
            source_object_key: source_object_key.to_string(),
            language: language.to_string(),
            num_speakers,
            requested_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC_SUBTITLE)
            .key(&file_id_key)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| {
                anyhow::anyhow!("Failed to send media.subtitle.requested: {}", err)
            })?;

        info!(
            "Published media.subtitle.requested (file_id={}, source={}/{}, language={}, num_speakers={:?})",
            file_id, source_bucket, source_object_key, language, num_speakers,
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
