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

fn default_need_subtitle() -> bool {
    true
}

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
        #[serde(default = "default_need_subtitle")]
        need_subtitle: bool,
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

#[derive(Debug, Clone, Serialize)]
struct BackendMediaWorkerEvent {
    object_type: &'static str,
    object_id: String,
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    audio_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audio_file_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    timestamp: DateTime<Utc>,
}

impl BackendMediaWorkerEvent {
    fn start_processing(file_id: Uuid) -> Self {
        Self {
            object_type: "podcast_file_url",
            object_id: file_id.to_string(),
            event: "start_processing",
            audio_url: None,
            duration_seconds: None,
            audio_file_size: None,
            error: None,
            timestamp: Utc::now(),
        }
    }

    fn processed(
        podcast_id: &str,
        audio_url: &str,
        duration_seconds: f64,
        audio_file_size: usize,
    ) -> Self {
        Self {
            object_type: "podcast_file_url",
            object_id: podcast_id.to_string(),
            event: "processed",
            audio_url: Some(audio_url.to_string()),
            // backend ожидает целое число секунд (Long), поэтому округляем f64.
            duration_seconds: Some((duration_seconds.round() as i64).to_string()),
            audio_file_size: Some(audio_file_size.to_string()),
            error: None,
            timestamp: Utc::now(),
        }
    }

    fn processing_failed(file_id: Uuid, error: &str) -> Self {
        Self {
            object_type: "podcast_file_url",
            object_id: file_id.to_string(),
            event: "processing_failed",
            audio_url: None,
            duration_seconds: None,
            audio_file_size: None,
            error: Some(error.to_string()),
            timestamp: Utc::now(),
        }
    }
}

/// Backend-события для podcast_core (start_processing/processed/processing_failed).
const TOPIC_MEDIA_WORKER: &str = "media.worker";
/// Публичный поток worker'а (converted/error/deleted) для внешних потребителей.
const TOPIC_MEDIA_WORKER_EVENTS: &str = "media.worker.events";
/// Запросы на генерацию субтитров (worker -> speech_service).
const TOPIC_SUBTITLE_REQUEST: &str = "media.subtitle.request";

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

    /// Публикует backend-событие начала обработки подкаста.
    pub async fn send_processing_started(&self, file_id: Uuid) -> Result<()> {
        let event = BackendMediaWorkerEvent::start_processing(file_id);

        self.send_json_to_topic(
            TOPIC_MEDIA_WORKER,
            &file_id.to_string(),
            &event,
            "backend media.worker.start_processing",
        )
        .await?;

        info!(
            "Published backend media.worker.start_processing (file_id={})",
            file_id,
        );

        Ok(())
    }

    /// Публикует media.worker.converted
    pub async fn send_converted(
        &self,
        file_id: Uuid,
        podcast_id: &str,
        hls_path: &str,
        duration: f64,
        bitrates: Vec<u32>,
        original_duration_seconds: f64,
        original_audio_file_size: usize,
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
        let backend_event = BackendMediaWorkerEvent::processed(
            podcast_id,
            hls_path,
            original_duration_seconds,
            original_audio_file_size,
        );

        let converted_result = self
            .send_json_to_topic(
                TOPIC_MEDIA_WORKER_EVENTS,
                &file_id_key,
                &event,
                "media.worker.converted",
            )
            .await;
        let backend_result = self
            .send_json_to_topic(
                TOPIC_MEDIA_WORKER,
                &file_id_key,
                &backend_event,
                "backend media.worker.processed",
            )
            .await;
        converted_result?;
        backend_result?;

        info!(
            "Published media.worker.converted and backend media.worker.processed (file_id={}, podcast_id={}, path={})",
            file_id, podcast_id, hls_path,
        );

        Ok(())
    }

    /// Публикует backend-событие неуспешной обработки подкаста.
    pub async fn send_processing_failed(&self, file_id: Uuid, error: &str) -> Result<()> {
        let event = BackendMediaWorkerEvent::processing_failed(file_id, error);

        self.send_json_to_topic(
            TOPIC_MEDIA_WORKER,
            &file_id.to_string(),
            &event,
            "backend media.worker.processing_failed",
        )
        .await?;

        info!(
            "Published backend media.worker.processing_failed (file_id={})",
            file_id,
        );

        Ok(())
    }

    /// Публикует запрос на генерацию субтитров в media.subtitle.request
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
        let record = FutureRecord::to(TOPIC_SUBTITLE_REQUEST)
            .key(&file_id_key)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| {
                anyhow::anyhow!("Failed to send media.subtitle.request: {}", err)
            })?;

        info!(
            "Published media.subtitle.request (file_id={}, source={}/{}, language={}, num_speakers={:?})",
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
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER_EVENTS)
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
        let record = FutureRecord::to(TOPIC_MEDIA_WORKER_EVENTS)
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

    async fn send_json_to_topic<T: Serialize>(
        &self,
        topic: &str,
        key: &str,
        value: &T,
        label: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(value)?;
        let record = FutureRecord::to(topic).key(key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send {label}: {err}"))?;

        Ok(())
    }
}

pub type SharedKafkaProducer = Arc<KafkaProducer>;

pub fn new_producer(brokers: &str) -> Result<SharedKafkaProducer> {
    let producer = KafkaProducer::new(brokers)?;
    Ok(Arc::new(producer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uploaded_event_deserializes_need_subtitle() {
        let event: MediaEvent = serde_json::from_value(serde_json::json!({
            "event": "uploaded",
            "type": "podcast_file",
            "object_id": "11111111-1111-4111-8111-111111111111",
            "url": "s3://bucket/source.mp3",
            "size": 123,
            "content_type": "audio/mpeg",
            "need_subtitle": false,
            "uploaded_at": "2026-04-07T12:00:00Z"
        }))
        .unwrap();

        assert!(matches!(
            event,
            MediaEvent::Uploaded {
                need_subtitle: false,
                ..
            }
        ));
    }

    #[test]
    fn uploaded_event_defaults_need_subtitle_to_true() {
        let event: MediaEvent = serde_json::from_value(serde_json::json!({
            "event": "uploaded",
            "type": "podcast_file",
            "object_id": "11111111-1111-4111-8111-111111111111",
            "url": "s3://bucket/source.mp3",
            "size": 123,
            "content_type": "audio/mpeg",
            "uploaded_at": "2026-04-07T12:00:00Z"
        }))
        .unwrap();

        assert!(matches!(
            event,
            MediaEvent::Uploaded {
                need_subtitle: true,
                ..
            }
        ));
    }

    #[test]
    fn backend_processed_event_uses_backend_contract() {
        let event = BackendMediaWorkerEvent::processed(
            "11111111-1111-4111-8111-111111111111",
            "/media/11111111-1111-4111-8111-111111111111/master.m3u8",
            2580.0,
            11232332,
        );
        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["object_type"], "podcast_file_url");
        assert_eq!(value["event"], "processed");
        assert_eq!(
            value["audio_url"],
            "/media/11111111-1111-4111-8111-111111111111/master.m3u8"
        );
        assert_eq!(value["duration_seconds"], "2580");
        assert_eq!(value["audio_file_size"], "11232332");
    }

    #[test]
    fn backend_processed_event_rounds_fractional_duration_to_long() {
        // duration из ffprobe приходит как f64 (например 30.040816); backend ждёт Long,
        // поэтому значение должно округляться до целого числа секунд.
        let event = BackendMediaWorkerEvent::processed(
            "11111111-1111-4111-8111-111111111111",
            "/media/11111111-1111-4111-8111-111111111111/master.m3u8",
            30.040816,
            512,
        );
        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["duration_seconds"], "30");
    }
}
