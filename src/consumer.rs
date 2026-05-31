use std::sync::Arc;

use anyhow::{Context, Result};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    kafka::{MediaEvent, MediaObjectType, SharedKafkaProducer},
    pipeline,
    progress::ProgressMap,
    storage::StorageBackend,
};

const TOPIC: &str = "media";
const GROUP_ID: &str = "media-worker-service";
const DEFAULT_HLS_BUCKET: &str = "4c5face5-544c-4bc2-a2e0-57a24d243af3";

pub async fn run_media_consumer(
    brokers: &str,
    storage: Arc<dyn StorageBackend>,
    kafka: SharedKafkaProducer,
    progress: ProgressMap,
) -> Result<()> {
    let consumer = create_consumer(brokers)?;
    info!(
        "Kafka consumer started: listening on '{}' (group={})",
        TOPIC, GROUP_ID
    );

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                handle_kafka_message(msg, &storage, &kafka, &progress).await;
            }
            Err(e) => {
                error!("Kafka consumer error: {}", e);
            }
        }
    }

    warn!("Kafka consumer stream ended unexpectedly");
    Ok(())
}

fn create_consumer(brokers: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", GROUP_ID)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .set("reconnect.backoff.ms", "500")
        .set("reconnect.backoff.max.ms", "10000")
        .set("retry.backoff.ms", "500")
        .set("socket.keepalive.enable", "true")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[TOPIC])
        .context("Failed to subscribe to media topic")?;

    Ok(consumer)
}

async fn handle_kafka_message(
    msg: rdkafka::message::BorrowedMessage<'_>,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
) {
    let Some(payload) = decode_payload(&msg) else {
        return;
    };

    match parse_media_event(payload) {
        Ok(event) => dispatch_media_event(event, storage, kafka, progress).await,
        Err(e) => warn!("Failed to parse media event payload: {}", e),
    }
}

fn decode_payload<'a>(msg: &'a rdkafka::message::BorrowedMessage<'a>) -> Option<&'a str> {
    match msg.payload_view::<str>() {
        Some(Ok(text)) => Some(text),
        Some(Err(e)) => {
            warn!("Error decoding Kafka message payload: {}", e);
            None
        }
        None => {
            warn!("Empty Kafka message on {}", TOPIC);
            None
        }
    }
}

fn parse_media_event(payload: &str) -> Result<MediaEvent> {
    serde_json::from_str::<MediaEvent>(payload).context("Failed to deserialize media event")
}

async fn dispatch_media_event(
    event: MediaEvent,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
) {
    match event {
        MediaEvent::StartUpload {
            media_type,
            object_id,
            started_at: _,
        } => {
            info!(
                "Received media.start_upload: type={}, object_id={}",
                media_type.as_str(),
                object_id
            );
        }
        MediaEvent::Uploaded {
            media_type,
            object_id,
            url,
            size,
            content_type,
            uploaded_at: _,
        } => {
            if media_type != MediaObjectType::PodcastFile {
                info!(
                    "Ignoring media.uploaded for type={} object_id={}",
                    media_type.as_str(),
                    object_id
                );
                return;
            }

            if !validate_uploaded_url(&object_id, &url) {
                return;
            }

            handle_uploaded(object_id, size, content_type, url, storage, kafka, progress).await;
        }
        MediaEvent::Error {
            media_type,
            object_id,
            error_message,
            timestamp: _,
        } => {
            warn!(
                "Received media.error from upstream: type={}, object_id={}, error={}",
                media_type.as_str(),
                object_id,
                error_message
            );
        }
        MediaEvent::Deleted {
            object_id,
            deleted_at: _,
        } => {
            handle_deleted(object_id, storage, kafka).await;
        }
    }
}

fn validate_uploaded_url(object_id: &str, url: &str) -> bool {
    if url.is_empty() {
        warn!(
            "Received media.uploaded with empty url for object_id={}",
            object_id
        );
        return false;
    }

    true
}

async fn handle_uploaded(
    object_id: String,
    size: usize,
    content_type: String,
    url: String,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
) {
    let file_id = match Uuid::parse_str(&object_id) {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid object_id in media.uploaded event: {}", e);
            return;
        }
    };
    let podcast_id = object_id.clone();
    let need_subtitle = true;

    info!(
        "Received media.uploaded podcast_file: object_id={}, need_subtitle={}, size={}, content_type={}, url={}",
        object_id, need_subtitle, size, content_type, url
    );

    let storage = storage.clone();
    let kafka = kafka.clone();
    let progress = progress.clone();

    tokio::spawn(async move {
        if let Err(e) = kafka.send_processing_started(file_id).await {
            warn!(
                "Failed to publish backend media.worker.start_processing: {}",
                e
            );
        }

        if let Err(e) = pipeline::run_pipeline(
            file_id,
            podcast_id,
            need_subtitle,
            url,
            content_type,
            size,
            storage,
            kafka.clone(),
            progress,
        )
        .await
        {
            error!("Pipeline task failed for file_id={}: {}", file_id, e);
            if let Err(publish_error) = kafka.send_processing_failed(file_id, &e.to_string()).await
            {
                warn!(
                    "Failed to publish backend media.worker.processing_failed: {}",
                    publish_error
                );
            }
        }
    });
}

async fn handle_deleted(
    file_id_raw: String,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
) {
    let file_id = match Uuid::parse_str(&file_id_raw) {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid file_id in media.deleted event: {}", e);
            return;
        }
    };

    info!("Received media.deleted: file_id={}", file_id);

    let prefix = format!("media/{}/", file_id);

    let hls_bucket = hls_bucket();

    match storage.delete_by_prefix(&hls_bucket, &prefix).await {
        Ok(count) => {
            info!(
                "Deleted {} objects for file_id={} from {}",
                count, file_id, hls_bucket
            );
            if let Err(e) = kafka.send_deleted(file_id, count).await {
                warn!("Failed to publish media.worker.deleted: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to delete objects for file_id={}: {}", file_id, e);
            if let Err(e) = kafka
                .send_worker_error(file_id, "deletion", &e.to_string())
                .await
            {
                warn!("Failed to publish media.worker.error: {}", e);
            }
        }
    }
}

fn hls_bucket() -> String {
    std::env::var("HLS_BUCKET")
        .ok()
        .or_else(|| std::env::var("S3_BUCKET").ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_HLS_BUCKET.to_string())
}
