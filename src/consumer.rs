use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::kafka::{MediaDeletedEvent, MediaUploadedEvent, SharedKafkaProducer};
use crate::pipeline;
use crate::progress::ProgressMap;
use crate::storage::StorageBackend;

const TOPIC: &str = "media";
const GROUP_ID: &str = "media-worker-service";
const HLS_BUCKET: &str = "audio-hls";

pub async fn run_media_consumer(
    brokers: &str,
    storage: Arc<dyn StorageBackend>,
    kafka: SharedKafkaProducer,
    progress: ProgressMap,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", GROUP_ID)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[TOPIC])
        .context("Failed to subscribe to media topic")?;

    info!(
        "Kafka consumer started: listening on '{}' (group={})",
        TOPIC, GROUP_ID
    );

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                let payload = match msg.payload_view::<str>() {
                    Some(Ok(text)) => text,
                    Some(Err(e)) => {
                        warn!("Error decoding Kafka message payload: {}", e);
                        continue;
                    }
                    None => {
                        warn!("Empty Kafka message on {}", TOPIC);
                        continue;
                    }
                };

                if let Ok(event) = serde_json::from_str::<MediaUploadedEvent>(payload) {
                    if !event.temp_path.is_empty() {
                        handle_uploaded(event, &storage, &kafka, &progress).await;
                        continue;
                    }
                }

                if let Ok(event) = serde_json::from_str::<MediaDeletedEvent>(payload) {
                    handle_deleted(event, &storage, &kafka).await;
                    continue;
                }
            }
            Err(e) => {
                error!("Kafka consumer error: {}", e);
            }
        }
    }

    warn!("Kafka consumer stream ended unexpectedly");
    Ok(())
}

async fn handle_uploaded(
    event: MediaUploadedEvent,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
) {
    let file_id = match Uuid::parse_str(&event.file_id) {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid file_id in media.uploaded event: {}", e);
            return;
        }
    };

    info!(
        "Received media.uploaded: file_id={}, size={}, format={}, path={}",
        event.file_id, event.size_bytes, event.original_format, event.temp_path
    );

    let storage = storage.clone();
    let kafka = kafka.clone();
    let progress = progress.clone();
    let temp_path = event.temp_path.clone();

    tokio::spawn(async move {
        pipeline::run_pipeline(file_id, &temp_path, storage, kafka, progress).await;
    });
}

async fn handle_deleted(
    event: MediaDeletedEvent,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
) {
    let file_id = match Uuid::parse_str(&event.file_id) {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid file_id in media.deleted event: {}", e);
            return;
        }
    };

    info!("Received media.deleted: file_id={}", file_id);

    let prefix = format!("media/{}/", file_id);

    match storage.delete_by_prefix(HLS_BUCKET, &prefix).await {
        Ok(count) => {
            info!(
                "Deleted {} objects for file_id={} from {}",
                count, file_id, HLS_BUCKET
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
