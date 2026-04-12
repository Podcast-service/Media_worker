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
    kafka::{MediaEvent, SharedKafkaProducer},
    pipeline,
    progress::ProgressMap,
    storage::StorageBackend,
};

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

                match serde_json::from_str::<MediaEvent>(payload) {
                    Ok(MediaEvent::Uploaded {
                        file_id,
                        author_id,
                        size_bytes,
                        original_format,
                        temp_path,
                        uploaded_at: _,
                    }) => {
                        if temp_path.is_empty() {
                            warn!(
                                "Received media.uploaded with empty temp_path for file_id={}",
                                file_id
                            );
                            continue;
                        }

                        handle_uploaded(
                            file_id,
                            author_id,
                            size_bytes,
                            original_format,
                            temp_path,
                            &storage,
                            &kafka,
                            &progress,
                        )
                        .await;
                    }
                    Ok(MediaEvent::Deleted {
                        file_id,
                        deleted_at: _,
                    }) => {
                        handle_deleted(file_id, &storage, &kafka).await;
                    }
                    Err(e) => {
                        warn!("Failed to parse media event payload: {}", e);
                    }
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
    file_id_raw: String,
    author_id: String,
    size_bytes: usize,
    original_format: String,
    temp_path: String,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
) {
    let file_id = match Uuid::parse_str(&file_id_raw) {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid file_id in media.uploaded event: {}", e);
            return;
        }
    };

    info!(
        "Received media.uploaded: file_id={}, author_id={}, size={}, format={}, path={}",
        file_id_raw, author_id, size_bytes, original_format, temp_path
    );

    let storage = storage.clone();
    let kafka = kafka.clone();
    let progress = progress.clone();

    tokio::spawn(async move {
        if let Err(e) = pipeline::run_pipeline(file_id, &temp_path, storage, kafka, progress).await
        {
            error!("Pipeline task failed for file_id={}: {}", file_id, e);
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
