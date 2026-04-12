use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use tokio::{fs, process::Command};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    hls,
    kafka::SharedKafkaProducer,
    progress::{ProgressMap, WorkerProgress, WorkerStage},
    storage::StorageBackend,
};

const HLS_BUCKET: &str = "audio-hls";
const MAX_RETRIES_ENV: &str = "PIPELINE_MAX_RETRIES";
const DEFAULT_MAX_RETRIES: u32 = 3;
const LOUDNORM_TARGET_I: &str = "-16";
const LOUDNORM_TARGET_TP: &str = "-1.5";
const LOUDNORM_TARGET_LRA: &str = "11";
const FFMPEG_LOG_LEVEL: &str = "error";
const FFMPEG_HIDE_BANNER_FLAG: &str = "-hide_banner";
const FFMPEG_NO_STATS_FLAG: &str = "-nostats";
const FFMPEG_NULL_FORMAT: &str = "null";
const NORMALIZED_SAMPLE_RATE: &str = "48000";
const NORMALIZED_CHANNELS: &str = "2";
const OVERWRITE_OUTPUT_FLAG: &str = "-y";

pub struct PipelineResult {
    pub hls_path: String,
    pub duration: f64,
    pub bitrates: Vec<u32>,
}

pub async fn run_pipeline(
    file_id: Uuid,
    temp_path: &str,
    storage: Arc<dyn StorageBackend>,
    kafka: SharedKafkaProducer,
    progress: ProgressMap,
) -> Result<()> {
    let max_retries = load_max_retries();
    mark_pipeline_queued(file_id, &progress);

    match execute_pipeline_with_retries(file_id, temp_path, &storage, &progress, max_retries).await
    {
        Ok(result) => {
            finalize_successful_pipeline(file_id, &result, &kafka, &progress, temp_path).await;
            Ok(())
        }
        Err(err) => {
            finalize_failed_pipeline(file_id, &err, &kafka, &progress, temp_path).await;
            Err(err)
        }
    }
}

fn load_max_retries() -> u32 {
    match std::env::var(MAX_RETRIES_ENV) {
        Ok(value) => match value.parse::<u32>() {
            Ok(0) => {
                warn!(
                    "{} must be greater than 0, using default {}",
                    MAX_RETRIES_ENV, DEFAULT_MAX_RETRIES
                );
                DEFAULT_MAX_RETRIES
            }
            Ok(retries) => retries,
            Err(e) => {
                warn!(
                    "Failed to parse {}='{}': {}, using default {}",
                    MAX_RETRIES_ENV, value, e, DEFAULT_MAX_RETRIES
                );
                DEFAULT_MAX_RETRIES
            }
        },
        Err(_) => DEFAULT_MAX_RETRIES,
    }
}

/// Внутренняя реализация pipeline (один прогон)
async fn execute_pipeline(
    file_id: Uuid,
    temp_path: &str,
    storage: &Arc<dyn StorageBackend>,
    progress: &ProgressMap,
) -> Result<PipelineResult> {
    let input = ensure_source_file_exists(temp_path).await?;
    let normalized_path = run_normalization_stage(file_id, &input, progress).await?;
    let hls_output = match run_hls_conversion_stage(file_id, &normalized_path, progress).await {
        Ok(output) => output,
        Err(err) => {
            cleanup_temp_path(&normalized_path).await;
            return Err(err);
        }
    };

    cleanup_temp_path(&normalized_path).await;
    upload_hls_stage(file_id, hls_output, storage, progress).await
}

// ─── Нормализация громкости (ffmpeg loudnorm) ──────────────────────────

#[derive(Debug, Deserialize)]
struct LoudnormStats {
    input_i: String,
    input_tp: String,
    input_lra: String,
    input_thresh: String,
    target_offset: String,
}

async fn normalize_loudness(input_path: &Path) -> Result<PathBuf> {
    let output_path = input_path.with_extension("normalized.wav");
    let input_str = input_path.to_str().context("non utf-8 input path")?;
    let output_str = output_path.to_str().context("non utf-8 output path")?;

    let stats = measure_loudness(input_str).await?;
    apply_loudness_normalization(input_str, output_str, &stats).await?;

    if fs::metadata(&output_path).await.is_err() {
        bail!("ffmpeg did not create normalized file");
    }

    info!(
        "Loudness normalized: {} -> {}",
        input_path.display(),
        output_path.display()
    );

    Ok(output_path)
}

async fn measure_loudness(input_str: &str) -> Result<LoudnormStats> {
    let filter = build_measure_loudnorm_filter();
    let args = build_measure_loudnorm_args(input_str, &filter);
    let output = Command::new("ffmpeg")
        .args(&args)
        .output()
        .await
        .context("Failed to start ffmpeg loudnorm first pass")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg loudnorm first pass failed: {}",
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        );
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let json = extract_loudnorm_json(&stderr)?;
    serde_json::from_str::<LoudnormStats>(&json)
        .context("Failed to parse loudnorm first pass output")
}

async fn apply_loudness_normalization(
    input_str: &str,
    output_str: &str,
    stats: &LoudnormStats,
) -> Result<()> {
    let filter = build_second_pass_loudnorm_filter(stats);
    let args = build_apply_loudnorm_args(input_str, output_str, &filter);
    let output = Command::new("ffmpeg")
        .args(&args)
        .output()
        .await
        .context("Failed to start ffmpeg loudnorm second pass")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg loudnorm second pass failed: {}",
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        );
    }

    Ok(())
}

fn build_measure_loudnorm_args(input_str: &str, filter: &str) -> Vec<String> {
    vec![
        "-i".to_string(),
        input_str.to_string(),
        FFMPEG_HIDE_BANNER_FLAG.to_string(),
        FFMPEG_NO_STATS_FLAG.to_string(),
        "-af".to_string(),
        filter.to_string(),
        "-f".to_string(),
        FFMPEG_NULL_FORMAT.to_string(),
        "-".to_string(),
    ]
}

fn build_apply_loudnorm_args(input_str: &str, output_str: &str, filter: &str) -> Vec<String> {
    vec![
        "-i".to_string(),
        input_str.to_string(),
        "-v".to_string(),
        FFMPEG_LOG_LEVEL.to_string(),
        "-af".to_string(),
        filter.to_string(),
        "-ar".to_string(),
        NORMALIZED_SAMPLE_RATE.to_string(),
        "-ac".to_string(),
        NORMALIZED_CHANNELS.to_string(),
        OVERWRITE_OUTPUT_FLAG.to_string(),
        output_str.to_string(),
    ]
}

fn build_measure_loudnorm_filter() -> String {
    format!(
        "loudnorm=I={}:TP={}:LRA={}:print_format=json",
        LOUDNORM_TARGET_I, LOUDNORM_TARGET_TP, LOUDNORM_TARGET_LRA
    )
}

async fn execute_pipeline_with_retries(
    file_id: Uuid,
    temp_path: &str,
    storage: &Arc<dyn StorageBackend>,
    progress: &ProgressMap,
    max_retries: u32,
) -> Result<PipelineResult> {
    let mut last_error = anyhow!("pipeline failed without error details");

    for attempt in 1..=max_retries {
        log_pipeline_attempt_start(attempt, max_retries, file_id);

        match execute_pipeline(file_id, temp_path, storage, progress).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                log_pipeline_attempt_failure(attempt, max_retries, file_id, &err);
                last_error = err;

                if attempt < max_retries {
                    wait_before_retry(attempt).await;
                }
            }
        }
    }

    Err(last_error)
}

async fn ensure_source_file_exists(temp_path: &str) -> Result<PathBuf> {
    let input = PathBuf::from(temp_path);
    if fs::metadata(&input).await.is_err() {
        bail!("Temporary file not found: {}", temp_path);
    }
    Ok(input)
}

async fn run_normalization_stage(
    file_id: Uuid,
    input: &Path,
    progress: &ProgressMap,
) -> Result<PathBuf> {
    set_progress(progress, file_id, WorkerStage::Normalizing, 10, None);
    let normalized_path = normalize_loudness(input).await?;
    set_progress(progress, file_id, WorkerStage::Normalizing, 30, None);
    Ok(normalized_path)
}

async fn run_hls_conversion_stage(
    file_id: Uuid,
    normalized_path: &Path,
    progress: &ProgressMap,
) -> Result<hls::HlsOutput> {
    set_progress(
        progress,
        file_id,
        WorkerStage::Converting,
        40,
        Some("Конвертация в HLS".into()),
    );

    let hls_input = normalized_path.to_path_buf();
    let entry_playlist_name = format!("{}.m3u8", file_id);
    let hls_output = hls::convert_to_hls(&hls_input, &entry_playlist_name)
        .await
        .context("HLS conversion failed")?;
    set_progress(progress, file_id, WorkerStage::Converting, 70, None);
    Ok(hls_output)
}

async fn upload_hls_stage(
    file_id: Uuid,
    hls_output: hls::HlsOutput,
    storage: &Arc<dyn StorageBackend>,
    progress: &ProgressMap,
) -> Result<PipelineResult> {
    set_progress(
        progress,
        file_id,
        WorkerStage::Uploading,
        75,
        Some("Загрузка в хранилище".into()),
    );

    let upload_prefix = format!("media/{}", file_id);

    let upload_result = perform_hls_upload(storage, &hls_output, &upload_prefix).await;
    if let Err(err) = upload_result {
        hls_output.cleanup().await;
        return Err(err);
    }

    let result = PipelineResult {
        hls_path: format!("/media/{}/{}", file_id, hls_output.playlist_name),
        duration: hls_output.duration_secs.unwrap_or(0.0),
        bitrates: hls_output.bitrates.clone(),
    };

    hls_output.cleanup().await;
    set_progress(progress, file_id, WorkerStage::Uploading, 95, None);

    Ok(result)
}

async fn perform_hls_upload(
    storage: &Arc<dyn StorageBackend>,
    hls_output: &hls::HlsOutput,
    upload_prefix: &str,
) -> Result<()> {
    storage
        .ensure_bucket(HLS_BUCKET)
        .await
        .context("Failed to create bucket")?;
    storage
        .upload_hls_output(&hls_output, HLS_BUCKET, &upload_prefix)
        .await
        .context("Error uploading to storage")
}

fn mark_pipeline_queued(file_id: Uuid, progress: &ProgressMap) {
    progress.insert(
        file_id,
        WorkerProgress {
            stage: WorkerStage::Queued,
            percent: 0,
            message: Some("Задача принята в обработку".into()),
        },
    );
}

async fn finalize_successful_pipeline(
    file_id: Uuid,
    result: &PipelineResult,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
    temp_path: &str,
) {
    if let Err(e) = kafka
        .send_converted(
            file_id,
            &result.hls_path,
            result.duration,
            result.bitrates.clone(),
        )
        .await
    {
        warn!("Failed to publish media.worker.converted: {}", e);
    }

    progress.insert(
        file_id,
        WorkerProgress {
            stage: WorkerStage::Done,
            percent: 100,
            message: Some(format!("Обработка завершена: {}", result.hls_path)),
        },
    );

    cleanup_temp(temp_path).await;
}

async fn finalize_failed_pipeline(
    file_id: Uuid,
    err: &anyhow::Error,
    kafka: &SharedKafkaProducer,
    progress: &ProgressMap,
    temp_path: &str,
) {
    let error_message = format!("{err:#}");
    error!(
        "Pipeline failed after retries for file_id={}: {}",
        file_id, error_message
    );

    if let Err(e) = kafka
        .send_worker_error(file_id, "conversion", &error_message)
        .await
    {
        warn!("Failed to publish media.worker.error: {}", e);
    }

    progress.insert(
        file_id,
        WorkerProgress {
            stage: WorkerStage::Error,
            percent: 0,
            message: Some(error_message),
        },
    );

    cleanup_temp(temp_path).await;
}

fn log_pipeline_attempt_start(attempt: u32, max_retries: u32, file_id: Uuid) {
    info!(
        "Pipeline attempt {}/{} for file_id={}",
        attempt, max_retries, file_id
    );
}

fn log_pipeline_attempt_failure(
    attempt: u32,
    max_retries: u32,
    file_id: Uuid,
    err: &anyhow::Error,
) {
    warn!(
        "Pipeline attempt {}/{} failed for file_id={}: {}",
        attempt,
        max_retries,
        file_id,
        format!("{err:#}")
    );
}

async fn wait_before_retry(attempt: u32) {
    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
}

fn build_second_pass_loudnorm_filter(stats: &LoudnormStats) -> String {
    format!(
        "loudnorm=I={}:TP={}:LRA={}:measured_I={}:measured_TP={}:measured_LRA={}:measured_thresh={}:offset={}:linear=true:print_format=summary",
        LOUDNORM_TARGET_I,
        LOUDNORM_TARGET_TP,
        LOUDNORM_TARGET_LRA,
        stats.input_i,
        stats.input_tp,
        stats.input_lra,
        stats.input_thresh,
        stats.target_offset,
    )
}

fn extract_loudnorm_json(stderr: &str) -> Result<String> {
    let start = stderr
        .find('{')
        .context("ffmpeg loudnorm first pass did not return JSON start")?;
    let end = stderr
        .rfind('}')
        .context("ffmpeg loudnorm first pass did not return JSON end")?;

    if end < start {
        bail!("ffmpeg loudnorm first pass returned malformed JSON");
    }

    Ok(stderr[start..=end].to_string())
}

fn set_progress(
    map: &ProgressMap,
    id: Uuid,
    stage: WorkerStage,
    percent: u8,
    message: Option<String>,
) {
    map.insert(
        id,
        WorkerProgress {
            stage,
            percent,
            message,
        },
    );
}

async fn cleanup_temp(path: &str) {
    if let Err(e) = tokio::fs::remove_file(path).await {
        debug!("Failed to cleanup temp file {}: {}", path, e);
    }
}

async fn cleanup_temp_path(path: &Path) {
    if let Some(path) = path.to_str() {
        cleanup_temp(path).await;
        return;
    }

    if let Err(e) = tokio::fs::remove_file(path).await {
        debug!("Failed to cleanup temp file {}: {}", path.display(), e);
    }
}
