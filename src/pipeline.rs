use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use serde::Deserialize;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::hls;
use crate::kafka::SharedKafkaProducer;
use crate::progress::{ProgressMap, WorkerProgress, WorkerStage};
use crate::storage::StorageBackend;

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
) {
    let max_retries = load_max_retries();

    progress.insert(
        file_id,
        WorkerProgress {
            stage: WorkerStage::Queued,
            percent: 0,
            message: Some("Задача принята в обработку".into()),
        },
    );

    let mut last_error = String::new();

    for attempt in 1..=max_retries {
        info!(
            "Pipeline attempt {}/{} for file_id={}",
            attempt, max_retries, file_id
        );

        match execute_pipeline(file_id, temp_path, &storage, &progress).await {
            Ok(result) => {
                if let Err(e) = kafka
                    .send_converted(file_id, &result.hls_path, result.duration, result.bitrates)
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

                return;
            }
            Err(e) => {
                last_error = e.clone();
                warn!(
                    "Pipeline attempt {}/{} failed for file_id={}: {}",
                    attempt,
                    max_retries,
                    file_id,
                    e
                );

                if attempt < max_retries {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
                }
            }
        }
    }

    error!(
        "Pipeline failed after {} retries for file_id={}: {}",
        max_retries,
        file_id,
        last_error
    );

    if let Err(e) = kafka
        .send_worker_error(file_id, "conversion", &last_error)
        .await
    {
        warn!("Failed to publish media.worker.error: {}", e);
    }

    progress.insert(
        file_id,
        WorkerProgress {
            stage: WorkerStage::Error,
            percent: 0,
            message: Some(last_error),
        },
    );

    cleanup_temp(temp_path).await;
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
) -> Result<PipelineResult, String> {
    let input = PathBuf::from(temp_path);

    if !input.exists() {
        return Err(format!("Temporary file not found: {}", temp_path));
    }

    // ── 1. Нормализация громкости (loudnorm) ───────────────────────────
    set_progress(progress, file_id, WorkerStage::Normalizing, 10, None);

    let normalized_path = normalize_loudness(&input)?;

    set_progress(progress, file_id, WorkerStage::Normalizing, 30, None);

    // ── 2. HLS конвертация ──────────────────────────────────────────────
    set_progress(
        progress,
        file_id,
        WorkerStage::Converting,
        40,
        Some("Конвертация в HLS".into()),
    );

    let hls_input = normalized_path.clone();
    let entry_playlist_name = format!("{}.m3u8", file_id);
    let hls_result =
        tokio::task::spawn_blocking(move || hls::convert_to_hls(&hls_input, &entry_playlist_name))
            .await
            .map_err(|e| format!("HLS task panicked: {}", e))?;

    let hls_output = hls_result.map_err(|e| format!("HLS conversion failed: {}", e))?;

    set_progress(progress, file_id, WorkerStage::Converting, 70, None);

    // Чистим нормализованный файл
    let _ = tokio::fs::remove_file(&normalized_path).await;

    // ── 3. Загрузка в RustFS ────────────────────────────────────────────
    set_progress(
        progress,
        file_id,
        WorkerStage::Uploading,
        75,
        Some("Загрузка в хранилище".into()),
    );

    let upload_prefix = format!("media/{}", file_id);

    storage
        .ensure_bucket(HLS_BUCKET)
        .await
        .map_err(|e| format!("Failed to create bucket: {}", e))?;

    storage
        .upload_hls_output(&hls_output, HLS_BUCKET, &upload_prefix)
        .await
        .map_err(|e| format!("Error uploading to storage: {}", e))?;

    let hls_path = format!("/media/{}/{}", file_id, hls_output.playlist_name);
    let duration = hls_output.duration_secs.unwrap_or(0.0);
    let bitrates = hls_output.bitrates.clone();

    hls_output.cleanup().await;

    set_progress(progress, file_id, WorkerStage::Uploading, 95, None);

    Ok(PipelineResult {
        hls_path,
        duration,
        bitrates,
    })
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

fn normalize_loudness(input_path: &Path) -> Result<PathBuf, String> {
    let output_path = input_path.with_extension("normalized.wav");
    let input_str = input_path.to_str().ok_or("non utf-8 input path")?;
    let output_str = output_path.to_str().ok_or("non utf-8 output path")?;

    let stats = measure_loudness(input_str)?;
    apply_loudness_normalization(input_str, output_str, &stats)?;

    if !output_path.exists() {
        return Err("ffmpeg did not create normalized file".to_string());
    }

    info!(
        "Loudness normalized: {} -> {}",
        input_path.display(),
        output_path.display()
    );

    Ok(output_path)
}

fn measure_loudness(input_str: &str) -> Result<LoudnormStats, String> {
    let filter = build_measure_loudnorm_filter();
    let args = build_measure_loudnorm_args(input_str, &filter);
    let output = Command::new("ffmpeg")
        .args(&args)
        .output()
        .map_err(|e| format!("Failed to start ffmpeg loudnorm first pass: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "ffmpeg loudnorm first pass failed: {}",
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        ));
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let json = extract_loudnorm_json(&stderr)?;
    serde_json::from_str::<LoudnormStats>(&json)
        .map_err(|e| format!("Failed to parse loudnorm first pass output: {}", e))
}

fn apply_loudness_normalization(
    input_str: &str,
    output_str: &str,
    stats: &LoudnormStats,
) -> Result<(), String> {
    let filter = build_second_pass_loudnorm_filter(stats);
    let args = build_apply_loudnorm_args(input_str, output_str, &filter);
    let output = Command::new("ffmpeg")
        .args(&args)
        .output()
        .map_err(|e| format!("Failed to start ffmpeg loudnorm second pass: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "ffmpeg loudnorm second pass failed: {}",
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        ));
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

fn extract_loudnorm_json(stderr: &str) -> Result<String, String> {
    let start = stderr
        .find('{')
        .ok_or("ffmpeg loudnorm first pass did not return JSON start")?;
    let end = stderr
        .rfind('}')
        .ok_or("ffmpeg loudnorm first pass did not return JSON end")?;

    if end < start {
        return Err("ffmpeg loudnorm first pass returned malformed JSON".to_string());
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
