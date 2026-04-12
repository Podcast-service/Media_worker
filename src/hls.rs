use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context, Result};
use tokio::fs;
use tracing::warn;
use uuid::Uuid;

const BITRATES: &[(u32, &str)] = &[(64, "64k"), (128, "128k"), (256, "256k")];
const VARIANT_PLAYLIST_FILE: &str = "playlist.m3u8";
const SEGMENT_FILE_PATTERN: &str = "seg_%05d.m4s";
const FFMPEG_LOG_LEVEL: &str = "error";
const HLS_AUDIO_CODEC: &str = "aac";
const HLS_AUDIO_CHANNELS: &str = "2";
const HLS_AUDIO_SAMPLE_RATE: &str = "48000";
const HLS_FORMAT: &str = "hls";
const HLS_SEGMENT_DURATION: &str = "6";
const HLS_PLAYLIST_TYPE: &str = "vod";
const HLS_SEGMENT_TYPE: &str = "fmp4";
const FFPROBE_DURATION_ARGS: &[&str] = &["-v", "quiet", "-print_format", "json", "-show_format"];

#[derive(Debug)]
pub struct HlsOutput {
    pub output_dir: PathBuf,
    pub playlist_name: String,
    pub duration_secs: Option<f64>,
    pub bitrates: Vec<u32>,
}

impl HlsOutput {
    /// Рекурсивно собирает все файлы с относительными путями от `output_dir`
    pub async fn list_files_relative(&self) -> Result<Vec<(PathBuf, String)>> {
        let mut result = Vec::new();
        collect_files_iterative(&self.output_dir, &mut result).await?;
        result.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(result)
    }

    pub async fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.output_dir).await;
    }
}

/// Итеративный обход: собирает `(абсолютный_путь, относительный_ключ)`
async fn collect_files_iterative(base: &Path, out: &mut Vec<(PathBuf, String)>) -> Result<()> {
    let mut dirs = vec![base.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        let mut entries = fs::read_dir(&dir)
            .await
            .with_context(|| format!("Failed to read directory {}", dir.display()))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .context("Failed to read directory entry")?
        {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.is_file() {
                let rel = path
                    .strip_prefix(base)
                    .context("Failed to build relative path")?
                    .to_string_lossy()
                    .to_string();
                out.push((path, rel));
            } else {
                warn!("Unexpected entry type: {}", path.display());
            }
        }
    }

    Ok(())
}

/// Структура на диске:
/// ```text
/// hls_{uuid}/
///   {playlist_name}       мастер-плейлист со ссылками на варианты
///   64k/
///     playlist.m3u8
///     seg_00000.m4s ...
///   ...
///   256k/
///     playlist.m3u8
///     seg_00000.m4s ...
/// ```
pub fn convert_to_hls(input_path: &Path, playlist_name: &str) -> Result<HlsOutput> {
    let hls_dir = create_hls_dir()?;

    let result = (|| {
        let input_str = input_path.to_str().context("non utf-8 input path")?;
        let generated_bitrates = generate_variant_playlists(input_str, &hls_dir)?;
        write_master_playlist(&hls_dir, playlist_name)?;

        Ok(HlsOutput {
            output_dir: hls_dir.clone(),
            playlist_name: playlist_name.to_string(),
            duration_secs: get_duration(input_path),
            bitrates: generated_bitrates,
        })
    })();

    match result {
        Ok(output) => Ok(output),
        Err(err) => {
            cleanup_hls_dir(&hls_dir);
            Err(err)
        }
    }
}

fn create_hls_dir() -> Result<PathBuf> {
    let hls_dir = std::env::temp_dir().join(format!("hls_{}", Uuid::new_v4()));

    std::fs::create_dir_all(&hls_dir).context("Failed to create HLS directory")?;

    Ok(hls_dir)
}

fn cleanup_hls_dir(hls_dir: &Path) {
    let _ = std::fs::remove_dir_all(hls_dir);
}

fn generate_variant_playlists(input_str: &str, hls_dir: &Path) -> Result<Vec<u32>> {
    let mut generated_bitrates = Vec::with_capacity(BITRATES.len());

    for &(kbps, label) in BITRATES {
        generate_variant_playlist(input_str, hls_dir, kbps, label)?;
        generated_bitrates.push(kbps);
    }

    Ok(generated_bitrates)
}

fn generate_variant_playlist(
    input_str: &str,
    hls_dir: &Path,
    kbps: u32,
    label: &str,
) -> Result<()> {
    let variant_dir = hls_dir.join(label);
    std::fs::create_dir_all(&variant_dir)
        .with_context(|| format!("Failed to create directory {}", label))?;

    let playlist_path = variant_dir.join(VARIANT_PLAYLIST_FILE);
    let segment_pattern = variant_dir.join(SEGMENT_FILE_PATTERN);
    let args = build_hls_ffmpeg_args(input_str, kbps, &segment_pattern, &playlist_path)?;

    let output = Command::new("ffmpeg")
        .args(&args)
        .output()
        .with_context(|| format!("ffmpeg {} failed to start", label))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg HLS {} failed: {}",
            label,
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        );
    }

    if !playlist_path.exists() {
        bail!("ffmpeg did not create playlist for {}", label);
    }

    Ok(())
}

fn build_hls_ffmpeg_args(
    input_str: &str,
    kbps: u32,
    segment_pattern: &Path,
    playlist_path: &Path,
) -> Result<Vec<String>> {
    let segment_pattern = segment_pattern.to_str().context("non utf-8 path")?;
    let playlist_path = playlist_path.to_str().context("non utf-8 path")?;

    Ok(vec![
        "-i".to_string(),
        input_str.to_string(),
        "-v".to_string(),
        FFMPEG_LOG_LEVEL.to_string(),
        "-c:a".to_string(),
        HLS_AUDIO_CODEC.to_string(),
        "-b:a".to_string(),
        format!("{}k", kbps),
        "-ac".to_string(),
        HLS_AUDIO_CHANNELS.to_string(),
        "-ar".to_string(),
        HLS_AUDIO_SAMPLE_RATE.to_string(),
        "-f".to_string(),
        HLS_FORMAT.to_string(),
        "-hls_time".to_string(),
        HLS_SEGMENT_DURATION.to_string(),
        "-hls_playlist_type".to_string(),
        HLS_PLAYLIST_TYPE.to_string(),
        "-hls_segment_type".to_string(),
        HLS_SEGMENT_TYPE.to_string(),
        "-hls_segment_filename".to_string(),
        segment_pattern.to_string(),
        playlist_path.to_string(),
    ])
}

fn write_master_playlist(hls_dir: &Path, playlist_name: &str) -> Result<()> {
    let master_path = hls_dir.join(playlist_name);
    let mut master = std::fs::File::create(&master_path)
        .with_context(|| format!("Failed to create {}", playlist_name))?;

    write_master_header(&mut master, playlist_name)?;

    for &(kbps, label) in BITRATES {
        write_variant_stream_info(&mut master, kbps, label, playlist_name)?;
    }

    Ok(())
}

fn write_master_header(master: &mut std::fs::File, playlist_name: &str) -> Result<()> {
    writeln!(master, "#EXTM3U") // спецификация требует, чтобы #EXTM3U был первой строкой в файле
        .with_context(|| format!("Error writing {}", playlist_name))
}

fn write_variant_stream_info(
    master: &mut std::fs::File,
    kbps: u32,
    label: &str,
    playlist_name: &str,
) -> Result<()> {
    writeln!(master, "{}", build_stream_info_tag(kbps))
        .with_context(|| format!("Error writing {}", playlist_name))?;
    writeln!(master, "{}/{}", label, VARIANT_PLAYLIST_FILE)
        .with_context(|| format!("Error writing {}", playlist_name))?;
    Ok(())
}

fn build_stream_info_tag(kbps: u32) -> String {
    format!(
        "#EXT-X-STREAM-INF:BANDWIDTH={},CODECS=\"mp4a.40.2\"",
        kbps * 1000
    )
}

/// Получаем длительность исходного файла через ffprobe
fn get_duration(input_path: &Path) -> Option<f64> {
    let output = Command::new("ffprobe")
        .args(FFPROBE_DURATION_ARGS)
        .arg(input_path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    parsed["format"]["duration"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
}
