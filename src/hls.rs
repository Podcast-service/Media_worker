use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use tokio::fs;
use uuid::Uuid;
use tracing::warn;

const BITRATES: &[(u32, &str)] = &[(64, "64k"), (128, "128k"), (256, "256k")];

#[derive(Debug)]
pub struct HlsOutput {
    pub output_dir: PathBuf,
    pub playlist_name: String,
    pub duration_secs: Option<f64>,
    pub bitrates: Vec<u32>,
}

impl HlsOutput {
    /// Рекурсивно собирает все файлы с относительными путями от `output_dir`
    pub async fn list_files_relative(&self) -> Result<Vec<(PathBuf, String)>, String> {
        let mut result = Vec::new();
        collect_files_recursive(&self.output_dir, &self.output_dir, &mut result).await?;
        result.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(result)
    }

    pub async fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.output_dir).await;
    }
}

/// Рекурсивный обход: собирает `(абсолютный_путь, относительный_ключ)`
async fn collect_files_recursive(
    base: &Path,
    dir: &Path,
    out: &mut Vec<(PathBuf, String)>,
) -> Result<(), String> {
    let mut entries = fs::read_dir(dir)
        .await
         .map_err(|e| format!("Failed to read directory {}: {}", dir.display(), e))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("Read error: {}", e))?
    {
        let path = entry.path();
        if path.is_dir() {
            Box::pin(collect_files_recursive(base, &path, out)).await?;
        } else if path.is_file() {
            let rel = path
                .strip_prefix(base)
                .map_err(|e| format!("strip_prefix: {}", e))?
                .to_string_lossy()
                .to_string();
            out.push((path, rel));
        } else {
            warn!("Unexpected entry type: {}", path.display());
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
pub fn convert_to_hls(input_path: &Path, playlist_name: &str) -> Result<HlsOutput, String> {
    let hls_dir = create_hls_dir()?;

    let result = (|| {
        let input_str = input_path.to_str().ok_or("non utf-8 input path")?;
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

fn create_hls_dir() -> Result<PathBuf, String> {
    let hls_dir = std::env::temp_dir().join(format!("hls_{}", Uuid::new_v4()));

    std::fs::create_dir_all(&hls_dir)
        .map_err(|e| format!("Failed to create HLS directory: {}", e))?;

    Ok(hls_dir)
}

fn cleanup_hls_dir(hls_dir: &Path) {
    let _ = std::fs::remove_dir_all(hls_dir);
}

fn generate_variant_playlists(input_str: &str, hls_dir: &Path) -> Result<Vec<u32>, String> {
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
) -> Result<(), String> {
    let variant_dir = hls_dir.join(label);
    std::fs::create_dir_all(&variant_dir)
        .map_err(|e| format!("Failed to create directory {}: {}", label, e))?;

    let playlist_path = variant_dir.join("playlist.m3u8");
    let segment_pattern = variant_dir.join("seg_%05d.m4s");

    let output = Command::new("ffmpeg")
        .args([
            "-i",
            input_str,
            "-v",
            "error",
            "-c:a",
            "aac",
            "-b:a",
            &format!("{}k", kbps),
            "-ac",
            "2",
            "-ar",
            "48000",
            "-f",
            "hls",
            "-hls_time",
            "6",
            "-hls_playlist_type",
            "vod",
            "-hls_segment_type",
            "fmp4",
            "-hls_segment_filename",
            segment_pattern.to_str().ok_or("non utf-8 path")?,
        ])
        .arg(playlist_path.to_str().ok_or("non utf-8 path")?)
        .output()
        .map_err(|e| format!("ffmpeg {} failed to start: {}", label, e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "ffmpeg HLS {} failed: {}",
            label,
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        ));
    }

    if !playlist_path.exists() {
        return Err(format!("ffmpeg did not create playlist for {}", label));
    }

    Ok(())
}

fn write_master_playlist(hls_dir: &Path, playlist_name: &str) -> Result<(), String> {
    let master_path = hls_dir.join(playlist_name);
    let mut master = std::fs::File::create(&master_path)
        .map_err(|e| format!("Failed to create {}: {}", playlist_name, e))?;

    write_master_header(&mut master, playlist_name)?;

    for &(kbps, label) in BITRATES {
        write_variant_stream_info(&mut master, kbps, label, playlist_name)?;
    }

    Ok(())
}

fn write_master_header(master: &mut std::fs::File, playlist_name: &str) -> Result<(), String> {
    writeln!(master, "#EXTM3U") // спецификация требует, чтобы #EXTM3U был первой строкой в файле
        .map_err(|e| format!("Error writing {}: {}", playlist_name, e))
}

fn write_variant_stream_info(
    master: &mut std::fs::File,
    kbps: u32,
    label: &str,
    playlist_name: &str,
) -> Result<(), String> {
    writeln!(master, "{}", build_stream_info_tag(kbps))
        .map_err(|e| format!("Error writing {}: {}", playlist_name, e))?;
    writeln!(master, "{}/playlist.m3u8", label)
        .map_err(|e| format!("Error writing {}: {}", playlist_name, e))?;
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
        .args(["-v", "quiet", "-print_format", "json", "-show_format"])
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
