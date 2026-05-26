use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{header, Client, Url};
use serde_json::Value;
use tracing::{debug, warn};

const BASE_URL_ENV: &str = "PODCAST_API_BASE_URL";
const TIMEOUT_SECONDS_ENV: &str = "PODCAST_API_TIMEOUT_SECONDS";
const DEFAULT_TIMEOUT_SECONDS: u64 = 5;

struct PodcastApiConfig {
    base_url: Url,
    timeout: Duration,
}

impl PodcastApiConfig {
    fn from_env() -> Result<Option<Self>> {
        let Some(base_url) = std::env::var(BASE_URL_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };

        let base_url = Url::parse(&base_url)
            .with_context(|| format!("failed to parse {BASE_URL_ENV}='{base_url}'"))?;
        let timeout = Duration::from_secs(timeout_seconds_from_env());

        Ok(Some(Self { base_url, timeout }))
    }

    fn speakers_url(&self, podcast_id: &str) -> Result<Url> {
        let mut url = self.base_url.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("{} must be a base URL", BASE_URL_ENV))?;
            segments.pop_if_empty();
            segments.push("podcasts");
            segments.push(podcast_id);
            segments.push("speakers");
        }
        Ok(url)
    }
}

pub async fn fetch_speaker_count(podcast_id: &str) -> Result<Option<u32>> {
    let Some(config) = PodcastApiConfig::from_env()? else {
        debug!(
            "{} is not set; subtitle request will be sent without num_speakers",
            BASE_URL_ENV
        );
        return Ok(None);
    };

    let client = Client::builder()
        .timeout(config.timeout)
        .build()
        .context("failed to build podcast API client")?;
    let url = config.speakers_url(podcast_id)?;
    let response = client
        .get(url)
        .header(header::ACCEPT, "application/json")
        .send()
        .await
        .context("failed to request podcast speakers")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "podcast speakers request failed with status {}{}",
            status,
            if body.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", body.trim())
            }
        );
    }

    let value = response
        .json::<Value>()
        .await
        .context("failed to parse podcast speakers response as JSON")?;

    let speaker_count = extract_speaker_count(&value);
    if speaker_count.is_none() {
        warn!(
            "Podcast speakers response did not contain a recognizable count: {}",
            value
        );
    }

    Ok(speaker_count)
}

fn timeout_seconds_from_env() -> u64 {
    std::env::var(TIMEOUT_SECONDS_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
}

fn extract_speaker_count(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(to_u32),
        Value::Array(items) => to_u32(items.len() as u64),
        Value::Object(object) => {
            for key in [
                "num_speakers",
                "speaker_count",
                "speakers_count",
                "count",
                "total",
                "total_count",
                "number_of_speakers",
                "speakers",
            ] {
                if let Some(count) = object
                    .get(key)
                    .and_then(|item| item.as_u64())
                    .and_then(to_u32)
                {
                    return Some(count);
                }
            }

            for key in ["speakers", "items", "data", "results"] {
                if let Some(items) = object.get(key).and_then(|item| item.as_array()) {
                    return to_u32(items.len() as u64);
                }
            }

            for key in ["data", "result", "payload"] {
                if let Some(count) = object.get(key).and_then(extract_speaker_count) {
                    return Some(count);
                }
            }

            None
        }
        _ => None,
    }
    .filter(|count| *count > 0)
}

fn to_u32(value: u64) -> Option<u32> {
    u32::try_from(value).ok()
}
