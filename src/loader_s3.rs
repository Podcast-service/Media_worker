use std::{env, path::Path};

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream, Client};
use tokio::fs;
use tracing::{error, info, warn};

const S3_UPLOAD_BUFFER_SIZE_BYTES_ENV: &str = "S3_UPLOAD_BUFFER_SIZE_BYTES";
const DEFAULT_S3_UPLOAD_BUFFER_SIZE_BYTES: usize = 64 * 1024;
const MIN_S3_UPLOAD_BUFFER_SIZE_BYTES: usize = 4 * 1024;
const DEFAULT_S3_REGION: &str = "ru-1";
const DEFAULT_S3_ENDPOINT_URL: &str = "https://s3.twcstorage.ru";

pub struct Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            region: env_value("S3_REGION").unwrap_or_else(|| DEFAULT_S3_REGION.to_string()),
            access_key_id: env_value("S3_ACCESS_KEY_ID").context("S3_ACCESS_KEY_ID is required")?,
            secret_access_key: env_value("S3_SECRET_ACCESS_KEY")
                .context("S3_SECRET_ACCESS_KEY is required")?,
            endpoint_url: env_value("S3_ENDPOINT_URL")
                .unwrap_or_else(|| DEFAULT_S3_ENDPOINT_URL.to_string()),
        })
    }
}

pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn new_bucket(&self, bucket: &str) -> Result<()> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("Bucket '{}' is available", bucket);
                return Ok(());
            }
            Err(err) if !should_create_bucket() => {
                return Err(err).with_context(|| {
                    format!(
                        "bucket '{bucket}' is not available; check S3_BUCKET and credentials, or set S3_CREATE_BUCKET=true for local S3-compatible storage"
                    )
                });
            }
            Err(err) => {
                warn!(
                    "Bucket '{}' is not available, trying to create it: code={:?}, message={:?}",
                    bucket,
                    err.code(),
                    err.message()
                );
            }
        }

        match self.client.create_bucket().bucket(bucket).send().await {
            Ok(_) => info!("Bucket '{}' created successfully", bucket),
            Err(err) if err.code() == Some("BucketAlreadyOwnedByYou") => {
                info!("Bucket '{}' already exists, skip create", bucket);
            }
            Err(err) => {
                error!(
                    "create_bucket error: code={:?}, message={:?}, raw={:?}",
                    err.code(),
                    err.message(),
                    err
                );
                return Err(err).with_context(|| format!("create_bucket failed for {bucket}"));
            }
        }
        Ok(())
    }

    pub async fn upload_file(
        &self,
        filepath: &Path,
        object_key: &str,
        bucket_name: &str,
    ) -> Result<()> {
        let metadata = fs::metadata(filepath)
            .await
            .with_context(|| format!("can not stat file {}", filepath.display()))?;

        if !metadata.is_file() {
            anyhow::bail!("file does not exist: {}", filepath.display());
        }

        let size_bytes = metadata.len();
        let buffer_size = load_upload_buffer_size_bytes();
        let body = ByteStream::read_from()
            .path(filepath)
            .buffer_size(buffer_size)
            .build()
            .await
            .with_context(|| format!("can not open file {}", filepath.display()))?;

        self.client
            .put_object()
            .bucket(bucket_name)
            .key(object_key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("error uploading '{object_key}' to bucket '{bucket_name}'"))?;

        info!(
            "uploaded: bucket='{}', object='{}', bytes={}",
            bucket_name, object_key, size_bytes
        );
        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, object_key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(object_key)
            .send()
            .await
            .with_context(|| {
                format!("error deleting object '{object_key}' from bucket '{bucket}'")
            })?;

        info!("deleted object '{object_key}' from bucket '{bucket}'");
        Ok(())
    }

    pub async fn download_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        local_path: &Path,
    ) -> Result<()> {
        let response = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(object_key)
            .send()
            .await
            .with_context(|| {
                format!("error downloading '{object_key}' from bucket '{bucket_name}'")
            })?;

        let bytes = response
            .body
            .collect()
            .await
            .with_context(|| {
                format!("error reading object body '{object_key}' from bucket '{bucket_name}'")
            })?
            .into_bytes();

        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        fs::write(local_path, &bytes)
            .await
            .with_context(|| format!("failed to write {}", local_path.display()))?;

        info!(
            "downloaded: bucket='{}', object='{}', path='{}', bytes={}",
            bucket_name,
            object_key,
            local_path.display(),
            bytes.len()
        );

        Ok(())
    }

    pub async fn delete_by_prefix(&self, bucket: &str, prefix: &str) -> Result<u32> {
        let mut deleted = 0;
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket).prefix(prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.with_context(|| {
                format!("error listing objects with prefix '{prefix}' in bucket '{bucket}'")
            })?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    self.delete_object(bucket, key).await?;
                    deleted += 1;
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        info!(
            "deleted {} objects with prefix '{}' from bucket '{}'",
            deleted, prefix, bucket
        );
        Ok(deleted)
    }
}

pub async fn create_client(cfg: &Config) -> Result<S3Client> {
    let credentials = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "s3-compatible",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(cfg.region.clone()))
        .credentials_provider(credentials)
        .endpoint_url(cfg.endpoint_url.clone())
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .build();

    Ok(S3Client::new(Client::from_conf(s3_config)))
}

fn load_upload_buffer_size_bytes() -> usize {
    match env::var(S3_UPLOAD_BUFFER_SIZE_BYTES_ENV) {
        Ok(value) => match value.parse::<usize>() {
            Ok(size) if size < MIN_S3_UPLOAD_BUFFER_SIZE_BYTES => {
                warn!(
                    "{}={} is too small, using minimum {}",
                    S3_UPLOAD_BUFFER_SIZE_BYTES_ENV, size, MIN_S3_UPLOAD_BUFFER_SIZE_BYTES
                );
                MIN_S3_UPLOAD_BUFFER_SIZE_BYTES
            }
            Ok(size) => size,
            Err(e) => {
                warn!(
                    "Failed to parse {}='{}': {}, using default {}",
                    S3_UPLOAD_BUFFER_SIZE_BYTES_ENV, value, e, DEFAULT_S3_UPLOAD_BUFFER_SIZE_BYTES
                );
                DEFAULT_S3_UPLOAD_BUFFER_SIZE_BYTES
            }
        },
        Err(_) => DEFAULT_S3_UPLOAD_BUFFER_SIZE_BYTES,
    }
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn should_create_bucket() -> bool {
    env::var("S3_CREATE_BUCKET")
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

use async_trait::async_trait;

use crate::storage::StorageBackend;

#[async_trait]
impl StorageBackend for S3Client {
    fn name(&self) -> &str {
        "s3"
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        S3Client::new_bucket(self, bucket).await
    }

    async fn upload_file(
        &self,
        local_path: &Path,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<()> {
        S3Client::upload_file(self, local_path, object_key, bucket_name).await
    }

    async fn download_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        local_path: &Path,
    ) -> Result<()> {
        S3Client::download_file(self, bucket_name, object_key, local_path).await
    }

    async fn delete_object(&self, bucket: &str, object_key: &str) -> Result<()> {
        S3Client::delete_object(self, bucket, object_key).await
    }

    async fn delete_by_prefix(&self, bucket: &str, prefix: &str) -> Result<u32> {
        S3Client::delete_by_prefix(self, bucket, prefix).await
    }
}
