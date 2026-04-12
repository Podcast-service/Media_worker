use std::{env, path::Path};

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream, Client};
use tokio::fs;
use tracing::{error, info};

pub struct Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            region: env::var("RUSTFS_REGION")?,
            access_key_id: env::var("RUSTFS_ACCESS_KEY_ID")?,
            secret_access_key: env::var("RUSTFS_SECRET_ACCESS_KEY")?,
            endpoint_url: env::var("RUSTFS_ENDPOINT_URL")?,
        })
    }
}

pub struct RustFsClient {
    client: Client,
}

impl RustFsClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn new_bucket(&self, bucket: &str) -> Result<()> {
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
        if !filepath.exists() {
            anyhow::bail!("file does not exist: {}", filepath.display());
        }

        let data = fs::read(filepath)
            .await
            .with_context(|| format!("can not open file {}", filepath.display()))?;
        let size_bytes = data.len();

        self.client
            .put_object()
            .bucket(bucket_name)
            .key(object_key)
            .body(ByteStream::from(data))
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

pub async fn create_client(cfg: &Config) -> Result<RustFsClient> {
    let credentials = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "rustfs",
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

    Ok(RustFsClient::new(Client::from_conf(s3_config)))
}

use async_trait::async_trait;

use crate::storage::StorageBackend;

#[async_trait]
impl StorageBackend for RustFsClient {
    fn name(&self) -> &str {
        "rustfs"
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        RustFsClient::new_bucket(self, bucket).await
    }

    async fn upload_file(
        &self,
        local_path: &Path,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<()> {
        RustFsClient::upload_file(self, local_path, object_key, bucket_name).await
    }

    async fn delete_object(&self, bucket: &str, object_key: &str) -> Result<()> {
        RustFsClient::delete_object(self, bucket, object_key).await
    }

    async fn delete_by_prefix(&self, bucket: &str, prefix: &str) -> Result<u32> {
        RustFsClient::delete_by_prefix(self, bucket, prefix).await
    }
}
