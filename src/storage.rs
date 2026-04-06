use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;
use tracing::info;

use crate::hls::HlsOutput;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    fn name(&self) -> &str;

    async fn ensure_bucket(&self, bucket: &str) -> Result<()>;

    async fn upload_file(
        &self,
        local_path: &Path,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<()>;

    async fn upload_hls_output(&self, hls: &HlsOutput, bucket: &str, prefix: &str) -> Result<()> {
        let files = hls
            .list_files_relative()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        info!(
            "[{}] Uploading {} HLS files to {}/{}",
            self.name(),
            files.len(),
            bucket,
            prefix
        );

        for (local_path, rel_key) in &files {
            let object_key = if prefix.is_empty() {
                rel_key.clone()
            } else {
                format!("{}/{}", prefix, rel_key)
            };

            self.upload_file(local_path, bucket, &object_key).await?;
        }

        info!(
            "[{}] HLS upload complete: {}/{}",
            self.name(),
            bucket,
            prefix
        );

        Ok(())
    }

    async fn delete_object(&self, bucket: &str, object_key: &str) -> Result<()>;

    async fn delete_by_prefix(&self, bucket: &str, prefix: &str) -> Result<u32>;
}
