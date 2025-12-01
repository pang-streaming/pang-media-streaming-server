use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use log::{info, warn, error};
use chrono::{Utc, DateTime};
use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};

type HmacSha256 = Hmac<Sha256>;

/// Reqwest 기반 S3 업로더 (AWS SDK 대신 직접 REST API 호출)
pub struct ReqwestS3Uploader {
    client: reqwest::Client,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    active_uploads: Arc<RwLock<HashMap<String, UploadState>>>,
    semaphore: Arc<Semaphore>,
    max_retries: u8,
}

#[derive(Debug, Clone)]
pub enum UploadState {
    Uploading,
    Completed,
    Failed(String),
}

impl ReqwestS3Uploader {
    pub fn new(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
        max_concurrent: usize,
    ) -> Self {
        // 커스텀 HTTP 클라이언트 (커넥션 풀 설정)
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .timeout(std::time::Duration::from_secs(60))
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        info!("[ReqwestS3] Initialized with {} concurrent uploads", max_concurrent);

        Self {
            client,
            bucket,
            region,
            access_key,
            secret_key,
            active_uploads: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(16))),
            max_retries: 3,
        }
    }

    /// 데이터를 S3에 업로드 (비동기)
    pub async fn queue_data(
        &self,
        stream_name: &str,
        file_name: &str,
        data: Vec<u8>,
        content_type: &str,
        _priority: u8,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = format!("{}/{}", stream_name, file_name);
        let file_size_kb = data.len() / 1024;

        info!("[S3 Queue] {} ({}KB)", file_name, file_size_kb);

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let region = self.region.clone();
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let semaphore = Arc::clone(&self.semaphore);
        let active_uploads = Arc::clone(&self.active_uploads);
        let max_retries = self.max_retries;
        let content_type = content_type.to_string();

        tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(_) => return,
            };

            {
                let mut uploads = active_uploads.write().await;
                uploads.insert(key.clone(), UploadState::Uploading);
            }

            let upload_start = std::time::Instant::now();
            let mut last_error = None;

            for attempt in 0..=max_retries {
                match upload_to_s3_direct(
                    &client,
                    &bucket,
                    &region,
                    &access_key,
                    &secret_key,
                    &key,
                    &data,
                    &content_type,
                ).await {
                    Ok(_) => {
                        let elapsed = upload_start.elapsed();
                        {
                            let mut uploads = active_uploads.write().await;
                            uploads.insert(key.clone(), UploadState::Completed);
                        }

                        let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
                            (file_size_kb as f64 / 1024.0) / elapsed.as_secs_f64()
                        } else {
                            0.0
                        };

                        info!("[S3 Done] {} ({}KB) in {:?} ({:.2} MB/s)",
                            key, file_size_kb, elapsed, speed_mbps);
                        return;
                    }
                    Err(e) => {
                        last_error = Some(format!("{:?}", e));
                        if attempt < max_retries {
                            let backoff = 100 * (attempt + 1) as u64;
                            warn!("[S3 Retry] {} (attempt {}/{}, wait {}ms): {}",
                                key, attempt + 1, max_retries + 1, backoff,
                                last_error.as_ref().unwrap_or(&String::new()));
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
                        }
                    }
                }
            }

            {
                let mut uploads = active_uploads.write().await;
                uploads.insert(key.clone(), UploadState::Failed(last_error.clone().unwrap_or_default()));
            }

            error!("[S3 Failed] {} after {} attempts: {}",
                key, max_retries + 1, last_error.unwrap_or_default());
        });

        Ok(())
    }

    pub async fn pending_count(&self) -> usize {
        let uploads = self.active_uploads.read().await;
        uploads.values().filter(|s| matches!(s, UploadState::Uploading)).count()
    }

    pub async fn active_count(&self) -> usize {
        self.pending_count().await
    }
}

/// S3에 직접 PUT 요청 (AWS Signature V4)
async fn upload_to_s3_direct(
    client: &reqwest::Client,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    key: &str,
    data: &[u8],
    content_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now: DateTime<Utc> = Utc::now();
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

    let host = format!("{}.s3.{}.amazonaws.com", bucket, region);
    let url = format!("https://{}/{}", host, key);

    // 페이로드 해시
    let payload_hash = hex::encode(Sha256::digest(data));

    // Cache-Control 헤더
    let cache_control = if key.ends_with(".m3u8") {
        "no-cache, no-store, must-revalidate"
    } else if key.ends_with("init.mp4") {
        "public, max-age=86400"
    } else {
        "public, max-age=3600"
    };

    // Canonical Request
    let canonical_uri = format!("/{}", key);
    let canonical_querystring = "";
    let canonical_headers = format!(
        "cache-control:{}\ncontent-type:{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        cache_control, content_type, host, payload_hash, amz_date
    );
    let signed_headers = "cache-control;content-type;host;x-amz-content-sha256;x-amz-date";

    let canonical_request = format!(
        "PUT\n{}\n{}\n{}\n{}\n{}",
        canonical_uri,
        canonical_querystring,
        canonical_headers,
        signed_headers,
        payload_hash
    );

    // String to Sign
    let algorithm = "AWS4-HMAC-SHA256";
    let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, region);
    let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        algorithm, amz_date, credential_scope, canonical_request_hash
    );

    // Signing Key
    let k_date = sign(format!("AWS4{}", secret_key).as_bytes(), &date_stamp);
    let k_region = sign(&k_date, region);
    let k_service = sign(&k_region, "s3");
    let k_signing = sign(&k_service, "aws4_request");

    // Signature
    let signature = hex::encode(sign(&k_signing, &string_to_sign));

    // Authorization Header
    let authorization = format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        algorithm, access_key, credential_scope, signed_headers, signature
    );

    // HTTP 요청
    let response = client
        .put(&url)
        .header("Host", &host)
        .header("Content-Type", content_type)
        .header("Cache-Control", cache_control)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("Authorization", &authorization)
        .body(data.to_vec())
        .send()
        .await?;

    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(format!("S3 upload failed: {} - {}", status, body).into())
    }
}

/// HMAC-SHA256 서명
fn sign(key: &[u8], msg: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(msg.as_bytes());
    mac.finalize().into_bytes().to_vec()
}
