use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use log::{info, warn, error, debug};
use chrono::Utc;

/// 업로드 상태
#[derive(Debug, Clone)]
pub enum UploadState {
    Uploading,
    Completed,
    Failed(String),
}

/// 메모리 세그먼트 데이터 (MemoryS3Uploader와 호환)
#[derive(Clone)]
pub struct CliSegment {
    pub stream_name: String,
    pub file_name: String,
    pub data: Vec<u8>,
    pub content_type: String,
    pub priority: u8,
    pub created_at: chrono::DateTime<Utc>,
}

/// AWS CLI 기반 S3 업로더
pub struct CliS3Uploader {
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    access_key: String,
    secret_key: String,
    active_uploads: Arc<RwLock<HashMap<String, UploadState>>>,
    semaphore: Arc<Semaphore>,
    max_retries: u8,
}

impl CliS3Uploader {
    pub fn new(
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        access_key: String,
        secret_key: String,
        worker_count: usize,
    ) -> Self {
        let effective_workers = worker_count.max(32);
        info!("[CliS3Uploader] Initialized with {} concurrent uploads", effective_workers);
        info!("[CliS3Uploader] Bucket: {}, Region: {}", bucket, region);
        if let Some(ref ep) = endpoint_url {
            info!("[CliS3Uploader] Endpoint: {}", ep);
        }

        Self {
            bucket,
            region,
            endpoint_url,
            access_key,
            secret_key,
            active_uploads: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(effective_workers)),
            max_retries: 3,
        }
    }

    /// 세그먼트를 업로드 (MemoryS3Uploader 호환 인터페이스)
    pub async fn queue_segment(&self, segment: CliSegment) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = format!("{}/{}", segment.stream_name, segment.file_name);
        let file_size_kb = segment.data.len() / 1024;

        debug!("[CLI S3 Queue] {} ({}KB) priority={}", segment.file_name, file_size_kb, segment.priority);

        let bucket = self.bucket.clone();
        let region = self.region.clone();
        let endpoint_url = self.endpoint_url.clone();
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let semaphore = Arc::clone(&self.semaphore);
        let active_uploads = Arc::clone(&self.active_uploads);
        let max_retries = self.max_retries;

        tokio::spawn(async move {
            // 세마포어 획득
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    error!("[CLI S3 Error] Failed to acquire semaphore for {}: {}", key, e);
                    return;
                }
            };

            // 업로드 상태 기록
            {
                let mut uploads = active_uploads.write().await;
                uploads.insert(key.clone(), UploadState::Uploading);
            }

            let upload_start = std::time::Instant::now();
            let mut last_error = String::new();

            // 재시도 루프
            for attempt in 0..=max_retries {
                match upload_with_cli(&bucket, &region, &endpoint_url, &access_key, &secret_key, &key, &segment.data, &segment.content_type).await {
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

                        info!("[CLI S3 Done] {} ({}KB) in {:?} ({:.2} MB/s)",
                            key, file_size_kb, elapsed, speed_mbps);
                        return;
                    }
                    Err(e) => {
                        last_error = e;

                        if attempt < max_retries {
                            let backoff_ms = 200 * (attempt + 1) as u64;
                            warn!("[CLI S3 Retry] {} (attempt {}/{}): {}",
                                key, attempt + 1, max_retries + 1, &last_error);
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        }
                    }
                }
            }

            // 모든 재시도 실패
            {
                let mut uploads = active_uploads.write().await;
                uploads.insert(key.clone(), UploadState::Failed(last_error.clone()));
            }

            error!("[CLI S3 Failed] {} after {} attempts: {}", key, max_retries + 1, last_error);
        });

        Ok(())
    }

    /// 데이터를 직접 업로드 (queue_segment의 편의 메서드)
    pub async fn queue_data(
        &self,
        stream_name: &str,
        file_name: &str,
        data: Vec<u8>,
        content_type: &str,
        priority: u8,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let segment = CliSegment {
            stream_name: stream_name.to_string(),
            file_name: file_name.to_string(),
            data,
            content_type: content_type.to_string(),
            priority,
            created_at: Utc::now(),
        };
        self.queue_segment(segment).await
    }

    pub async fn pending_count(&self) -> usize {
        let uploads = self.active_uploads.read().await;
        uploads.values().filter(|s| matches!(s, UploadState::Uploading)).count()
    }

    pub async fn active_count(&self) -> usize {
        self.pending_count().await
    }
}

/// AWS CLI를 사용하여 S3에 업로드
async fn upload_with_cli(
    bucket: &str,
    region: &str,
    endpoint_url: &Option<String>,
    access_key: &str,
    secret_key: &str,
    key: &str,
    data: &[u8],
    content_type: &str,
) -> Result<(), String> {
    let s3_path = format!("s3://{}/{}", bucket, key);

    // 임시 파일에 데이터 쓰기
    let temp_path = format!("/tmp/s3_upload_{}", uuid_simple());
    tokio::fs::write(&temp_path, data).await
        .map_err(|e| format!("Failed to write temp file: {}", e))?;

    // 클로저로 이동할 값들
    let region_owned = region.to_string();
    let content_type_owned = content_type.to_string();
    let endpoint_url_owned = endpoint_url.clone();
    let access_key_owned = access_key.to_string();
    let secret_key_owned = secret_key.to_string();
    let key_owned = key.to_string();
    let temp_path_clone = temp_path.clone();

    // AWS CLI 실행
    let result = tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new("aws");

        // config.toml의 자격증명을 환경변수로 전달
        cmd.env("AWS_ACCESS_KEY_ID", &access_key_owned);
        cmd.env("AWS_SECRET_ACCESS_KEY", &secret_key_owned);
        cmd.env("AWS_DEFAULT_REGION", &region_owned);

        cmd.args(["s3", "cp", &temp_path_clone, &s3_path]);
        cmd.args(["--region", &region_owned]);
        cmd.args(["--content-type", &content_type_owned]);

        // 캐시 컨트롤 설정
        let cache_control = if key_owned.ends_with(".m3u8") {
            "no-cache, no-store, must-revalidate"
        } else if key_owned.ends_with("init.mp4") {
            "public, max-age=86400"
        } else {
            "public, max-age=3600"
        };
        cmd.args(["--cache-control", cache_control]);

        if let Some(endpoint) = endpoint_url_owned {
            if !endpoint.is_empty() {
                cmd.args(["--endpoint-url", &endpoint]);
            }
        }

        let output = cmd.output();

        // 임시 파일 삭제
        let _ = std::fs::remove_file(&temp_path_clone);

        output
    }).await.map_err(|e| format!("Task join error: {}", e))?;

    match result {
        Ok(output) => {
            if output.status.success() {
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                Err(format!("AWS CLI failed: {}", stderr))
            }
        }
        Err(e) => Err(format!("Failed to execute AWS CLI: {}", e))
    }
}

/// 간단한 UUID 생성
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}_{}", duration.as_secs(), duration.subsec_nanos())
}
