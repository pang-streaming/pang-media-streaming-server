use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use log::{info, warn, error, debug};

/// 업로드 상태
#[derive(Debug, Clone)]
pub enum UploadState {
    Uploading,
    Completed,
    Failed(String),
}

/// AWS CLI 기반 S3 업로더
pub struct CliS3Uploader {
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    active_uploads: Arc<RwLock<HashMap<String, UploadState>>>,
    semaphore: Arc<Semaphore>,
    max_retries: u8,
}

impl CliS3Uploader {
    pub fn new(
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        worker_count: usize,
    ) -> Self {
        let effective_workers = worker_count.max(32);
        info!("[CliS3Uploader] Initialized with {} concurrent uploads", effective_workers);

        Self {
            bucket,
            region,
            endpoint_url,
            active_uploads: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(effective_workers)),
            max_retries: 3,
        }
    }

    /// 데이터를 S3에 업로드 (AWS CLI 사용)
    /// priority: 높을수록 우선 (100=최우선, 50=낮음)
    pub async fn queue_data(
        &self,
        stream_name: &str,
        file_name: &str,
        data: Vec<u8>,
        content_type: &str,
        priority: u8,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = format!("{}/{}", stream_name, file_name);
        let file_size_kb = data.len() / 1024;

        debug!("[CLI S3 Queue] {} ({}KB) priority={}", file_name, file_size_kb, priority);

        let bucket = self.bucket.clone();
        let region = self.region.clone();
        let endpoint_url = self.endpoint_url.clone();
        let semaphore = Arc::clone(&self.semaphore);
        let active_uploads = Arc::clone(&self.active_uploads);
        let max_retries = self.max_retries;
        let content_type = content_type.to_string();

        tokio::spawn(async move {
            // 우선순위 기반 지연: 낮은 우선순위 = 더 긴 대기
            // priority 100 = 0ms, priority 50 = 50ms
            let delay_ms = ((100 - priority.min(100)) as u64) * 1;
            if delay_ms > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }

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
                match upload_with_cli(&bucket, &region, &endpoint_url, &key, &data, &content_type).await {
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

    /// 디렉토리 전체를 S3에 동기화
    pub async fn sync_directory(
        &self,
        local_dir: &str,
        s3_prefix: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let s3_path = format!("s3://{}/{}", self.bucket, s3_prefix);

        let mut cmd = Command::new("aws");
        cmd.args(["s3", "sync", local_dir, &s3_path]);
        cmd.args(["--region", &self.region]);

        if let Some(ref endpoint) = self.endpoint_url {
            cmd.args(["--endpoint-url", endpoint]);
        }

        let output = cmd.output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("S3 sync failed: {}", stderr).into());
        }

        info!("[CLI S3] Synced {} to {}", local_dir, s3_path);
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

/// AWS CLI를 사용하여 S3에 업로드
async fn upload_with_cli(
    bucket: &str,
    region: &str,
    endpoint_url: &Option<String>,
    key: &str,
    data: &[u8],
    content_type: &str,
) -> Result<(), String> {
    let s3_path = format!("s3://{}/{}", bucket, key);

    // 임시 파일에 데이터 쓰기
    let temp_path = format!("/tmp/s3_upload_{}", uuid_simple());
    tokio::fs::write(&temp_path, data).await
        .map_err(|e| format!("Failed to write temp file: {}", e))?;

    // 클로저로 이동할 값들을 소유권 있는 String으로 변환
    let region_owned = region.to_string();
    let content_type_owned = content_type.to_string();
    let endpoint_url_owned = endpoint_url.clone();
    let key_owned = key.to_string();
    let temp_path_clone = temp_path.clone();

    // AWS CLI 실행
    let result = tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new("aws");
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
            cmd.args(["--endpoint-url", &endpoint]);
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
