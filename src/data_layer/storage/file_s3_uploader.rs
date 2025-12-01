use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc::{self, Sender}};
use log::{info, warn, error, debug};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use chrono::Utc;
use bytes::Bytes;

/// 업로드 상태
#[derive(Debug, Clone)]
pub enum UploadState {
    Uploading,
    Completed,
    Failed(String),
}

/// 업로드 작업
struct UploadJob {
    key: String,
    data: Bytes,
    content_type: String,
    file_size_kb: usize,
}

/// AWS SDK 기반 S3 업로더 (워커 풀 최적화)
pub struct FileS3Uploader {
    active_uploads: Arc<RwLock<HashMap<String, UploadState>>>,
    job_sender: Sender<UploadJob>,
}

impl FileS3Uploader {
    pub async fn new(
        client: Arc<Client>,
        bucket: String,
        worker_count: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let effective_workers = worker_count.max(16);
        info!("[FileS3Uploader] Initializing with {} upload workers", effective_workers);

        let (job_sender, job_receiver) = mpsc::channel::<UploadJob>(effective_workers * 2);
        let receiver = Arc::new(tokio::sync::Mutex::new(job_receiver));
        let active_uploads = Arc::new(RwLock::new(HashMap::new()));
        let max_retries = 3;

        for worker_id in 0..effective_workers {
            let worker_receiver = receiver.clone();
            let worker_client = client.clone();
            let worker_bucket = bucket.clone();
            let worker_active_uploads = active_uploads.clone();

            tokio::spawn(async move {
                loop {
                    let job = {
                        let mut locked_receiver = worker_receiver.lock().await;
                        locked_receiver.recv().await
                    };

                    if let Some(job) = job {
                        let upload_start = std::time::Instant::now();
                        let key = job.key;
                        let file_size_kb = job.file_size_kb;
                        let mut last_error = String::new();

                        for attempt in 0..=max_retries {
                            match upload_to_s3(&worker_client, &worker_bucket, &key, job.data.clone(), &job.content_type).await {
                                Ok(_) => {
                                    let elapsed = upload_start.elapsed();
                                    {
                                        let mut uploads = worker_active_uploads.write().await;
                                        uploads.insert(key.clone(), UploadState::Completed);
                                    }

                                    let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
                                        (file_size_kb as f64 / 1024.0) / elapsed.as_secs_f64()
                                    } else {
                                        0.0
                                    };

                                    info!("[S3 Done] {} ({}KB) in {:?} ({:.2} MB/s)",
                                        key, file_size_kb, elapsed, speed_mbps);
                                    
                                    last_error.clear();
                                    break;
                                }
                                Err(e) => {
                                    last_error = format!("{:?}", e);
                                    if attempt < max_retries {
                                        let backoff_ms = calculate_backoff(&last_error, attempt + 1);
                                        warn!("[S3 Retry] {} (attempt {}/{}, wait {}ms): {}",
                                            key, attempt + 1, max_retries + 1, backoff_ms,
                                            &last_error[..last_error.len().min(100)]);
                                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                                    }
                                }
                            }
                        }

                        if !last_error.is_empty() {
                            {
                                let mut uploads = worker_active_uploads.write().await;
                                uploads.insert(key.clone(), UploadState::Failed(last_error.clone()));
                            }
                            error!("[S3 Failed] {} after {} attempts: {}",
                                key, max_retries + 1, &last_error[..last_error.len().min(150)]);
                        }
                    } else {
                        info!("[UploadWorker-{}] Channel closed. Shutting down.", worker_id);
                        break;
                    }
                }
            });
        }

        Ok(Self {
            active_uploads,
            job_sender,
        })
    }

    /// 데이터를 S3 업로드 큐에 추가 (비동기)
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
        let bytes_data = Bytes::from(data);

        debug!("[S3 Queue] {} ({}KB)", file_name, file_size_kb);
        
        let job = UploadJob {
            key,
            data: bytes_data,
            content_type: content_type.to_string(),
            file_size_kb,
        };

        {
            let mut uploads = self.active_uploads.write().await;
            uploads.insert(job.key.clone(), UploadState::Uploading);
        }
        
        if let Err(e) = self.job_sender.send(job).await {
            error!("[S3 Queue] Failed to send job to upload worker: {}", e);
            let key = e.0.key;
            let mut uploads = self.active_uploads.write().await;
            uploads.insert(key, UploadState::Failed("Failed to queue".to_string()));
        }

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

/// S3에 데이터 업로드
async fn upload_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Bytes,
    content_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let content_length = data.len() as i64;
    let body = ByteStream::from(data);

    let cache_control = if key.ends_with(".m3u8") {
        "no-cache, no-store, must-revalidate"
    } else if key.ends_with("init.mp4") {
        "public, max-age=86400"
    } else {
        "public, max-age=3600"
    };

    let mut put_request = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .content_length(content_length)
        .content_type(content_type)
        .cache_control(cache_control);

    if key.ends_with(".m3u8") {
        put_request = put_request
            .metadata("x-amz-meta-last-modified", &Utc::now().to_rfc3339())
            .metadata("x-amz-meta-type", "playlist");
    }

    put_request.send().await?;
    Ok(())
}

/// 에러 유형별 백오프 계산
fn calculate_backoff(error_str: &str, retry_count: u8) -> u64 {
    let base = if error_str.contains("SlowDown") {
        2000
    } else if error_str.contains("connection") || error_str.contains("Connection")
           || error_str.contains("re-used") || error_str.contains("dispatch") {
        500
    } else if error_str.contains("timeout") || error_str.contains("Timeout") {
        1000
    } else {
        200
    };

    base * retry_count as u64
}
