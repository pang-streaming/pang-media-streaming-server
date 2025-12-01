use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, mpsc};
use log::{info, warn, error, debug};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use chrono::Utc;
use futures::future::join_all;

/// 메모리 세그먼트 데이터
#[derive(Clone)]
pub struct MemorySegment {
    pub stream_name: String,
    pub file_name: String,
    pub data: Vec<u8>,
    pub content_type: String,
    pub priority: u8, // 0 = lowest, 255 = highest
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub retry_count: u8,
}

/// 업로드 작업 상태
#[derive(Debug, Clone)]
pub enum UploadState {
    Pending,
    Uploading,
    Completed,
    Failed(String),
}

/// 업로드 작업
pub struct UploadTask {
    pub segment: MemorySegment,
    pub state: UploadState,
    pub attempt: u8,
}

/// 메모리 기반 S3 업로더
pub struct MemoryS3Uploader {
    client: Arc<Client>,
    bucket: String,
    upload_queue: Arc<RwLock<VecDeque<MemorySegment>>>,
    active_uploads: Arc<RwLock<HashMap<String, UploadState>>>,
    worker_semaphore: Arc<Semaphore>,
    max_retries: u8,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<RwLock<Option<mpsc::Receiver<()>>>>,
}

impl MemoryS3Uploader {
    /// 새로운 메모리 S3 업로더 생성
    pub async fn new(
        client: Arc<Client>,
        bucket: String,
        worker_count: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // worker_count를 최소 8개로 설정 (병렬 업로드 성능 향상)
        let effective_workers = worker_count.max(8);

        info!("[S3 Uploader] Initialized with {} workers", effective_workers);

        Ok(Self {
            client,
            bucket,
            upload_queue: Arc::new(RwLock::new(VecDeque::new())),
            active_uploads: Arc::new(RwLock::new(HashMap::new())),
            worker_semaphore: Arc::new(Semaphore::new(effective_workers)),
            max_retries: 5,  // 연결 끊김 대응 - 앱 레벨 5회 재시도
            shutdown_tx,
            shutdown_rx: Arc::new(RwLock::new(Some(shutdown_rx))),
        })
    }

    /// 세그먼트를 업로드 큐에 추가 (메모리에서 직접)
    pub async fn queue_segment(&self, segment: MemorySegment) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let queue_len = {
            let mut queue = self.upload_queue.write().await;

            // 우선순위에 따라 큐에 삽입
            let insert_pos = queue.iter().position(|s| s.priority < segment.priority).unwrap_or(queue.len());

            debug!("[S3 Queue] {} ({}KB, priority={}, queue_pos={}/{})",
                segment.file_name,
                segment.data.len() / 1024,
                segment.priority,
                insert_pos,
                queue.len()
            );

            queue.insert(insert_pos, segment);
            queue.len()
        };

        // 큐에 있는 모든 세그먼트에 대해 워커 생성 시도
        for _ in 0..queue_len {
            self.spawn_upload_worker().await;
        }

        Ok(())
    }

    /// 업로드 워커 생성
    async fn spawn_upload_worker(&self) {
        let queue = Arc::clone(&self.upload_queue);
        let client = Arc::clone(&self.client);
        let bucket = self.bucket.clone();
        let semaphore = Arc::clone(&self.worker_semaphore);
        let active_uploads = Arc::clone(&self.active_uploads);
        let max_retries = self.max_retries;

        tokio::spawn(async move {
            // 세마포어 획득 시도 (논블로킹)
            let permit = match semaphore.try_acquire() {
                Ok(p) => p,
                Err(_) => return, // 슬롯 없으면 종료 (다른 워커가 처리)
            };

            // 큐에서 세그먼트 가져오기
            let segment = {
                let mut q = queue.write().await;
                match q.pop_front() {
                    Some(s) => s,
                    None => return, // 큐가 비어있으면 종료
                }
            };

            let key = format!("{}/{}", segment.stream_name, segment.file_name);
            let file_size_kb = segment.data.len() / 1024;

            // 업로드 상태 업데이트
            {
                let mut uploads = active_uploads.write().await;
                uploads.insert(key.clone(), UploadState::Uploading);
            }

            info!("[S3 Upload] {} ({}KB)", key, file_size_kb);
            let upload_start = std::time::Instant::now();

            // 재시도 로직
            let mut retry_count = 0;
            let mut last_error = None;

            while retry_count <= max_retries {
                match Self::upload_to_s3(&client, &bucket, &key, &segment).await {
                    Ok(_) => {
                        // 성공
                        let elapsed = upload_start.elapsed();
                        {
                            let mut uploads = active_uploads.write().await;
                            uploads.insert(key.clone(), UploadState::Completed);
                        }

                        // 업로드 속도 계산 (MB/s)
                        let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
                            (file_size_kb as f64 / 1024.0) / elapsed.as_secs_f64()
                        } else {
                            0.0
                        };

                        info!("[S3 Done] {} ({}KB) in {:?} ({:.2} MB/s)",
                            key, file_size_kb, elapsed, speed_mbps);

                        drop(permit);
                        return;
                    }
                    Err(e) => {
                        retry_count += 1;
                        let error_str = format!("{:?}", e);
                        last_error = Some(error_str.clone());

                        if retry_count <= max_retries {
                            // 에러 유형별 백오프 설정
                            let (error_type, backoff_ms) = if error_str.contains("SlowDown") {
                                ("SlowDown", 2000 * retry_count as u64)
                            } else if error_str.contains("connection") || error_str.contains("Connection")
                                   || error_str.contains("re-used") || error_str.contains("dispatch") {
                                ("Connection", 500 * retry_count as u64)
                            } else if error_str.contains("timeout") || error_str.contains("Timeout") {
                                ("Timeout", 1000 * retry_count as u64)
                            } else {
                                ("Other", 300 * retry_count as u64)
                            };

                            warn!("[S3 Retry] {} ({}, attempt {}/{}, wait {}ms)",
                                key, error_type, retry_count + 1, max_retries + 1, backoff_ms);

                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        }
                    }
                }
            }

            // 최종 실패
            let elapsed = upload_start.elapsed();
            {
                let mut uploads = active_uploads.write().await;
                uploads.insert(key.clone(), UploadState::Failed(last_error.clone().unwrap_or_default()));
            }

            error!("[S3 Failed] {} after {} attempts in {:?}: {}",
                key, max_retries + 1, elapsed,
                last_error.unwrap_or_default().chars().take(150).collect::<String>());

            drop(permit);
        });
    }

    /// S3에 실제 업로드 (최적화됨)
    async fn upload_to_s3(
        client: &Client,
        bucket: &str,
        key: &str,
        segment: &MemorySegment,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let content_length = segment.data.len() as i64;
        let body = ByteStream::from(segment.data.clone());

        // 파일 타입별 캐시 설정
        let cache_control = if key.ends_with(".m3u8") {
            "no-cache, no-store, must-revalidate"
        } else if key.ends_with("init.mp4") {
            "public, max-age=86400"  // init 세그먼트는 24시간 캐시
        } else {
            "public, max-age=3600"   // 일반 세그먼트는 1시간 캐시
        };

        let mut put_request = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .content_length(content_length)  // 명시적 content-length (청크 업로드 방지)
            .content_type(&segment.content_type)
            .cache_control(cache_control);

        // 플레이리스트 메타데이터
        if key.ends_with(".m3u8") {
            put_request = put_request
                .metadata("x-amz-meta-last-modified", &Utc::now().to_rfc3339())
                .metadata("x-amz-meta-type", "playlist");
        }

        put_request.send().await?;
        Ok(())
    }

    /// 병렬 배치 업로드
    pub async fn batch_upload(&self, segments: Vec<MemorySegment>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let upload_futures: Vec<_> = segments
            .into_iter()
            .map(|segment| self.queue_segment(segment))
            .collect();

        join_all(upload_futures).await;
        Ok(())
    }

    /// 해당 스트림의 모든 세그먼트를 메모리에서 업로드
    pub async fn upload_stream_from_memory(
        &self,
        stream_name: &str,
        segments: HashMap<String, Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut upload_segments = Vec::new();

        for (file_name, data) in segments {
            let content_type = if file_name.ends_with(".m3u8") {
                "application/vnd.apple.mpegurl"
            } else if file_name.ends_with(".ts") {
                "video/mp2t"
            } else if file_name.ends_with(".m4s") {
                "video/iso.segment"
            } else if file_name.ends_with(".mp4") {
                "video/mp4"
            } else {
                "application/octet-stream"
            };

            // 각 파일별 우선 순위를 지정합니다.
            let priority = if file_name.ends_with(".m3u8") {
                255 // 플레이리스트
            } else if file_name.contains("init") {
                200 // init.mp4
            } else {
                100 // 일반 세그먼트
            };

            upload_segments.push(MemorySegment {
                stream_name: stream_name.to_string(),
                file_name: file_name.clone(),
                data,
                content_type: content_type.to_string(),
                priority,
                created_at: Utc::now(),
                retry_count: 0,
            });
        }

        self.batch_upload(upload_segments).await
    }

    /// 업로드 상태 조회
    pub async fn get_upload_status(&self, key: &str) -> Option<UploadState> {
        let uploads = self.active_uploads.read().await;
        uploads.get(key).cloned()
    }

    /// 대기 중인 업로드 수 조회
    pub async fn pending_count(&self) -> usize {
        let queue = self.upload_queue.read().await;
        queue.len()
    }

    /// 활성 업로드 수 조회
    pub async fn active_count(&self) -> usize {
        let uploads = self.active_uploads.read().await;
        uploads.values().filter(|s| matches!(s, UploadState::Uploading)).count()
    }

    /// 업로더 종료
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(()).await;

        // 모든 활성 업로드가 완료될 때까지 대기
        while self.active_count().await > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}