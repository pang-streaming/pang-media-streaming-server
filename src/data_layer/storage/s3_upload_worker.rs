use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use std::collections::HashMap;
use super::s3_client::S3ClientWrapper;
use super::s3_types::{UploadTask, UploadStatus};

/// S3 업로드 워커
pub struct S3UploadWorker {
    client: Arc<S3ClientWrapper>,
    upload_status: Arc<RwLock<HashMap<String, UploadStatus>>>,
}

impl S3UploadWorker {
    /// 새로운 업로드 워커 생성
    pub fn new(client: Arc<S3ClientWrapper>, upload_status: Arc<RwLock<HashMap<String, UploadStatus>>>) -> Self {
        Self {
            client,
            upload_status,
        }
    }

    /// 업로드 워커 시작 (순차 처리)
    pub async fn start_worker(&self, mut upload_receiver: mpsc::UnboundedReceiver<UploadTask>) {
        println!("🚀 S3 Upload Worker started");
        while let Some(task) = upload_receiver.recv().await {
            println!("📤 Processing upload task: {} -> {}", task.file_path, task.s3_key);
            if let Err(e) = self.process_upload_task(&task).await {
                eprintln!("❌ Upload worker error: {}", e);
            }
        }
        println!("🛑 S3 Upload Worker stopped");
    }

    /// 비동기 업로드 워커 시작 (동시 처리)
    pub async fn start_async_worker(&self, mut upload_receiver: mpsc::UnboundedReceiver<UploadTask>) {
        println!("🚀 S3 Async Upload Worker started");
        
        // 동시에 처리할 최대 업로드 수
        let max_concurrent_uploads = 10;
        let mut upload_futures = std::collections::VecDeque::new();
        
        loop {
            // 새로운 업로드 태스크 수신
            if let Some(task) = upload_receiver.recv().await {
                println!("📤 Queued async upload: {} -> {}", task.file_path, task.s3_key);
                
                // 비동기 업로드 태스크 생성
                let client = self.client.clone();
                let upload_status = self.upload_status.clone();
                let task_clone = task.clone();
                
                let upload_future = tokio::spawn(async move {
                    Self::process_upload_task_async(&client, &upload_status, &task_clone).await
                });
                
                upload_futures.push_back(upload_future);
                
                // 최대 동시 업로드 수에 도달하면 완료된 것부터 처리
                if upload_futures.len() >= max_concurrent_uploads {
                    if let Some(future) = upload_futures.pop_front() {
                        if let Err(e) = future.await {
                            eprintln!("❌ Async upload task failed: {}", e);
                        }
                    }
                }
            } else {
                // 채널이 닫혔으면 남은 업로드들 완료 대기
                break;
            }
        }
        
        // 남은 모든 업로드 완료 대기
        while let Some(future) = upload_futures.pop_front() {
            if let Err(e) = future.await {
                eprintln!("❌ Async upload task failed: {}", e);
            }
        }
        
        println!("🛑 S3 Async Upload Worker stopped");
    }

    /// 업로드 작업 처리
    async fn process_upload_task(&self, task: &UploadTask) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.client.upload_file(task).await {
            Ok(result) => {
                if result.success {
                    // 업로드 성공
                    let mut status_map = self.upload_status.write().await;
                    if let Some(status) = status_map.get_mut(&task.stream_key) {
                        status.uploaded_files += 1;
                        status.is_complete = status.uploaded_files + status.failed_files >= status.total_files;
                    }
                    println!("✅ Uploaded to S3: {}", task.s3_key);
                } else {
                    // 업로드 실패
                    let mut status_map = self.upload_status.write().await;
                    if let Some(status) = status_map.get_mut(&task.stream_key) {
                        status.failed_files += 1;
                        status.is_complete = status.uploaded_files + status.failed_files >= status.total_files;
                    }
                    eprintln!("❌ Upload failed: {} - {}", task.s3_key, result.error_message.unwrap_or_default());
                }
            }
            Err(e) => {
                // 워커 오류
                let mut status_map = self.upload_status.write().await;
                if let Some(status) = status_map.get_mut(&task.stream_key) {
                    status.failed_files += 1;
                    status.is_complete = status.uploaded_files + status.failed_files >= status.total_files;
                }
                eprintln!("❌ Upload worker error: {}", e);
            }
        }
        Ok(())
    }

    /// 비동기 업로드 작업 처리
    async fn process_upload_task_async(
        client: &Arc<S3ClientWrapper>,
        upload_status: &Arc<RwLock<HashMap<String, UploadStatus>>>,
        task: &UploadTask,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match client.upload_file(task).await {
            Ok(result) => {
                if result.success {
                    // 업로드 성공
                    let mut status_map = upload_status.write().await;
                    if let Some(status) = status_map.get_mut(&task.stream_key) {
                        status.uploaded_files += 1;
                        status.is_complete = status.uploaded_files + status.failed_files >= status.total_files;
                    }
                    println!("✅ Async uploaded to S3: {}", task.s3_key);
                } else {
                    // 업로드 실패
                    let mut status_map = upload_status.write().await;
                    if let Some(status) = status_map.get_mut(&task.stream_key) {
                        status.failed_files += 1;
                        status.is_complete = status.uploaded_files + status.failed_files >= status.total_files;
                    }
                    eprintln!("❌ Async upload failed: {} - {}", task.s3_key, result.error_message.unwrap_or_default());
                }
            }
            Err(e) => {
                // 워커 오류
                let mut status_map = upload_status.write().await;
                if let Some(status) = status_map.get_mut(&task.stream_key) {
                    status.failed_files += 1;
                    status.is_complete = status.uploaded_files + status.failed_files >= status.total_files;
                }
                eprintln!("❌ Async upload worker error: {}", e);
            }
        }
        Ok(())
    }
}
