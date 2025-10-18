use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::config::S3Config;

use super::s3_types::{UploadTask, UploadStatus};
use super::s3_client::S3ClientWrapper;
use super::s3_upload_worker::S3UploadWorker;

pub struct S3Storage {
    client: Arc<S3ClientWrapper>,
    upload_status: Arc<RwLock<HashMap<String, UploadStatus>>>,
    pub upload_sender: Option<mpsc::UnboundedSender<UploadTask>>,
}

impl S3Storage {
    pub async fn new(config: &S3Config) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // S3 클라이언트 생성
        let client = Arc::new(S3ClientWrapper::new(config).await?);
        
        // 백그라운드 업로드 워커를 위한 채널 생성
        let (upload_sender, upload_receiver) = mpsc::unbounded_channel::<UploadTask>();
        let upload_status = Arc::new(RwLock::new(HashMap::new()));

        // 비동기 업로드 워커 시작
        let worker = S3UploadWorker::new(client.clone(), upload_status.clone());
        tokio::spawn(async move {
            println!("🚀 S3 Async Upload Worker started");
            worker.start_async_worker(upload_receiver).await;
            println!("🛑 S3 Async Upload Worker stopped");
        });

        Ok(Self {
            client,
            upload_status,
            upload_sender: Some(upload_sender),
        })
    }


    /// 스트리밍 업로드 - 디스크 I/O 최소화
    pub async fn upload_file_streaming(
        &self,
        stream_key: &str,
        file_path: &Path,
        s3_key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(sender) = &self.upload_sender {
            let content_type = self.get_content_type(file_path);
            
            let task = UploadTask {
                stream_key: stream_key.to_string(),
                file_path: file_path.to_string_lossy().to_string(),
                s3_key: s3_key.to_string(),
                content_type: content_type.to_string(),
            };

            // 업로드 상태 초기화
            {
                let mut status_map = self.upload_status.write().await;
                if !status_map.contains_key(stream_key) {
                    status_map.insert(stream_key.to_string(), UploadStatus {
                        stream_key: stream_key.to_string(),
                        total_files: 0,
                        uploaded_files: 0,
                        failed_files: 0,
                        is_complete: false,
                    });
                }
            }

            // 백그라운드 업로드 큐에 추가
            sender.send(task)?;
            Ok(())
        } else {
            Err("Upload sender not initialized".into())
        }
    }

    /// 디렉토리의 모든 파일을 스트리밍 업로드
    pub async fn upload_directory_streaming(
        &self,
        local_dir: &Path,
        stream_key: &str,
        s3_prefix: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !local_dir.exists() {
            return Ok(());
        }

        let mut file_count = 0;
        let mut entries = fs::read_dir(local_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            
            if path.is_file() {
                let file_name = path.file_name()
                    .and_then(|name| name.to_str())
                    .ok_or("Invalid file name")?;
                
                let s3_key = if s3_prefix.is_empty() {
                    file_name.to_string()
                } else {
                    format!("{}/{}", s3_prefix, file_name)
                };

                self.upload_file_streaming(stream_key, &path, &s3_key).await?;
                file_count += 1;
            }
        }

        // 총 파일 수 업데이트
        {
            let mut status_map = self.upload_status.write().await;
            if let Some(status) = status_map.get_mut(stream_key) {
                status.total_files = file_count;
            }
        }

        println!("📤 Queued {} files for streaming upload to S3", file_count);
        Ok(())
    }

    /// 업로드 상태 조회
    pub async fn get_upload_status(&self, stream_key: &str) -> Option<UploadStatus> {
        let status_map = self.upload_status.read().await;
        status_map.get(stream_key).cloned()
    }


    /// 파일 확장자에 따른 Content-Type 결정
    fn get_content_type(&self, path: &Path) -> &'static str {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("m3u8") => "application/vnd.apple.mpegurl",
            Some("m4s") => "video/mp4",
            Some("mp4") => "video/mp4",
            Some("ts") => "video/mp2t",
            Some("json") => "application/json",
            _ => "application/octet-stream",
        }
    }

    /// S3에서 파일 삭제
    pub async fn delete_file(
        &self,
        s3_key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client.delete_file(s3_key).await?;
        println!("🗑️ Deleted from S3: {}", s3_key);
        Ok(())
    }

    /// S3에서 디렉토리의 모든 파일 삭제
    pub async fn delete_directory(
        &self,
        s3_prefix: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client.delete_directory(s3_prefix).await?;
        Ok(())
    }
}
