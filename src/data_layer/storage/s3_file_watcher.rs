use std::path::Path;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;
use notify::{Watcher, RecursiveMode, Event, EventKind};
use super::s3_types::UploadTask;

/// S3 파일 감시기
pub struct S3FileWatcher {
    upload_sender: mpsc::UnboundedSender<UploadTask>,
    stream_key: String,
    s3_prefix: String,
}

impl S3FileWatcher {
    /// 새로운 파일 감시기 생성
    pub fn new(
        upload_sender: mpsc::UnboundedSender<UploadTask>,
        stream_key: String,
        s3_prefix: String,
    ) -> Self {
        Self {
            upload_sender,
            stream_key,
            s3_prefix,
        }
    }

    /// 파일 감시 시작
    pub async fn start_watching(&self, watch_dir: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !watch_dir.exists() {
            return Err("Watch directory does not exist".into());
        }

        // 파일 감시기 생성
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let upload_sender = self.upload_sender.clone();
        let stream_key = self.stream_key.clone();
        let s3_prefix = self.s3_prefix.clone();
        
        let mut watcher = match notify::recommended_watcher(move |res| {
            match res {
                Ok(event) => {
                    if let Err(_) = tx.try_send(event) {
                        // 채널이 가득 찬 경우 무시
                    }
                }
                Err(e) => eprintln!("❌ Watch error: {:?}", e),
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                return Err(format!("Failed to create file watcher: {}", e).into());
            }
        };

        // 디렉토리 감시 시작
        if let Err(e) = watcher.watch(watch_dir, RecursiveMode::NonRecursive) {
            return Err(format!("Failed to watch directory: {}", e).into());
        }

        println!("👀 Started realtime file watching for stream '{}'", stream_key);

        // 파일 변경 이벤트 처리 (중복 제거 및 배치 처리)
        let mut pending_files = HashSet::new();
        let mut last_process_time = Instant::now();
        let batch_delay = Duration::from_millis(100); // 100ms 배치 지연

        while let Some(event) = rx.recv().await {
            if let Event { kind: EventKind::Create(_) | EventKind::Modify(_), paths, .. } = event {
                for path in paths {
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                        // HLS 세그먼트 파일인지 확인
                        if file_name.ends_with(".ts") || file_name.ends_with(".m3u8") || file_name.ends_with(".m4s") || file_name.ends_with(".mp4") {
                            pending_files.insert(path.clone());
                        }
                    }
                }
            }

            // 배치 처리: 100ms마다 또는 파일이 쌓이면 처리
            if last_process_time.elapsed() >= batch_delay || pending_files.len() >= 5 {
                Self::process_pending_files(&pending_files, &upload_sender, &stream_key, &s3_prefix).await;
                pending_files.clear();
                last_process_time = Instant::now();
            }
        }

        Ok(())
    }

    /// 대기 중인 파일들을 배치로 처리
    async fn process_pending_files(
        pending_files: &HashSet<std::path::PathBuf>,
        upload_sender: &mpsc::UnboundedSender<UploadTask>,
        stream_key: &str,
        s3_prefix: &str,
    ) {
        if pending_files.is_empty() {
            return;
        }

        // m3u8 파일을 우선순위로 정렬
        let mut sorted_files: Vec<_> = pending_files.iter().collect();
        sorted_files.sort_by(|a, b| {
            let a_is_playlist = a.file_name().and_then(|n| n.to_str()).map_or(false, |n| n.ends_with(".m3u8"));
            let b_is_playlist = b.file_name().and_then(|n| n.to_str()).map_or(false, |n| n.ends_with(".m3u8"));
            b_is_playlist.cmp(&a_is_playlist) // m3u8 파일이 먼저
        });

        for path in sorted_files {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                println!("📁 Processing file: {}", file_name);
                
                let s3_key = format!("{}/{}", s3_prefix, file_name);
                let content_type = Self::get_content_type(path);
                
                let task = UploadTask {
                    stream_key: stream_key.to_string(),
                    file_path: path.to_string_lossy().to_string(),
                    s3_key,
                    content_type: content_type.to_string(),
                };

                // 우선순위에 따라 즉시 또는 지연 전송
                if file_name.ends_with(".m3u8") {
                    // m3u8 파일은 즉시 전송
                    if let Err(e) = upload_sender.send(task) {
                        eprintln!("❌ Failed to queue playlist for S3 upload: {}", e);
                    } else {
                        println!("📤 Queued playlist '{}' for immediate S3 upload", file_name);
                    }
                } else {
                    // 세그먼트 파일은 배치 전송
                    if let Err(e) = upload_sender.send(task) {
                        eprintln!("❌ Failed to queue segment for S3 upload: {}", e);
                    } else {
                        println!("📤 Queued segment '{}' for batch S3 upload", file_name);
                    }
                }
            }
        }
    }

    /// 파일 확장자에 따른 Content-Type 결정
    fn get_content_type(path: &Path) -> &'static str {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("m3u8") => "application/vnd.apple.mpegurl",
            Some("m4s") => "video/mp4",
            Some("mp4") => "video/mp4",
            Some("ts") => "video/mp2t",
            Some("json") => "application/json",
            _ => "application/octet-stream",
        }
    }
}
