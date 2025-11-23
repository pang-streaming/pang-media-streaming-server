use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::fs;
use log::{info, error};
use notify::{Watcher, RecursiveMode, Event, EventKind};
use crate::data_layer::storage::memory_buffer_manager::MemoryBufferManager;
use crate::data_layer::storage::memory_s3_uploader::{MemoryS3Uploader, MemorySegment};

/// 파일 변경 감지 및 메모리로 이동
pub async fn start_file_watcher(
    stream_name: String,
    watch_dir: String,
    buffer_manager: Arc<MemoryBufferManager>,
    s3_uploader: Arc<MemoryS3Uploader>,
    s3_bucket: String,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    let handle = tokio::spawn(async move {
        let (tx, mut rx) = mpsc::channel(100);

        // 파일 시스템 watcher 생성
        let mut watcher = match notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create watcher: {}", e);
                return;
            }
        };

        // 디렉토리 감시 시작
        if let Err(e) = watcher.watch(std::path::Path::new(&watch_dir), RecursiveMode::NonRecursive) {
            error!("Failed to watch directory: {}", e);
            return;
        }

        // 이벤트 처리
        while let Some(event) = rx.recv().await {
            if let EventKind::Create(_) | EventKind::Modify(_) = event.kind {
                for path in event.paths {
                    if let Some(file_name) = path.file_name() {
                        let file_name_str = file_name.to_string_lossy().to_string();

                        // 파일을 메모리로 읽기
                        if let Ok(data) = fs::read(&path).await {
                            process_file(
                                &file_name_str,
                                data,
                                &stream_name,
                                &buffer_manager,
                                &s3_uploader,
                                &s3_bucket,
                            ).await;

                            // 파일 삭제 처리
                            // init.mp4는 삭제하지 않음 (계속 사용)
                            // 다른 파일들은 5초 후 삭제 (FFmpeg가 쓰기 완료할 시간 확보)
                            if !file_name_str.contains("init.mp4") {
                                let _ = fs::remove_file(&path).await;
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// 파일 타입별 처리
async fn process_file(
    file_name: &str,
    data: Vec<u8>,
    stream_name: &str,
    buffer_manager: &Arc<MemoryBufferManager>,
    s3_uploader: &Arc<MemoryS3Uploader>,
    s3_bucket: &str,
) {
    // 파일 타입에 따라 처리
    if file_name.ends_with(".m3u8") {
        // 플레이리스트의 URL을 S3 URL로 수정
        let base_url = format!("https://{}.s3.ap-northeast-2.amazonaws.com/{}", s3_bucket, stream_name);
        let modified_data = update_playlist_urls(&data, &base_url);

        // 플레이리스트를 메모리 버퍼에 저장
        let _ = buffer_manager.store_playlist(stream_name, modified_data.clone()).await;

        // S3에 즉시 업로드 (플레이리스트는 최우선)
        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: file_name.to_string(),
            data: modified_data.clone(),
            content_type: "application/vnd.apple.mpegurl".to_string(),
            priority: 255,
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let _ = s3_uploader.queue_segment(segment).await;

    } else if file_name == "init.mp4" {
        // 초기화 세그먼트를 메모리 버퍼에 저장
        let _ = buffer_manager.store_init_segment(stream_name, data.clone()).await;

        // S3에 업로드
        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: file_name.to_string(),
            data,
            content_type: "video/mp4".to_string(),
            priority: 200,
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let _ = s3_uploader.queue_segment(segment).await;

    } else if file_name.ends_with(".m4s") || file_name.ends_with(".ts") {
        // 세그먼트를 메모리 버퍼에 저장
        let _ = buffer_manager.store_segment(stream_name, file_name, data.clone()).await;

        // S3에 업로드
        let content_type = if file_name.ends_with(".m4s") {
            "video/iso.segment"
        } else {
            "video/mp2t"
        };

        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: file_name.to_string(),
            data,
            content_type: content_type.to_string(),
            priority: 100,
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let _ = s3_uploader.queue_segment(segment).await;

    } else if file_name == "thumbnail.jpg" {
        // 썸네일을 S3에 업로드
        info!("Uploading thumbnail to S3...");

        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: file_name.to_string(),
            data,
            content_type: "image/jpeg".to_string(),
            priority: 150,  // 썸네일은 중간 우선순위
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let _ = s3_uploader.queue_segment(segment).await;
    }
}

/// 플레이리스트 URL 업데이트
fn update_playlist_urls(data: &[u8], base_url: &str) -> Vec<u8> {
    let content = String::from_utf8_lossy(data);
    let mut updated = String::new();

    for line in content.lines() {
        if line.starts_with("#") {
            // 메타데이터 라인은 그대로 유지
            updated.push_str(line);
            updated.push('\n');
        } else if !line.trim().is_empty() {
            // 세그먼트 파일명을 S3 URL로 변환
            if line.starts_with("http") {
                // 이미 절대 URL인 경우
                updated.push_str(line);
            } else {
                // 상대 경로인 경우 S3 URL로 변환
                updated.push_str(&format!("{}/{}", base_url, line));
            }
            updated.push('\n');
        }
    }

    updated.into_bytes()
}

