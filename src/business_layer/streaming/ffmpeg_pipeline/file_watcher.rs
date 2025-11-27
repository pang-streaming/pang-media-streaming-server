use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::fs;
use log::{info, warn, error, debug};
use notify::{Watcher, RecursiveMode, Event, EventKind};
use crate::data_layer::storage::memory_buffer_manager::MemoryBufferManager;
use crate::data_layer::storage::memory_s3_uploader::{MemoryS3Uploader, MemorySegment};
use crate::business_layer::streaming::ll_hls_playlist::{
    LLHlsPlaylistGenerator, LLHlsPart
};

struct LLHlsState {
    generator: LLHlsPlaylistGenerator,
}

pub async fn start_file_watcher(
    stream_name: String,
    watch_dir: String,
    buffer_manager: Arc<MemoryBufferManager>,
    s3_uploader: Arc<MemoryS3Uploader>,
    s3_bucket: String,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    info!("[FileWatcher] Initializing for stream: {} dir: {}", stream_name, watch_dir);

    let handle = tokio::spawn(async move {
        let (tx, mut rx) = mpsc::channel(100);
        info!("[FileWatcher] Task started for: {}", stream_name);

        // LL-HLS 상태 초기화
        let ll_hls_state = Arc::new(RwLock::new(LLHlsState {
            generator: LLHlsPlaylistGenerator::new(
                stream_name.clone(),
                format!("https://{}.s3.ap-northeast-2.amazonaws.com/{}", s3_bucket, stream_name),
                "1080p".to_string(),
                5000000,
            )
            .with_target_duration(2.0)
            .with_part_target(1.0)
            .with_max_segments(10),
        }));

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
            error!("[FileWatcher] Failed to watch directory: {}", e);
            return;
        }
        info!("[FileWatcher] Watching directory: {}", watch_dir);

        // 이벤트 처리
        while let Some(event) = rx.recv().await {
            if let EventKind::Create(_) | EventKind::Modify(_) = event.kind {
                for path in event.paths {
                    if let Some(file_name) = path.file_name() {
                        let file_name_str = file_name.to_string_lossy().to_string();
                        debug!("[FileWatcher] File event: {}", file_name_str);

                        // 파일을 메모리로 읽기
                        if let Ok(data) = fs::read(&path).await {
                            info!("[FileWatcher] Processing file: {} ({}KB)", file_name_str, data.len() / 1024);
                            process_file_ll_hls(
                                &file_name_str,
                                data,
                                &stream_name,
                                &buffer_manager,
                                &s3_uploader,
                                &s3_bucket,
                                &ll_hls_state,
                            ).await;

                            // 파일 삭제 처리 (init.mp4 제외)
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

async fn process_file_ll_hls(
    file_name: &str,
    data: Vec<u8>,
    stream_name: &str,
    buffer_manager: &Arc<MemoryBufferManager>,
    s3_uploader: &Arc<MemoryS3Uploader>,
    s3_bucket: &str,
    ll_hls_state: &Arc<RwLock<LLHlsState>>,
) {
    let base_url = format!("https://{}.s3.ap-northeast-2.amazonaws.com/{}", s3_bucket, stream_name);

    if file_name.ends_with(".m3u8") {
        let modified_data = update_playlist_urls(&data, &base_url);
        let _ = buffer_manager.store_playlist(stream_name, modified_data).await;

    } else if file_name == "init.mp4" {
        let init_filename = "init.m4s".to_string();
        let mut state = ll_hls_state.write().await;
        state.generator.set_init_segment(init_filename.clone());

        let _ = buffer_manager.store_init_segment(stream_name, data.clone()).await;

        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: init_filename,
            data,
            content_type: "video/mp4".to_string(),
            priority: 200,
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let _ = s3_uploader.queue_segment(segment).await;

    } else if file_name.ends_with(".m4v") || file_name.ends_with(".m4s") || file_name.ends_with(".ts") {
        // 세그먼트 파일 처리
        let file_size = data.len();
        info!("[Segment] {} ({}KB) - processing", file_name, file_size / 1024);
        let _ = buffer_manager.store_segment(stream_name, file_name, data.clone()).await;

        // LL-HLS 상태 업데이트
        let mut state = ll_hls_state.write().await;

        // 기본 듀레이션 추정
        let estimated_duration = 2.0;  // 2초 세그먼트
        let part_duration = 1.0;       // 1초 파트
        let num_parts = 2;             // 2초 / 1초 = 2개 파트
        let bitrate = if estimated_duration > 0.0 {
            ((file_size as f64 * 8.0) / estimated_duration) as u64
        } else {
            5000000
        };

        // 전체 세그먼트 처리
        if state.generator.segment_count() == 0 {
            state.generator.start_segment();
        }

        // 세그먼트 이름에서 확장자 제거
        let segment_base = file_name.trim_end_matches(".m4v").trim_end_matches(".m4s").trim_end_matches(".ts");

        // 파트를 2등분으로 분할 후 업로드
        let part_size = data.len() / num_parts;
        let mut part_filenames = Vec::new();
        let mut part_data_list = Vec::new();

        for i in 0..num_parts {
            let part_filename = format!("{}_{}.m4s", segment_base, i);

            // 파트 데이터 추출
            let start = i * part_size;
            let end = if i == num_parts - 1 { data.len() } else { (i + 1) * part_size };
            let part_data = data[start..end].to_vec();

            part_filenames.push(part_filename.clone());
            part_data_list.push(part_data);

            let part = LLHlsPart {
                index: i,
                duration: part_duration,
                filename: part_filename,
                independent: i == 0,  // 첫 번째 파트만 independent
            };
            state.generator.add_part(part);
        }

        // 전체 세그먼트 파일명을 .m4s로
        let full_segment_filename = format!("{}.m4s", segment_base);

        state.generator.complete_segment(
            full_segment_filename.clone(),
            estimated_duration,
            bitrate,
        );

        // 새 세그먼트 시작
        state.generator.start_segment();

        // LL-HLS 플레이리스트 생성
        let ll_hls_playlist = state.generator.generate_media_playlist(None);
        drop(state);

        // 1. 파트 세그먼트 업로드 (최고 우선순위)
        for (part_filename, part_data) in part_filenames.iter().zip(part_data_list.iter()) {
            debug!("[S3 Queue] part: {} ({}KB)", part_filename, part_data.len() / 1024);
            let part_segment = MemorySegment {
                stream_name: stream_name.to_string(),
                file_name: part_filename.clone(),
                data: part_data.clone(),
                content_type: "video/iso.segment".to_string(),
                priority: 255,  // 파트 세그먼트 최우선
                created_at: chrono::Utc::now(),
                retry_count: 0,
            };
            if let Err(e) = s3_uploader.queue_segment(part_segment).await {
                error!("[S3 Error] Failed to queue part {}: {:?}", part_filename, e);
            }
        }

        // 2. 전체 세그먼트 파일 업로드 (높은 우선순위)
        info!("[S3 Queue] segment: {} ({}KB)", full_segment_filename, data.len() / 1024);
        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: full_segment_filename.clone(),
            data,
            content_type: "video/iso.segment".to_string(),
            priority: 200,  // 전체 세그먼트 두번째
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        if let Err(e) = s3_uploader.queue_segment(segment).await {
            error!("[S3 Error] Failed to queue segment {}: {:?}", full_segment_filename, e);
        }

        // 3. 플레이리스트 업로드 (세그먼트 업로드 후 - 낮은 우선순위)
        debug!("[S3 Queue] playlist: chunklist.m3u8 (after segments)");
        let playlist_segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: "chunklist.m3u8".to_string(),
            data: ll_hls_playlist.into_bytes(),
            content_type: "application/vnd.apple.mpegurl".to_string(),
            priority: 50,   // 플레이리스트는 세그먼트 후에
            created_at: chrono::Utc::now(),
            retry_count: 0,
        };
        if let Err(e) = s3_uploader.queue_segment(playlist_segment).await {
            error!("[S3 Error] Failed to queue playlist: {:?}", e);
        }

    } else if file_name == "thumbnail.jpg" {
        // 썸네일 업로드
        info!("Uploading thumbnail to S3...");

        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: file_name.to_string(),
            data,
            content_type: "image/jpeg".to_string(),
            priority: 150,
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

