use std::sync::Arc;
use std::collections::VecDeque;
use std::process::Stdio;
use tokio::sync::{mpsc, RwLock};
use tokio::fs;
use tokio::process::Command;
use log::{info, error, debug};
use notify::{Watcher, RecursiveMode, Event, EventKind};
use chrono::Utc;
use crate::config::ThumbnailConfig;
use crate::data_layer::storage::cli_s3_uploader::{CliS3Uploader, CliSegment};

/// 세그먼트 정보 (LL-HLS 파트로 사용)
#[derive(Clone, Debug)]
struct SegmentInfo {
    file_name: String,
    duration: f64,
}

/// LL-HLS 플레이리스트 상태
struct LLHlsState {
    segments: VecDeque<SegmentInfo>,
    media_sequence: u64,
    init_uploaded: bool,
    max_segments: usize,
    segment_count: u64,
    processed_segments: std::collections::HashSet<String>,  // 중복 처리 방지
}

impl LLHlsState {
    fn new() -> Self {
        Self {
            segments: VecDeque::new(),
            media_sequence: 0,
            init_uploaded: false,
            max_segments: 10,
            segment_count: 0,
            processed_segments: std::collections::HashSet::new(),
        }
    }

    fn is_processed(&self, file_name: &str) -> bool {
        self.processed_segments.contains(file_name)
    }

    fn mark_processed(&mut self, file_name: String) {
        self.processed_segments.insert(file_name);
        // 오래된 항목 정리 (최근 20개만 유지)
        if self.processed_segments.len() > 20 {
            self.processed_segments.clear();
        }
    }

    fn add_segment(&mut self, file_name: String, duration: f64) {
        self.segments.push_back(SegmentInfo {
            file_name,
            duration,
        });
        self.segment_count += 1;

        // 오래된 세그먼트 제거
        while self.segments.len() > self.max_segments {
            self.segments.pop_front();
            self.media_sequence += 1;
        }
    }

    fn generate_playlist(&self) -> String {
        let mut playlist = String::new();

        // 헤더
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str("#EXT-X-TARGETDURATION:1\n");
        playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", self.media_sequence));

        // init 세그먼트
        playlist.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");
        playlist.push_str("\n");

        // 각 세그먼트 추가 (1초 단위)
        for seg in &self.segments {
            playlist.push_str(&format!("#EXTINF:{:.3},\n", seg.duration));
            playlist.push_str(&format!("{}\n", seg.file_name));
        }

        playlist
    }
}


/// FFmpeg 플레이리스트에서 duration 파싱
fn parse_extinf_duration(line: &str) -> Option<f64> {
    // #EXTINF:1.001, -> 1.001
    if line.starts_with("#EXTINF:") {
        let duration_str = line
            .trim_start_matches("#EXTINF:")
            .split(',')
            .next()?;
        duration_str.parse().ok()
    } else {
        None
    }
}

/// 세그먼트 파일에서 썸네일 추출 (비동기, 별도 태스크)
/// fMP4 세그먼트는 init.mp4와 함께 사용해야 디코딩 가능
async fn extract_thumbnail_from_segment(
    watch_dir: String,
    segment_path: String,
    stream_name: String,
    thumbnail_config: ThumbnailConfig,
    s3_uploader: Arc<CliS3Uploader>,
) {
    let init_path = format!("{}/init.mp4", watch_dir);
    let thumb_path = format!("{}/thumbnail.jpg", watch_dir);
    let combined_path = format!("{}/combined.mp4", watch_dir);
    let scale = format!("scale={}:{}", thumbnail_config.width, thumbnail_config.height);
    let quality = thumbnail_config.quality.to_string();

    // init.mp4 + segment.m4s를 바이너리로 합쳐서 combined.mp4 생성
    let init_data = match fs::read(&init_path).await {
        Ok(d) => d,
        Err(_) => {
            let _ = fs::remove_file(&segment_path).await;
            return;
        }
    };
    let segment_data = match fs::read(&segment_path).await {
        Ok(d) => d,
        Err(_) => {
            let _ = fs::remove_file(&segment_path).await;
            return;
        }
    };

    let mut combined = init_data;
    combined.extend(segment_data);

    if fs::write(&combined_path, &combined).await.is_err() {
        let _ = fs::remove_file(&segment_path).await;
        return;
    }

    // FFmpeg으로 첫 프레임 추출
    let result = Command::new("ffmpeg")
        .args([
            "-y",
            "-i", &combined_path,
            "-vf", &scale,
            "-vframes", "1",
            "-q:v", &quality,
            &thumb_path,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await;

    // 임시 파일 삭제
    let _ = fs::remove_file(&combined_path).await;
    let _ = fs::remove_file(&segment_path).await;

    if let Ok(output) = result {
        if output.status.success() {
            // 썸네일 파일 읽기 및 업로드
            if let Ok(data) = fs::read(&thumb_path).await {
                let file_size_kb = data.len() / 1024;
                info!("[Thumbnail] Extracted from segment ({}KB)", file_size_kb);

                let _ = s3_uploader.queue_segment(CliSegment {
                    stream_name,
                    file_name: "thumbnail.jpg".to_string(),
                    data,
                    content_type: "image/jpeg".to_string(),
                    priority: 50,  // 낮은 우선순위
                    created_at: Utc::now(),
                }).await;

                // 임시 썸네일 파일 삭제
                let _ = fs::remove_file(&thumb_path).await;
            }
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("[Thumbnail] FFmpeg extraction failed: {}", stderr);
        }
    }
}

pub async fn start_file_watcher(
    stream_name: String,
    watch_dir: String,
    s3_uploader: Arc<CliS3Uploader>,
    thumbnail_config: ThumbnailConfig,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    info!("[FileWatcher] Initializing LL-HLS for stream: {} dir: {}", stream_name, watch_dir);

    let handle = tokio::spawn(async move {
        let (tx, mut rx) = mpsc::channel(500);
        let state = Arc::new(RwLock::new(LLHlsState::new()));

        info!("[FileWatcher] Task started for: {}", stream_name);

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

        // 마지막 처리된 duration 저장 (플레이리스트 파싱용)
        let mut pending_duration: Option<f64> = None;

        // 이벤트 처리
        while let Some(event) = rx.recv().await {
            if let EventKind::Create(_) | EventKind::Modify(_) = event.kind {
                for path in event.paths {
                    if let Some(file_name) = path.file_name() {
                        let file_name_str = file_name.to_string_lossy().to_string();

                        // .tmp 파일 무시
                        if file_name_str.ends_with(".tmp") {
                            continue;
                        }

                        // 파일 읽기
                        let data = match fs::read(&path).await {
                            Ok(d) => d,
                            Err(e) => {
                                if e.kind() != std::io::ErrorKind::NotFound {
                                    debug!("[FileWatcher] Failed to read {}: {}", file_name_str, e);
                                }
                                continue;
                            }
                        };

                        let file_size_kb = data.len() / 1024;

                        if file_name_str == "playlist.m3u8" {
                            // FFmpeg 플레이리스트 파싱하여 duration 추출
                            let content = String::from_utf8_lossy(&data).to_string();

                            // 새로운 EXTINF 줄 찾기
                            for line in content.lines() {
                                if let Some(duration) = parse_extinf_duration(line) {
                                    pending_duration = Some(duration);
                                }
                            }
                            // FFmpeg 플레이리스트는 삭제하지 않음 (참조용)

                        } else if file_name_str == "init.mp4" {
                            let mut state_guard = state.write().await;
                            if !state_guard.init_uploaded {
                                info!("[Init] Uploading init.mp4 ({}KB)", file_size_kb);
                                let _ = s3_uploader.queue_segment(CliSegment {
                                    stream_name: stream_name.clone(),
                                    file_name: "init.mp4".to_string(),
                                    data,
                                    content_type: "video/mp4".to_string(),
                                    priority: 255,
                                    created_at: Utc::now(),
                                }).await;
                                state_guard.init_uploaded = true;
                            }

                        } else if file_name_str.ends_with(".m4s") {
                            // 중복 처리 체크
                            {
                                let state_guard = state.read().await;
                                if state_guard.is_processed(&file_name_str) {
                                    continue;
                                }
                            }

                            // 세그먼트 처리
                            let duration = pending_duration.unwrap_or(1.0);
                            pending_duration = None;

                            info!("[Segment] {} ({}KB, {:.3}s)", file_name_str, file_size_kb, duration);

                            // 세그먼트 업로드
                            let _ = s3_uploader.queue_segment(CliSegment {
                                stream_name: stream_name.clone(),
                                file_name: file_name_str.clone(),
                                data,
                                content_type: "video/iso.segment".to_string(),
                                priority: 150,
                                created_at: Utc::now(),
                            }).await;

                            // 상태 업데이트 및 LL-HLS 플레이리스트 생성
                            let (playlist_content, should_extract_thumbnail, current_count) = {
                                let mut state_guard = state.write().await;
                                state_guard.mark_processed(file_name_str.clone());
                                state_guard.add_segment(file_name_str.clone(), duration);
                                let playlist = state_guard.generate_playlist();

                                // 첫 번째 세그먼트는 스킵 (init.mp4 안정화 대기), 두 번째부터 추출
                                // 이후 interval_seconds마다 추출
                                let is_second = state_guard.segment_count == 2;
                                let interval_segments = thumbnail_config.interval_seconds as u64;
                                let should_extract = thumbnail_config.enabled
                                    && (is_second || state_guard.segment_count % interval_segments == 0);

                                (playlist, should_extract, state_guard.segment_count)
                            };

                            if should_extract_thumbnail {
                                info!("[Thumbnail] Will extract thumbnail at segment #{}", current_count);
                            }

                            // LL-HLS 플레이리스트 업로드
                            let _ = s3_uploader.queue_segment(CliSegment {
                                stream_name: stream_name.clone(),
                                file_name: "playlist.m3u8".to_string(),
                                data: playlist_content.into_bytes(),
                                content_type: "application/vnd.apple.mpegurl".to_string(),
                                priority: 200,  // 세그먼트보다 높은 우선순위
                                created_at: Utc::now(),
                            }).await;

                            // 썸네일 추출 (별도 태스크, 메인 스트림 블로킹 없음)
                            if should_extract_thumbnail {
                                let segment_path = path.to_string_lossy().to_string();
                                let watch_dir_clone = watch_dir.clone();
                                let stream_name_clone = stream_name.clone();
                                let thumb_config = thumbnail_config.clone();
                                let uploader = Arc::clone(&s3_uploader);

                                tokio::spawn(async move {
                                    extract_thumbnail_from_segment(
                                        watch_dir_clone,
                                        segment_path,
                                        stream_name_clone,
                                        thumb_config,
                                        uploader,
                                    ).await;
                                });
                            } else {
                                // 썸네일 추출하지 않을 때만 세그먼트 파일 삭제
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
