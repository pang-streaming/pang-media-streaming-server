use std::sync::Arc;
use std::collections::VecDeque;
use tokio::sync::{mpsc, RwLock};
use tokio::fs;
use log::{info, error, debug};
use notify::{Watcher, RecursiveMode, Event, EventKind};
use chrono::Utc;
use crate::data_layer::storage::cli_s3_uploader::{CliS3Uploader, CliSegment};

/// 세그먼트 정보 (LL-HLS 파트로 사용)
#[derive(Clone, Debug)]
struct SegmentInfo {
    file_name: String,
    duration: f64,
    sequence: u64,
}

/// LL-HLS 플레이리스트 상태
struct LLHlsState {
    segments: VecDeque<SegmentInfo>,
    media_sequence: u64,
    init_uploaded: bool,
    max_segments: usize,
}

impl LLHlsState {
    fn new() -> Self {
        Self {
            segments: VecDeque::new(),
            media_sequence: 0,
            init_uploaded: false,
            max_segments: 10,  // 최근 10개 세그먼트 유지
        }
    }

    fn add_segment(&mut self, file_name: String, duration: f64) -> u64 {
        let sequence = self.media_sequence + self.segments.len() as u64;

        self.segments.push_back(SegmentInfo {
            file_name,
            duration,
            sequence,
        });

        // 오래된 세그먼트 제거
        while self.segments.len() > self.max_segments {
            self.segments.pop_front();
            self.media_sequence += 1;
        }

        sequence
    }

    fn generate_playlist(&self) -> String {
        let mut playlist = String::new();

        // 헤더
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str("#EXT-X-TARGETDURATION:2\n");
        playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", self.media_sequence));

        // init 세그먼트
        playlist.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");
        playlist.push_str("\n");

        // 각 세그먼트 추가 (2초 단위)
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

pub async fn start_file_watcher(
    stream_name: String,
    watch_dir: String,
    s3_uploader: Arc<CliS3Uploader>,
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
        let mut last_playlist_content = String::new();

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

                            last_playlist_content = content;
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
                            let playlist_content = {
                                let mut state_guard = state.write().await;
                                state_guard.add_segment(file_name_str.clone(), duration);
                                state_guard.generate_playlist()
                            };

                            // LL-HLS 플레이리스트 업로드
                            let broadcast_id = stream_name.rsplit('/').next().unwrap_or("live");
                            let playlist_name = format!("playlist_{}.m3u8", broadcast_id);

                            let _ = s3_uploader.queue_segment(CliSegment {
                                stream_name: stream_name.clone(),
                                file_name: playlist_name,
                                data: playlist_content.into_bytes(),
                                content_type: "application/vnd.apple.mpegurl".to_string(),
                                priority: 200,  // 세그먼트보다 높은 우선순위
                                created_at: Utc::now(),
                            }).await;

                            // 원본 세그먼트 파일 삭제
                            let _ = fs::remove_file(&path).await;
                        }
                    }
                }
            }
        }
    });

    Ok(handle)
}
