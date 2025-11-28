use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::fs;
use log::{info, error, debug};
use notify::{Watcher, RecursiveMode, Event, EventKind};
use crate::data_layer::storage::cli_s3_uploader::CliS3Uploader;

pub async fn start_file_watcher(
    stream_name: String,
    watch_dir: String,
    s3_uploader: Arc<CliS3Uploader>,
    s3_bucket: String,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    info!("[FileWatcher] Initializing for stream: {} dir: {}", stream_name, watch_dir);

    let handle = tokio::spawn(async move {
        let (tx, mut rx) = mpsc::channel(100);
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


        // 이벤트 처리
        while let Some(event) = rx.recv().await {
            if let EventKind::Create(_) | EventKind::Modify(_) = event.kind {
                for path in event.paths {
                    if let Some(file_name) = path.file_name() {
                        let file_name_str = file_name.to_string_lossy().to_string();

                        // .tmp 파일 및 .part 파일 무시 (파트는 내부에서 생성/삭제됨)
                        if file_name_str.ends_with(".tmp") || file_name_str.contains(".part") {
                            continue;
                        }

                        debug!("[FileWatcher] File event: {}", file_name_str);

                        // 파일 처리
                        process_file(
                            &path,
                            &file_name_str,
                            &stream_name,
                            &s3_uploader,
                        ).await;
                    }
                }
            }
        }
    });

    Ok(handle)
}

async fn process_file(
    file_path: &std::path::Path,
    file_name: &str,
    stream_name: &str,
    s3_uploader: &Arc<CliS3Uploader>,
) {
    // 파일 존재 여부 확인 (삭제된 파일 이벤트 무시)
    if !file_path.exists() {
        return;
    }

    // 파일 읽기
    let data = match fs::read(file_path).await {
        Ok(d) => d,
        Err(e) => {
            // 파일이 이미 삭제된 경우 무시
            if e.kind() == std::io::ErrorKind::NotFound {
                return;
            }
            error!("[FileWatcher] Failed to read {}: {}", file_name, e);
            return;
        }
    };

    let file_size_kb = data.len() / 1024;
    info!("[FileWatcher] Processing: {} ({}KB)", file_name, file_size_kb);

    if file_name.ends_with(".m3u8") {
        // 플레이리스트: LL-HLS 형식으로 변환 후 업로드 (Priority 3)
        info!("[Playlist] {} - converting to LL-HLS format (Priority 3)", file_name);
        let modified_data = convert_to_ll_hls_playlist(&data);

        if let Err(e) = s3_uploader.queue_data(
            stream_name,
            file_name,
            modified_data,
            "application/vnd.apple.mpegurl",
            80,  // Priority 3 (part0=100, part1=90, playlist=80, full=50)
        ).await {
            error!("[S3 Error] Failed to queue playlist: {:?}", e);
        }

        // 원본 플레이리스트 삭제
        let _ = fs::remove_file(file_path).await;

    } else if file_name == "init.mp4" {
        // init 세그먼트
        info!("[Init] {} - uploading", file_name);

        if let Err(e) = s3_uploader.queue_data(
            stream_name,
            file_name,
            data,
            "video/mp4",
            250,
        ).await {
            error!("[S3 Error] Failed to queue init: {:?}", e);
        }
        // init.mp4는 삭제하지 않음 (FFmpeg가 계속 사용)

    } else if file_name.ends_with(".m4s") || file_name.ends_with(".ts") {
        // 세그먼트 처리: 2초 세그먼트를 1초씩 2개 파트로 분할
        let base_name = file_name.trim_end_matches(".m4s").trim_end_matches(".ts");

        info!("[Segment] {} ({}KB) - splitting 2s segment into 2x1s parts", file_name, file_size_kb);

        let parts = split_segment_into_parts(&data, file_name);

        // 우선순위 순서대로 업로드: part0 -> part1 -> full segment
        // 1. Part 0 업로드 (최우선)
        if let Some(part0_data) = parts.get(0) {
            let part0_name = format!("{}.part0.m4s", base_name);
            info!("[Part0] {} ({}KB) - Priority 1", part0_name, part0_data.len() / 1024);

            if let Err(e) = s3_uploader.queue_data(
                stream_name,
                &part0_name,
                part0_data.clone(),
                "video/iso.segment",
                100,  // 최우선
            ).await {
                error!("[S3 Error] Failed to queue part0: {:?}", e);
            }
        }

        // 2. Part 1 업로드 (두번째)
        if let Some(part1_data) = parts.get(1) {
            let part1_name = format!("{}.part1.m4s", base_name);
            info!("[Part1] {} ({}KB) - Priority 2", part1_name, part1_data.len() / 1024);

            if let Err(e) = s3_uploader.queue_data(
                stream_name,
                &part1_name,
                part1_data.clone(),
                "video/iso.segment",
                90,  // 두번째
            ).await {
                error!("[S3 Error] Failed to queue part1: {:?}", e);
            }
        }

        // 3. Full segment 업로드 (낮은 우선순위, 백그라운드)
        let s3_uploader_clone = Arc::clone(s3_uploader);
        let stream_name_owned = stream_name.to_string();
        let file_name_owned = file_name.to_string();
        let data_clone = data.clone();

        tokio::spawn(async move {
            // 파트 업로드 후 약간의 딜레이
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            info!("[FullSeg] {} ({}KB) - Priority 4 (background)", file_name_owned, data_clone.len() / 1024);

            if let Err(e) = s3_uploader_clone.queue_data(
                &stream_name_owned,
                &file_name_owned,
                data_clone,
                "video/iso.segment",
                50,  // 낮은 우선순위
            ).await {
                error!("[S3 Error] Failed to queue full segment: {:?}", e);
            }
        });

        // 원본 세그먼트 삭제
        let _ = fs::remove_file(file_path).await;

    } else if file_name == "thumbnail.jpg" {
        info!("[Thumbnail] Uploading...");

        if let Err(e) = s3_uploader.queue_data(
            stream_name,
            file_name,
            data,
            "image/jpeg",
            150,
        ).await {
            error!("[S3 Error] Failed to queue thumbnail: {:?}", e);
        }

        let _ = fs::remove_file(file_path).await;
    }
}

/// fMP4 2초 세그먼트를 1초씩 2개 파트로 분할
fn split_segment_into_parts(data: &[u8], file_name: &str) -> Vec<Vec<u8>> {
    let moof_positions = find_moof_positions(data);

    if moof_positions.len() >= 2 {
        let mid_idx = moof_positions.len() / 2;
        let split_pos = moof_positions[mid_idx];
        info!("[Split] {} - {} moof boxes, split at {}", file_name, moof_positions.len(), split_pos);
        vec![data[..split_pos].to_vec(), data[split_pos..].to_vec()]
    } else {
        let mid = data.len() / 2;
        info!("[Split] {} - simple split (only {} moof)", file_name, moof_positions.len());
        vec![data[..mid].to_vec(), data[mid..].to_vec()]
    }
}

/// fMP4에서 moof box 위치 찾기
fn find_moof_positions(data: &[u8]) -> Vec<usize> {
    let mut positions = Vec::new();
    let mut i = 0;

    while i + 8 <= data.len() {
        let size = u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;

        if size == 0 || size > data.len() - i {
            break;
        }

        if &data[i + 4..i + 8] == b"moof" {
            positions.push(i);
        }

        i += size;
    }

    positions
}

/// FFmpeg 플레이리스트를 LL-HLS 형식으로 변환 (파트 태그 포함)
fn convert_to_ll_hls_playlist(data: &[u8]) -> Vec<u8> {
    let content = String::from_utf8_lossy(data);
    let mut updated = String::new();
    let mut added_part_inf = false;

    for line in content.lines() {
        if line.starts_with("#EXTM3U") {
            updated.push_str(line);
            updated.push('\n');
        } else if line.starts_with("#EXT-X-VERSION") {
            // LL-HLS는 버전 9 이상 필요
            updated.push_str("#EXT-X-VERSION:9\n");
        } else if line.starts_with("#EXT-X-TARGETDURATION") {
            updated.push_str(line);
            updated.push('\n');
            // PART-INF 추가 (파트 타겟 1초)
            if !added_part_inf {
                updated.push_str("#EXT-X-PART-INF:PART-TARGET=1.0\n");
                added_part_inf = true;
            }
        } else if line.starts_with("#EXTINF:") {
            updated.push_str(line);
            updated.push('\n');
        } else if line.starts_with("#") {
            updated.push_str(line);
            updated.push('\n');
        } else if !line.trim().is_empty() {
            let segment_name = line.trim();

            if segment_name.ends_with(".m4s") || segment_name.ends_with(".ts") {
                let base_name = segment_name.trim_end_matches(".m4s").trim_end_matches(".ts");

                // 파트 0 (1초, INDEPENDENT)
                updated.push_str(&format!(
                    "#EXT-X-PART:DURATION=1.0,URI=\"{}.part0.m4s\",INDEPENDENT=YES\n",
                    base_name
                ));

                // 파트 1 (1초)
                updated.push_str(&format!(
                    "#EXT-X-PART:DURATION=1.0,URI=\"{}.part1.m4s\"\n",
                    base_name
                ));

                // 원본 세그먼트 URL
                updated.push_str(&format!("{}\n", segment_name));
            } else if segment_name.starts_with("http") {
                updated.push_str(segment_name);
                updated.push('\n');
            } else {
                updated.push_str(&format!("{}\n", segment_name));
            }
        }
    }

    updated.into_bytes()
}
