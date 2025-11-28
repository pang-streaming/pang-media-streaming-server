use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use log::{error, info, warn};

use crate::config::{ApiConfig, Config};
use crate::data_layer::storage::cli_s3_uploader::CliS3Uploader;
use super::ffmpeg_pipeline::MemoryFfmpegPipelineManager;
use super::master_playlist_generator::{MasterPlaylistGenerator, ReplayManager, StreamMetadata};

/// 파일 기반 HLS 변환 관리자 (AWS CLI 사용)
pub struct MemoryHlsManager {
    ffmpeg_manager: Arc<MemoryFfmpegPipelineManager>,
    s3_uploader: Arc<CliS3Uploader>,
    master_generator: Arc<MasterPlaylistGenerator>,
    replay_manager: Arc<tokio::sync::RwLock<ReplayManager>>,
    stream_metadata: Arc<tokio::sync::RwLock<HashMap<String, StreamMetadata>>>,
    pub api_config: ApiConfig,
    config: Config,
}

impl MemoryHlsManager {
    /// 새로운 메모리 기반 HLS 변환 관리자 생성
    pub async fn new(
        config: Config,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // AWS CLI용 환경변수 설정 (프로세스 시작 시 한번만 실행되므로 안전)
        // SAFETY: 이 코드는 싱글스레드 초기화 시점에 실행됨
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", &config.s3.access_key);
            std::env::set_var("AWS_SECRET_ACCESS_KEY", &config.s3.secret_access_key);
            std::env::set_var("AWS_DEFAULT_REGION", &config.s3.region);
        }
        info!("AWS credentials configured for CLI");

        // AWS CLI 기반 S3 업로더 생성
        let s3_uploader = Arc::new(CliS3Uploader::new(
            config.s3.bucket.clone(),
            config.s3.region.clone(),
            None,  // endpoint_url (기본 S3 사용)
            config.streaming.upload_workers.max(32),  // 최소 32 워커
        ));

        // AWS CLI 연결 테스트
        info!("Testing AWS CLI S3 connection...");
        let test_result = std::process::Command::new("aws")
            .args(["s3", "ls", &format!("s3://{}", config.s3.bucket), "--max-items", "1"])
            .args(["--region", &config.s3.region])
            .output();

        match test_result {
            Ok(output) => {
                if output.status.success() {
                    info!("AWS CLI S3 connection successful!");
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    warn!("AWS CLI S3 test warning: {}", stderr);
                }
            }
            Err(e) => {
                error!("AWS CLI not found or failed: {}", e);
                error!("Make sure AWS CLI is installed and configured");
            }
        }

        // FFmpeg 파이프라인 관리자 생성 (파일 기반)
        let ffmpeg_manager = Arc::new(MemoryFfmpegPipelineManager::new(
            config.clone(),
            s3_uploader.clone(),
        ));

        // 마스터 플레이리스트 생성기
        let bucket_url = format!("https://{}.s3.{}.amazonaws.com", config.s3.bucket, config.s3.region);
        let master_generator = Arc::new(MasterPlaylistGenerator::new(bucket_url));

        // 리플레이 매니저
        let replay_manager = Arc::new(tokio::sync::RwLock::new(ReplayManager::new()));
        let stream_metadata = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        info!("AWS CLI-based HLS Manager initialized");
        info!("   - S3 upload workers: {}", config.streaming.upload_workers.max(32));
        info!("   - Bucket: {}", config.s3.bucket);
        info!("   - Region: {}", config.s3.region);

        Ok(Self {
            ffmpeg_manager,
            s3_uploader,
            master_generator,
            replay_manager,
            stream_metadata,
            api_config: config.api.clone(),
            config,
        })
    }

    /// HLS 변환 시작
    pub async fn start_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // FFmpeg 파이프라인 시작 (파일 기반으로 S3에 업로드)
        self.ffmpeg_manager
            .start_pipeline(stream_id, stream_name)
            .await?;

        // 스트림 메타데이터 저장
        {
            let mut metadata_map = self.stream_metadata.write().await;
            metadata_map.insert(
                stream_name.to_string(),
                StreamMetadata {
                    stream_name: stream_name.to_string(),
                    start_time: Utc::now(),
                    end_time: None,
                    is_live: true,
                    bitrate: 5_000_000,
                    resolution: "1920x1080".to_string(),
                    codecs: "avc1.640028,mp4a.40.2".to_string(),
                    frame_rate: 60.0,
                },
            );
        }

        info!(
            "File-based HLS conversion started for stream {} ({})",
            stream_id, stream_name
        );
        Ok(())
    }

    /// HLS 변환 중지
    pub async fn stop_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // FFmpeg 파이프라인 중지
        self.ffmpeg_manager.stop_pipeline(stream_id).await?;

        // VOD 플레이리스트 생성 및 업로드
        self.generate_vod_playlist(stream_name).await?;

        // Master 플레이리스트 생성 (VOD 전용)
        self.generate_master_playlist(stream_name).await?;

        // 스트림을 리플레이 가능한 상태로 마킹
        {
            let mut metadata_map = self.stream_metadata.write().await;
            if let Some(mut metadata) = metadata_map.remove(stream_name) {
                metadata.end_time = Some(Utc::now());
                metadata.is_live = false;

                let mut replay_mgr = self.replay_manager.write().await;
                replay_mgr.mark_stream_completed(stream_name.to_string(), metadata);
            }
        }

        info!(
            "File-based HLS conversion stopped for stream {} ({})",
            stream_id, stream_name
        );
        info!("Stream is now available for replay");
        Ok(())
    }

    /// 스트림 데이터 처리
    pub async fn process_stream_data(
        &self,
        stream_id: u32,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.ffmpeg_manager.send_data(stream_id, data).await
    }

    /// 스트림을 S3에 업로드 (파일 기반에서는 자동 업로드됨)
    pub async fn upload_stream_to_s3(
        &self,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 파일 기반 시스템에서는 file_watcher가 자동으로 S3에 업로드
        info!("File-based upload - stream {} is auto-uploaded via file watcher", stream_name);
        Ok(())
    }

    /// S3 업로드 상태 조회
    pub async fn get_s3_upload_status(&self, stream_name: &str) -> UploadStatusInfo {
        let pending = self.s3_uploader.pending_count().await;
        let active = self.s3_uploader.active_count().await;

        UploadStatusInfo {
            stream_name: stream_name.to_string(),
            pending_uploads: pending,
            active_uploads: active,
            is_complete: pending == 0 && active == 0,
        }
    }

    /// S3에서 스트림 삭제 (미구현)
    pub async fn delete_stream_from_s3(
        &self,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        warn!("S3 deletion not implemented yet for: {}", stream_name);
        Ok(())
    }

    /// 활성 스트림 수 조회
    pub async fn get_active_stream_count(&self) -> usize {
        self.ffmpeg_manager.active_pipeline_count().await
    }

    /// 스트림 존재 여부 확인
    pub async fn has_stream(&self, stream_id: u32) -> bool {
        self.ffmpeg_manager.has_pipeline(stream_id).await
    }

    /// 메모리 사용량 통계 (파일 기반에서는 최소 메모리 사용)
    pub async fn get_memory_stats(&self) -> MemoryStats {
        // 파일 기반 시스템에서는 메모리 버퍼를 사용하지 않음
        MemoryStats {
            used_bytes: 0,
            max_bytes: 0,
            usage_percentage: 0,
            streams: HashMap::new(),
        }
    }

    /// 업로드 큐 통계
    pub async fn get_upload_stats(&self) -> UploadStats {
        UploadStats {
            pending_count: self.s3_uploader.pending_count().await,
            active_count: self.s3_uploader.active_count().await,
        }
    }

    /// 시스템 상태 확인
    pub async fn health_check(&self) -> HealthStatus {
        let memory_stats = self.get_memory_stats().await;
        let upload_stats = self.get_upload_stats().await;
        let active_streams = self.get_active_stream_count().await;

        HealthStatus {
            healthy: memory_stats.usage_percentage < 90 && upload_stats.pending_count < 100,
            memory_usage_percentage: memory_stats.usage_percentage,
            active_streams,
            pending_uploads: upload_stats.pending_count,
            active_uploads: upload_stats.active_count,
        }
    }

    /// Master 플레이리스트 생성 (VOD 전용)
    async fn generate_master_playlist(
        &self,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let base_url = format!(
            "https://{}.s3.ap-northeast-2.amazonaws.com/{}",
            self.config.s3.bucket, stream_name
        );

        let mut playlist = String::new();

        // HLS 헤더
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n\n");

        // VOD 스트림 정보
        playlist.push_str("# VOD Replay Stream\n");
        playlist.push_str(
            "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,FRAME-RATE=60.00,CODECS=\"avc1.640028,mp4a.40.2\",NAME=\"1080p60-VOD\"\n",
        );
        playlist.push_str(&format!("{}/vod_playlist.m3u8\n", base_url));

        // Master 플레이리스트 업로드
        if let Err(e) = self.s3_uploader.queue_data(
            stream_name,
            "master.m3u8",
            playlist.into_bytes(),
            "application/vnd.apple.mpegurl",
            255,
        ).await {
            error!("Failed to upload master playlist: {:?}", e);
        }

        info!("Master playlist (VOD) generated for stream: {}", stream_name);
        Ok(())
    }

    /// VOD 플레이리스트 생성 (모든 세그먼트 포함)
    async fn generate_vod_playlist(
        &self,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bucket = self.config.s3.bucket.clone();
        let region = self.config.s3.region.clone();

        // AWS CLI로 S3에서 모든 세그먼트 파일 목록 가져오기
        let prefix = format!("{}/", stream_name);
        let s3_path = format!("s3://{}/{}", bucket, prefix);

        let list_result = std::process::Command::new("aws")
            .args(["s3", "ls", &s3_path, "--region", &region])
            .output();

        match list_result {
            Ok(output) => {
                if !output.status.success() {
                    error!("Failed to list segments for VOD playlist");
                    return Ok(());
                }

                let stdout = String::from_utf8_lossy(&output.stdout);
                let mut segments = Vec::new();
                let mut init_segment = None;
                let target_duration: f32 = 2.0;

                // AWS CLI 출력 파싱 (형식: "2024-01-01 00:00:00 1234 filename")
                for line in stdout.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 4 {
                        let file_name = parts[3];

                        if file_name == "init.mp4" {
                            init_segment = Some(file_name.to_string());
                        } else if file_name.starts_with("segment_")
                            && file_name.ends_with(".m4s")
                        {
                            if (!file_name.ends_with("part1.m4s")
                                && !file_name.ends_with("part0.m4s")
                            ) {
                                segments.push(file_name.to_string());
                            }

                        }
                    }
                }

                // 세그먼트를 시간 순으로 정렬
                segments.sort();

                // VOD 플레이리스트 생성
                let mut vod_content = String::new();

                // HLS 헤더
                vod_content.push_str("#EXTM3U\n");
                vod_content.push_str("#EXT-X-VERSION:7\n");
                vod_content.push_str(&format!(
                    "#EXT-X-TARGETDURATION:{}\n",
                    target_duration.ceil() as i32
                ));
                vod_content.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");
                vod_content.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");

                // init 세그먼트
                if let Some(init) = init_segment {
                    vod_content.push_str(&format!("#EXT-X-MAP:URI=\"{}\"\n", init));
                }

                vod_content.push_str("\n");

                // 모든 세그먼트 추가
                let base_url = format!(
                    "https://{}.s3.{}.amazonaws.com/{}",
                    bucket, region, stream_name
                );

                for segment in &segments {
                    vod_content.push_str(&format!("#EXTINF:{:.6},\n", target_duration));
                    vod_content.push_str(&format!("{}/{}\n", base_url, segment));
                }

                // 종료 태그
                vod_content.push_str("#EXT-X-ENDLIST\n");

                // VOD 플레이리스트 업로드
                if let Err(e) = self.s3_uploader.queue_data(
                    stream_name,
                    "vod_playlist.m3u8",
                    vod_content.into_bytes(),
                    "application/vnd.apple.mpegurl",
                    255,
                ).await {
                    error!("Failed to upload VOD playlist: {:?}", e);
                }

                info!(
                    "VOD playlist generated with {} segments for stream: {}",
                    segments.len(), stream_name
                );
            }
            Err(e) => {
                error!("Failed to list segments for VOD playlist: {:?}", e);
            }
        }

        Ok(())
    }

    /// S3에서 기존 스트림 파일 정리 (선택적 - 수동 호출용)
    #[allow(dead_code)]
    pub async fn cleanup_s3_stream(&self, stream_name: &str) {
        let bucket = &self.config.s3.bucket;
        let region = &self.config.s3.region;
        let s3_path = format!("s3://{}/{}/", bucket, stream_name);

        info!("Cleaning up S3 stream: {}", s3_path);

        // AWS CLI로 디렉토리 삭제
        match std::process::Command::new("aws")
            .args(["s3", "rm", &s3_path, "--recursive", "--region", region])
            .output()
        {
            Ok(output) => {
                if output.status.success() {
                    info!("Successfully cleaned up S3 stream: {}", stream_name);
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    warn!("S3 cleanup warning: {}", stderr);
                }
            }
            Err(e) => {
                error!("Failed to cleanup S3 stream: {:?}", e);
            }
        }
    }
}

/// 업로드 상태 정보
#[derive(Debug, Clone)]
pub struct UploadStatusInfo {
    pub stream_name: String,
    pub pending_uploads: usize,
    pub active_uploads: usize,
    pub is_complete: bool,
}

/// 메모리 통계
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub used_bytes: usize,
    pub max_bytes: usize,
    pub usage_percentage: u32,
    pub streams: std::collections::HashMap<String, (usize, usize)>, // (segment_count, total_size)
}

/// 업로드 통계
#[derive(Debug, Clone)]
pub struct UploadStats {
    pub pending_count: usize,
    pub active_count: usize,
}

/// 시스템 상태
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub memory_usage_percentage: u32,
    pub active_streams: usize,
    pub pending_uploads: usize,
    pub active_uploads: usize,
}
