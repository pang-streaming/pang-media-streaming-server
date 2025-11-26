use std::sync::Arc;
use std::collections::HashMap;
use log::{info, warn, error};
use crate::config::{ApiConfig, Config};
use crate::data_layer::storage::memory_buffer_manager::MemoryBufferManager;
use crate::data_layer::storage::memory_s3_uploader::{MemoryS3Uploader, MemorySegment};
use super::ffmpeg_pipeline::MemoryFfmpegPipelineManager;
use super::master_playlist_generator::{MasterPlaylistGenerator, StreamMetadata, ReplayManager};
use aws_sdk_s3::Client as S3Client;
use chrono::Utc;

/// 메모리 기반 HLS 변환 관리자
pub struct MemoryHlsManager {
    ffmpeg_manager: Arc<MemoryFfmpegPipelineManager>,
    buffer_manager: Arc<MemoryBufferManager>,
    s3_uploader: Arc<MemoryS3Uploader>,
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
        // S3 클라이언트 생성 (config.toml의 자격 증명 사용)
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::{BehaviorVersion, Region, timeout::TimeoutConfig};
        use std::time::Duration;

        let credentials = Credentials::new(
            &config.s3.access_key,
            &config.s3.secret_access_key,
            None,
            None,
            "static",
        );

        // 타임아웃 설정 (RequestTimeout 방지)
        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .operation_timeout(Duration::from_secs(60))
            .operation_attempt_timeout(Duration::from_secs(30))
            .build();

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.s3.region.clone()))
            .credentials_provider(credentials)
            .timeout_config(timeout_config)
            .build();

        let s3_client = Arc::new(S3Client::from_conf(s3_config));

        // S3 연결 테스트
        info!("Testing S3 connection to bucket: {}", config.s3.bucket);
        match s3_client.list_objects_v2()
            .bucket(&config.s3.bucket)
            .max_keys(1)
            .send()
            .await
        {
            Ok(_) => info!("S3 connection successful!"),
            Err(e) => {
                error!("S3 connection failed: {:?}", e);
                error!("   Please check:");
                error!("   - Bucket name: {}", config.s3.bucket);
                error!("   - Region: {}", config.s3.region);
                error!("   - Access key is configured: {}", !config.s3.access_key.is_empty());
                error!("   - Secret key is configured: {}", !config.s3.secret_access_key.is_empty());
            }
        }

        // 메모리 버퍼 관리자 생성 (config 기반)
        let buffer_manager = Arc::new(MemoryBufferManager::new(
            config.streaming.memory_buffer_mb,
            config.streaming.max_segments_per_stream,
        ));

        // 메모리 S3 업로더 생성 (config 기반)
        let s3_uploader = Arc::new(
            MemoryS3Uploader::new(
                s3_client.clone(),
                config.s3.bucket.clone(),
                config.streaming.upload_workers,
            ).await?
        );

        // 메모리 FFmpeg 파이프라인 관리자 생성
        let ffmpeg_manager = Arc::new(
            MemoryFfmpegPipelineManager::new(
                config.clone(),
                buffer_manager.clone(),
                s3_uploader.clone(),
            )
        );

        // 마스터 플레이리스트 생성기
        let bucket_url = format!("https://{}.s3.ap-northeast-2.amazonaws.com", config.s3.bucket);
        let master_generator = Arc::new(MasterPlaylistGenerator::new(bucket_url));

        // 리플레이 매니저
        let replay_manager = Arc::new(tokio::sync::RwLock::new(ReplayManager::new()));
        let stream_metadata = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        info!("Memory-based HLS Manager initialized");
        info!("   - Max buffer size: {}MB", config.streaming.memory_buffer_mb);
        info!("   - Max segments per stream: {}", config.streaming.max_segments_per_stream);
        info!("   - S3 upload workers: {}", config.streaming.upload_workers);
        info!("   - Bucket: {}", config.s3.bucket);

        Ok(Self {
            ffmpeg_manager,
            buffer_manager,
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
        // S3 정리는 하지 않음 (에포크 시간 기반 세그먼트 번호로 중복 없음)
        // 필요시 수동으로 cleanup_s3_stream() 호출 가능

        // 메모리 버퍼에 스트림 생성
        self.buffer_manager.create_stream(stream_name.to_string()).await?;

        // FFmpeg 파이프라인 시작 (자동으로 메모리에 쓰고 S3에 업로드)
        self.ffmpeg_manager.start_pipeline(stream_id, stream_name).await?;

        // 스트림 메타데이터 저장
        {
            let mut metadata_map = self.stream_metadata.write().await;
            metadata_map.insert(stream_name.to_string(), StreamMetadata {
                stream_name: stream_name.to_string(),
                start_time: Utc::now(),
                end_time: None,
                is_live: true,
                bitrate: 5000000,
                resolution: "1920x1080".to_string(),
                codecs: "avc1.640028,mp4a.40.2".to_string(),
                frame_rate: 60.0,
            });
        }

        info!("Memory-based HLS conversion started for stream {} ({})", stream_id, stream_name);
        info!("Segments use epoch-based numbering (no conflicts)");
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

        // 남은 세그먼트 모두 업로드
        if let Some(data) = self.buffer_manager.get_stream_data(stream_name).await {
            self.s3_uploader.upload_stream_from_memory(stream_name, data).await?;
        }

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

        // 메모리 버퍼 정리 (리플레이를 위해 S3에는 유지)
        self.buffer_manager.remove_stream(stream_name).await?;

        info!("Memory-based HLS conversion stopped for stream {} ({})", stream_id, stream_name);
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

    /// 스트림을 S3에 업로드 (이미 자동으로 업로드되지만 수동 트리거용)
    pub async fn upload_stream_to_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(data) = self.buffer_manager.get_stream_data(stream_name).await {
            self.s3_uploader.upload_stream_from_memory(stream_name, data).await?;
            info!("Manual upload triggered for stream: {}", stream_name);
        } else {
            warn!("No data found for stream: {}", stream_name);
        }
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

    /// S3에서 스트림 삭제
    pub async fn delete_stream_from_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // S3 삭제 로직 구현 필요
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

    /// 메모리 사용량 통계
    pub async fn get_memory_stats(&self) -> MemoryStats {
        let (used, max) = self.buffer_manager.get_memory_usage().await;
        let buffer_stats = self.buffer_manager.get_stats().await;

        MemoryStats {
            used_bytes: used,
            max_bytes: max,
            usage_percentage: (used as f64 / max as f64 * 100.0) as u32,
            streams: buffer_stats,
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
    async fn generate_master_playlist(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let base_url = format!("https://{}.s3.ap-northeast-2.amazonaws.com/{}",
            self.config.s3.bucket, stream_name);

        let mut playlist = String::new();

        // HLS 버전
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n\n");

        // VOD 스트림 정보
        playlist.push_str("# VOD Replay Stream\n");
        playlist.push_str("#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,FRAME-RATE=60.00,CODECS=\"avc1.640028,mp4a.40.2\",NAME=\"1080p60-VOD\"\n");
        playlist.push_str(&format!("{}/vod_playlist.m3u8\n", base_url));

        // Master 플레이리스트 업로드
        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: "master.m3u8".to_string(),
            data: playlist.into_bytes(),
            content_type: "application/vnd.apple.mpegurl".to_string(),
            priority: 255,
            created_at: Utc::now(),
            retry_count: 0,
        };
        self.s3_uploader.queue_segment(segment).await;

        info!("Master playlist (VOD) generated for stream: {}", stream_name);
        Ok(())
    }

    /// VOD 플레이리스트 생성 (모든 세그먼트 포함)
    async fn generate_vod_playlist(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use aws_sdk_s3::config::{BehaviorVersion, Region};
        use aws_credential_types::Credentials;

        let credentials = Credentials::new(
            &self.config.s3.access_key,
            &self.config.s3.secret_access_key,
            None,
            None,
            "static",
        );

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(self.config.s3.region.clone()))
            .credentials_provider(credentials)
            .build();

        let client = S3Client::from_conf(s3_config);

        // S3에서 모든 세그먼트 파일 목록 가져오기
        let prefix = format!("{}/", stream_name);
        let list_result = client
            .list_objects_v2()
            .bucket(&self.config.s3.bucket)
            .prefix(&prefix)
            .send()
            .await;

        match list_result {
            Ok(output) => {
                let mut segments = Vec::new();
                let mut init_segment = None;
                let target_duration: f32 = 1.0;

                if let Some(objects) = output.contents {
                    // 세그먼트 파일 수집 및 정렬
                    for obj in objects {
                        if let Some(key) = obj.key {
                            let file_name = key.strip_prefix(&prefix).unwrap_or(&key);

                            if file_name == "init.mp4" {
                                init_segment = Some(file_name.to_string());
                            } else if file_name.starts_with("segment_") && file_name.ends_with(".m4s") {
                                segments.push(file_name.to_string());
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
                    vod_content.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", target_duration.ceil() as i32));
                    vod_content.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");
                    vod_content.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");

                    // init 세그먼트
                    if let Some(init) = init_segment {
                        vod_content.push_str(&format!("#EXT-X-MAP:URI=\"{}\"\n", init));
                    }

                    vod_content.push_str("\n");

                    // 모든 세그먼트 추가
                    let base_url = format!("https://{}.s3.ap-northeast-2.amazonaws.com/{}",
                        self.config.s3.bucket, stream_name);

                    for segment in segments {
                        vod_content.push_str(&format!("#EXTINF:{:.6},\n", target_duration));
                        vod_content.push_str(&format!("{}/{}\n", base_url, segment));
                    }

                    // 종료 태그
                    vod_content.push_str("#EXT-X-ENDLIST\n");

                    // VOD 플레이리스트 업로드
                    let segment = MemorySegment {
                        stream_name: stream_name.to_string(),
                        file_name: "vod_playlist.m3u8".to_string(),
                        data: vod_content.into_bytes(),
                        content_type: "application/vnd.apple.mpegurl".to_string(),
                        priority: 255,
                        created_at: Utc::now(),
                        retry_count: 0,
                    };
                    self.s3_uploader.queue_segment(segment).await;

                    info!("VOD playlist generated with all segments for stream: {}", stream_name);
                }
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
        use aws_sdk_s3::config::{BehaviorVersion, Region};
        use aws_credential_types::Credentials;

        let credentials = Credentials::new(
            &self.config.s3.access_key,
            &self.config.s3.secret_access_key,
            None,
            None,
            "static",
        );

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(self.config.s3.region.clone()))
            .credentials_provider(credentials)
            .build();

        let client = S3Client::from_conf(s3_config);

        // 스트림 디렉토리의 모든 객체 나열
        let prefix = format!("{}/", stream_name);
        match client
            .list_objects_v2()
            .bucket(&self.config.s3.bucket)
            .prefix(&prefix)
            .send()
            .await
        {
            Ok(output) => {
                if let Some(objects) = output.contents {
                    info!("   Found {} existing files to clean up", objects.len());

                    for obj in objects {
                        if let Some(key) = obj.key {
                            // 각 객체 삭제
                            match client
                                .delete_object()
                                .bucket(&self.config.s3.bucket)
                                .key(&key)
                                .send()
                                .await
                            {
                                Ok(_) => info!("Deleted: {}", key),
                                Err(e) => error!("Failed to delete {}: {:?}", key, e),
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Could not list objects for cleanup: {:?}", e);
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