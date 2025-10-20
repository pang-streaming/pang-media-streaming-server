use std::path::PathBuf;
use std::sync::Arc;
use crate::config::{ApiConfig, Config};
use crate::business_layer::monitoring::{MetricsCollector, LatencyMonitor};
use crate::business_layer::streaming::ll_hls::{
    playlist_generator::LLHLSPlaylistGenerator,
    segment_manager::LLHLSSegmentManager,
    server_push::LLHLSServerPush,
    preload_hint::LLHLSPreloadHintManager,
};
use crate::data_layer::storage::s3_storage::S3Storage;
use super::ffmpeg_pipeline::FfmpegPipelineManager;

/// HLS 변환 관리자
pub struct HlsConversionManager {
    ffmpeg_manager: Arc<FfmpegPipelineManager>,
    playlist_generator: Arc<LLHLSPlaylistGenerator>,
    segment_manager: Arc<LLHLSSegmentManager>,
    server_push: Arc<LLHLSServerPush>,
    preload_hint_manager: Arc<LLHLSPreloadHintManager>,
    metrics_collector: Arc<MetricsCollector>,
    latency_monitor: Arc<LatencyMonitor>,
    s3_storage: Option<Arc<S3Storage>>,
    output_dir: String,
    pub api_config: ApiConfig,
}

impl HlsConversionManager {
    /// 새로운 HLS 변환 관리자 생성
    pub async fn new(
        config: Config,
        metrics_collector: Arc<MetricsCollector>,
        latency_monitor: Arc<LatencyMonitor>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let output_dir = config.hls.save_dir.clone();
        
        // FFmpeg 파이프라인 관리자 생성
        let ffmpeg_manager = Arc::new(FfmpegPipelineManager::new(config.clone()));

        // LL-HLS 컴포넌트들 생성
        let playlist_generator = Arc::new(LLHLSPlaylistGenerator::new(config.hls.clone()));
        let segment_manager = Arc::new(LLHLSSegmentManager::new(output_dir.clone(), config.hls.clone()));
        let server_push = Arc::new(LLHLSServerPush::new());
        let preload_hint_manager = Arc::new(LLHLSPreloadHintManager::new());

        // S3 스토리지 초기화
        let s3_storage = match S3Storage::new(&config.s3).await {
            Ok(storage) => {
                println!("✅ S3 Storage initialized: {}/{}", config.s3.region, config.s3.bucket);
                Some(Arc::new(storage))
            }
            Err(e) => {
                eprintln!("⚠️ Failed to initialize S3 storage: {}", e);
                None
            }
        };

        Ok(Self {
            ffmpeg_manager,
            playlist_generator,
            segment_manager,
            server_push,
            preload_hint_manager,
            metrics_collector,
            latency_monitor,
            s3_storage,
            output_dir,
            api_config: config.api.clone(),
        })
    }

    /// HLS 변환 시작
    pub async fn start_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // FFmpeg 파이프라인 시작
        self.ffmpeg_manager.start_pipeline(stream_id, stream_name).await?;

        // 실시간 S3 업로드 시작
        if let Some(s3_storage) = &self.s3_storage {
            if let Err(e) = self.start_realtime_s3_upload(stream_name, stream_id, s3_storage).await {
                eprintln!("⚠️ Failed to start realtime S3 upload: {}", e);
            }
        }

        // 메트릭 기록 (기존 메서드가 없으므로 주석 처리)
        // self.metrics_collector.record_stream_start(stream_name).await?;

        Ok(())
    }

    /// HLS 변환 중지
    pub async fn stop_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // FFmpeg 파이프라인 중지
        self.ffmpeg_manager.stop_pipeline(stream_id)?;

        // 메트릭 기록 (기존 메서드가 없으므로 주석 처리)
        // self.metrics_collector.record_stream_end(stream_name).await?;

        println!("🛑 HLS conversion stopped for stream {} (key: {})", stream_id, stream_name);
        Ok(())
    }

    /// 스트림 데이터 처리
    pub async fn process_stream_data(
        &self,
        stream_id: u32,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // FFmpeg 파이프라인에 데이터 전송
        self.ffmpeg_manager.send_data(stream_id, data)?;

        // 메트릭 업데이트
        // self.metrics_collector.record_data_processed(stream_id, data.len()).await?;

        Ok(())
    }

    /// 실시간 S3 업로드 시작
    async fn start_realtime_s3_upload(
        &self,
        stream_name: &str,
        stream_id: u32,
        s3_storage: &Arc<S3Storage>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let output_dir = PathBuf::from(&self.output_dir).join(stream_name);
        let stream_name_clone = stream_name.to_string();
        let s3_prefix = format!("hls_output/{}", stream_name);
        let s3_storage_clone = s3_storage.clone();

        tokio::spawn(async move {
            // 출력 디렉토리가 존재하지 않으면 생성
            if let Err(e) = tokio::fs::create_dir_all(&output_dir).await {
                eprintln!("❌ Failed to create output directory {}: {}", output_dir.display(), e);
                return;
            }

            // S3 파일 감시기 생성
            if let Some(upload_sender) = &s3_storage_clone.upload_sender {
                use crate::data_layer::storage::s3_file_watcher::S3FileWatcher;
                
                let watcher = S3FileWatcher::new(
                    upload_sender.clone(),
                    stream_name_clone.clone(),
                    s3_prefix,
                );

                if let Err(e) = watcher.start_watching(&output_dir).await {
                    eprintln!("❌ Failed to start file watching: {}", e);
                }
            }
        });

        println!("📤 Realtime S3 upload started for stream '{}'", stream_name);
        Ok(())
    }

    /// S3 업로드 상태 조회
    pub async fn get_s3_upload_status(&self, stream_name: &str) -> Option<crate::data_layer::storage::s3_types::UploadStatus> {
        if let Some(s3_storage) = &self.s3_storage {
            s3_storage.get_upload_status(stream_name).await
        } else {
            None
        }
    }

    /// 스트림을 S3에 업로드
    pub async fn upload_stream_to_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(s3_storage) = &self.s3_storage {
            let local_dir = std::path::Path::new(&self.output_dir).join(stream_name);
            let s3_prefix = format!("hls_output/{}", stream_name);
            
            println!("📤 Starting S3 upload for stream '{}'...", stream_name);
            s3_storage.upload_directory_streaming(&local_dir, &s3_prefix, stream_name).await?;
            
            Ok(())
        } else {
            Err("S3 storage not initialized".into())
        }
    }

    /// S3에서 스트림 삭제
    pub async fn delete_stream_from_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(s3_storage) = &self.s3_storage {
            let s3_prefix = format!("hls_output/{}", stream_name);
            
            println!("🗑️ Deleting stream '{}' from S3...", stream_name);
            s3_storage.delete_directory(&s3_prefix).await?;
            
            Ok(())
        } else {
            Err("S3 storage not initialized".into())
        }
    }

    /// 활성 스트림 수 조회
    pub fn get_active_stream_count(&self) -> usize {
        self.ffmpeg_manager.active_pipeline_count()
    }

    /// 스트림 존재 여부 확인
    pub fn has_stream(&self, stream_id: u32) -> bool {
        self.ffmpeg_manager.has_pipeline(stream_id)
    }
}
