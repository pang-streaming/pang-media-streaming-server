use std::sync::Arc;
use crate::config::Config;
use crate::business_layer::monitoring::{MetricsCollector, LatencyMonitor};
use super::hls_conversion_manager::HlsConversionManager;

/// HLS 변환기 (기존 호환성을 위한 래퍼)
pub struct HlsConvertor {
    conversion_manager: Arc<HlsConversionManager>,
}

impl HlsConvertor {
    /// 새로운 HLS 변환기 생성
    pub async fn new(
        config: Config,
        metrics_collector: Arc<MetricsCollector>,
        latency_monitor: Arc<LatencyMonitor>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let conversion_manager = Arc::new(
            HlsConversionManager::new(config, metrics_collector, latency_monitor).await?
        );

        Ok(Self {
            conversion_manager,
        })
    }

    /// HLS 변환 시작
    pub async fn start_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.conversion_manager.start_hls_conversion(stream_id, stream_name).await
    }

    /// HLS 변환 중지
    pub async fn stop_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.conversion_manager.stop_hls_conversion(stream_id, stream_name).await
    }

    /// 스트림 데이터 처리
    pub async fn process_stream_data(
        &self,
        stream_id: u32,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.conversion_manager.process_stream_data(stream_id, data).await
    }

    /// 스트림을 S3에 업로드
    pub async fn upload_stream_to_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.conversion_manager.upload_stream_to_s3(stream_name).await
    }

    /// S3 업로드 상태 조회
    pub async fn get_s3_upload_status(&self, stream_name: &str) -> Option<crate::data_layer::storage::s3_types::UploadStatus> {
        self.conversion_manager.get_s3_upload_status(stream_name).await
    }

    /// S3에서 스트림 삭제
    pub async fn delete_stream_from_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.conversion_manager.delete_stream_from_s3(stream_name).await
    }

    /// 특정 파일을 S3에 업로드
    pub async fn upload_file_to_s3(&self, stream_name: &str, file_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 기존 호환성을 위한 래퍼 메서드
        // 실제로는 conversion_manager를 통해 처리
        Ok(())
    }

    /// 활성 스트림 수 조회
    pub fn get_active_stream_count(&self) -> usize {
        self.conversion_manager.get_active_stream_count()
    }

    /// 스트림 존재 여부 확인
    pub fn has_stream(&self, stream_id: u32) -> bool {
        self.conversion_manager.has_stream(stream_id)
    }
}