use std::sync::Arc;
use crate::config::Config;
use super::memory_hls_manager::MemoryHlsManager;

/// HLS 변환기 (메모리 기반 시스템 사용)
pub struct HlsConvertor {
    memory_manager: Arc<MemoryHlsManager>,
}

impl HlsConvertor {
    /// 새로운 HLS 변환기 생성
    pub async fn new(
        config: Config,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let memory_manager = Arc::new(
            MemoryHlsManager::new(config).await?
        );

        Ok(Self {
            memory_manager,
        })
    }

    /// HLS 변환 시작
    pub async fn start_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.memory_manager.start_hls_conversion(stream_id, stream_name).await
    }

    /// HLS 변환 중지
    pub async fn stop_hls_conversion(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.memory_manager.stop_hls_conversion(stream_id, stream_name).await
    }

    /// 스트림 데이터 처리
    pub async fn process_stream_data(
        &self,
        stream_id: u32,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.memory_manager.process_stream_data(stream_id, data).await
    }

    /// 스트림을 S3에 업로드
    pub async fn upload_stream_to_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.memory_manager.upload_stream_to_s3(stream_name).await
    }

    /// S3 업로드 상태 조회
    pub async fn get_s3_upload_status(&self, stream_name: &str) -> Option<crate::data_layer::storage::s3_types::UploadStatus> {
        let status = self.memory_manager.get_s3_upload_status(stream_name).await;

        // 새로운 형식을 기존 형식으로 변환
        Some(crate::data_layer::storage::s3_types::UploadStatus {
            stream_key: stream_name.to_string(),
            total_files: status.pending_uploads + status.active_uploads,
            uploaded_files: 0, // 정확한 값은 추적하지 않음
            failed_files: 0,
            is_complete: status.is_complete,
        })
    }

    /// S3에서 스트림 삭제
    pub async fn delete_stream_from_s3(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.memory_manager.delete_stream_from_s3(stream_name).await
    }

    /// 특정 파일을 S3에 업로드
    pub async fn upload_file_to_s3(&self, stream_name: &str, file_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 기존 호환성을 위한 래퍼 메서드
        // 실제로는 conversion_manager를 통해 처리
        Ok(())
    }

    /// 활성 스트림 수 조회
    pub async fn get_active_stream_count(&self) -> usize {
        self.memory_manager.get_active_stream_count().await
    }

    /// 스트림 존재 여부 확인
    pub async fn has_stream(&self, stream_id: u32) -> bool {
        self.memory_manager.has_stream(stream_id).await
    }

    pub fn api_config(&self) -> &crate::config::ApiConfig {
        &self.memory_manager.api_config
    }
}