use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

use super::segment_types::{SegmentInfo, PartInfo, SegmentConfig};
use super::segment_processor::SegmentProcessor;

/// LL-HLS 세그먼트 관리자
pub struct LLHLSSegmentManager {
    segments: Arc<RwLock<HashMap<String, Vec<SegmentInfo>>>>,
    output_dir: PathBuf,
    config: SegmentConfig,
}

impl LLHLSSegmentManager {
    /// 새로운 세그먼트 관리자 생성
    pub fn new(output_dir: String, hls_config: crate::config::HlsConfig) -> Self {
        let config = SegmentConfig {
            target_duration: hls_config.segment_duration,
            part_duration: hls_config.part_duration,
            max_segments: hls_config.max_segments,
            max_parts: hls_config.max_parts,
        };

        Self {
            segments: Arc::new(RwLock::new(HashMap::new())),
            output_dir: PathBuf::from(output_dir),
            config,
        }
    }

    /// 스트림 초기화
    pub async fn initialize_stream(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut segments = self.segments.write().await;
        segments.insert(stream_id.to_string(), Vec::new());
        Ok(())
    }

    /// 세그먼트 생성
    pub async fn create_segment(
        &self,
        stream_id: &str,
        sequence: u64,
        duration: f64,
    ) -> Result<SegmentInfo, Box<dyn std::error::Error + Send + Sync>> {
        let segment_info = SegmentProcessor::create_segment_file(
            &self.output_dir,
            stream_id,
            sequence,
            duration,
        ).await?;

        // 세그먼트 목록에 추가
        {
            let mut segments = self.segments.write().await;
            if let Some(stream_segments) = segments.get_mut(stream_id) {
                stream_segments.push(segment_info.clone());
                SegmentProcessor::cleanup_old_segments(stream_segments, self.config.max_segments).await?;
            }
        }

        Ok(segment_info)
    }

    /// 파트 생성
    async fn create_parts(
        &self,
        stream_id: &str,
        segment_sequence: u64,
        segment_duration: f64,
    ) -> Result<Vec<PartInfo>, Box<dyn std::error::Error + Send + Sync>> {
        let mut parts = Vec::new();
        let part_count = (segment_duration / self.config.part_duration).ceil() as u64;
        
        for part_sequence in 0..part_count {
            let part_duration = if part_sequence == part_count - 1 {
                segment_duration - (part_sequence as f64 * self.config.part_duration)
            } else {
                self.config.part_duration
            };

            let part_info = SegmentProcessor::create_part_file(
                &self.output_dir,
                stream_id,
                segment_sequence,
                part_sequence,
                part_duration,
            ).await?;

            parts.push(part_info);
        }

        Ok(parts)
    }

    /// 오래된 세그먼트 정리
    pub async fn cleanup_old_segments(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut segments = self.segments.write().await;
        if let Some(stream_segments) = segments.get_mut(stream_id) {
            SegmentProcessor::cleanup_old_segments(stream_segments, self.config.max_segments).await?;
        }
        Ok(())
    }

    /// 세그먼트 경로 조회
    pub async fn get_segment_path(&self, stream_id: &str, filename: &str) -> Option<PathBuf> {
        SegmentProcessor::get_segment_path(&self.output_dir, stream_id, filename)
    }

    /// 스트림 제거
    pub async fn remove_stream(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut segments = self.segments.write().await;
        segments.remove(stream_id);
        Ok(())
    }
}