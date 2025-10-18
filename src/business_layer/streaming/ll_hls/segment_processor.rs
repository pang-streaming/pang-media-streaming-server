use std::path::PathBuf;
use tokio::fs;
use super::segment_types::{SegmentInfo, PartInfo, SegmentConfig};

/// 세그먼트 프로세서
pub struct SegmentProcessor;

impl SegmentProcessor {
    /// 세그먼트 파일 생성
    pub async fn create_segment_file(
        output_dir: &PathBuf,
        stream_id: &str,
        sequence: u64,
        duration: f64,
    ) -> Result<SegmentInfo, Box<dyn std::error::Error + Send + Sync>> {
        let filename = format!("segment_{:06}.m4s", sequence);
        let file_path = output_dir.join(stream_id).join(&filename);
        
        // 디렉토리 생성
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        // 빈 파일 생성 (실제로는 FFmpeg가 생성)
        fs::write(&file_path, b"").await?;
        
        let metadata = fs::metadata(&file_path).await?;
        
        Ok(SegmentInfo {
            file_path,
            duration,
            size: metadata.len(),
            created_at: chrono::Utc::now(),
        })
    }

    /// 파트 파일 생성
    pub async fn create_part_file(
        output_dir: &PathBuf,
        stream_id: &str,
        segment_sequence: u64,
        part_sequence: u64,
        duration: f64,
    ) -> Result<PartInfo, Box<dyn std::error::Error + Send + Sync>> {
        let filename = format!("segment_{:06}_part_{:03}.m4s", segment_sequence, part_sequence);
        let file_path = output_dir.join(stream_id).join(&filename);
        
        // 디렉토리 생성
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        // 빈 파일 생성 (실제로는 FFmpeg가 생성)
        fs::write(&file_path, b"").await?;
        
        let metadata = fs::metadata(&file_path).await?;
        
        Ok(PartInfo {
            file_path,
            duration,
            size: metadata.len(),
            created_at: chrono::Utc::now(),
            is_independent: part_sequence % 10 == 0, // 10번째 파트마다 independent
        })
    }

    /// 세그먼트 정리
    pub async fn cleanup_old_segments(
        segments: &mut Vec<SegmentInfo>,
        max_segments: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if segments.len() > max_segments as usize {
            let remove_count = segments.len() - max_segments as usize;
            let to_remove = segments.drain(0..remove_count).collect::<Vec<_>>();
            
            for segment in to_remove {
                if let Err(e) = fs::remove_file(&segment.file_path).await {
                    eprintln!("Failed to remove segment file {:?}: {}", segment.file_path, e);
                }
            }
        }
        Ok(())
    }

    /// 세그먼트 경로 조회
    pub fn get_segment_path(
        output_dir: &PathBuf,
        stream_id: &str,
        filename: &str,
    ) -> Option<PathBuf> {
        let path = output_dir.join(stream_id).join(filename);
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }
}
