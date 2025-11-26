use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::Utc;
use log::{info, warn};

use super::ll_hls_playlist::{
    LLHlsPlaylistGenerator, LLHlsPart, RenditionReport, SegmentInfo
};
use crate::data_layer::storage::memory_s3_uploader::{MemoryS3Uploader, MemorySegment};

/// LL-HLS 스트림 상태
#[derive(Clone, Debug)]
pub struct LLHlsStreamState {
    /// 플레이리스트 생성기
    pub generator: Arc<RwLock<LLHlsPlaylistGenerator>>,
    /// 현재 세그먼트 파트 카운터
    pub current_part_count: usize,
    /// 파트 타겟 듀레이션 (초)
    pub part_target: f64,
    /// 세그먼트 타겟 듀레이션 (초)
    pub segment_target: f64,
    /// 스트림 시작 시간
    pub start_time: chrono::DateTime<Utc>,
    /// 마지막 업데이트 시간
    pub last_update: chrono::DateTime<Utc>,
}

/// LL-HLS 매니저
pub struct LLHlsManager {
    /// 스트림별 상태
    streams: Arc<RwLock<HashMap<String, LLHlsStreamState>>>,
    /// S3 업로더
    s3_uploader: Arc<MemoryS3Uploader>,
    /// S3 버킷
    bucket: String,
    /// 기본 URL
    base_url: String,
}

impl LLHlsManager {
    /// 새로운 LL-HLS 매니저 생성
    pub fn new(s3_uploader: Arc<MemoryS3Uploader>, bucket: String) -> Self {
        let base_url = format!("https://{}.s3.ap-northeast-2.amazonaws.com", bucket);
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            s3_uploader,
            bucket,
            base_url,
        }
    }

    /// 스트림 시작
    pub async fn start_stream(
        &self,
        stream_name: &str,
        quality_name: &str,
        bitrate: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let generator = LLHlsPlaylistGenerator::new(
            stream_name.to_string(),
            self.base_url.clone(),
            quality_name.to_string(),
            bitrate,
        )
        .with_target_duration(4.0)
        .with_part_target(1.0)
        .with_max_segments(10);

        let state = LLHlsStreamState {
            generator: Arc::new(RwLock::new(generator)),
            current_part_count: 0,
            part_target: 1.0,
            segment_target: 4.0,
            start_time: Utc::now(),
            last_update: Utc::now(),
        };

        let mut streams = self.streams.write().await;
        streams.insert(stream_name.to_string(), state);

        info!("LL-HLS stream started: {} ({})", stream_name, quality_name);
        Ok(())
    }

    /// 스트림 중지
    pub async fn stop_stream(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut streams = self.streams.write().await;
        if streams.remove(stream_name).is_some() {
            info!("LL-HLS stream stopped: {}", stream_name);
        }
        Ok(())
    }

    /// init 세그먼트 설정
    pub async fn set_init_segment(&self, stream_name: &str, filename: &str) {
        let streams = self.streams.read().await;
        if let Some(state) = streams.get(stream_name) {
            let mut generator = state.generator.write().await;
            generator.set_init_segment(filename.to_string());
            info!("LL-HLS init segment set for {}: {}", stream_name, filename);
        }
    }

    /// 세그먼트 파일 처리
    pub async fn process_segment_file(
        &self,
        stream_name: &str,
        filename: &str,
        data: Vec<u8>,
        duration: f64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let streams = self.streams.read().await;
        let state = match streams.get(stream_name) {
            Some(s) => s,
            None => {
                warn!("LL-HLS stream not found: {}", stream_name);
                return Ok(());
            }
        };

        // 파일명에서 세그먼트 정보 파싱
        let seg_info = SegmentInfo::parse_filename(filename);

        let mut generator = state.generator.write().await;

        if let Some(info) = seg_info {
            if info.is_part() {
                // 파트 세그먼트
                let part = LLHlsPart {
                    index: info.part_index.unwrap_or(0),
                    duration,
                    filename: filename.to_string(),
                    independent: info.part_index == Some(0), // 첫 번째 파트만 독립
                };
                generator.add_part(part);
                info!("LL-HLS part added: {} (duration: {:.3}s)", filename, duration);
            } else {
                // 전체 세그먼트 - 현재 세그먼트 완료 처리
                let bitrate = self.calculate_bitrate(&data, duration);
                generator.complete_segment(filename.to_string(), duration, bitrate);

                // 다음 세그먼트 시작
                generator.start_segment();
                info!("LL-HLS segment completed: {} (duration: {:.3}s, bitrate: {})", filename, duration, bitrate);
            }
        } else {
            // 파싱 실패 - 단순 세그먼트로 처리
            let bitrate = self.calculate_bitrate(&data, duration);
            generator.start_segment();
            generator.complete_segment(filename.to_string(), duration, bitrate);
            info!("LL-HLS segment added (simple): {}", filename);
        }

        drop(generator);

        // 플레이리스트 업데이트 및 업로드
        self.update_playlist(stream_name).await?;

        Ok(())
    }

    /// 파트 세그먼트 추가 (수동)
    pub async fn add_part(
        &self,
        stream_name: &str,
        filename: &str,
        duration: f64,
        independent: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let streams = self.streams.read().await;
        let state = match streams.get(stream_name) {
            Some(s) => s,
            None => return Ok(()),
        };

        let mut generator = state.generator.write().await;
        let part_index = generator.current_part_index() + 1;

        let part = LLHlsPart {
            index: part_index,
            duration,
            filename: filename.to_string(),
            independent,
        };
        generator.add_part(part);

        drop(generator);

        // 플레이리스트 업데이트
        self.update_playlist(stream_name).await?;

        Ok(())
    }

    /// 세그먼트 완료
    pub async fn complete_segment(
        &self,
        stream_name: &str,
        filename: &str,
        duration: f64,
        bitrate: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let streams = self.streams.read().await;
        let state = match streams.get(stream_name) {
            Some(s) => s,
            None => return Ok(()),
        };

        let mut generator = state.generator.write().await;
        generator.complete_segment(filename.to_string(), duration, bitrate);
        generator.start_segment();

        drop(generator);

        // 플레이리스트 업데이트
        self.update_playlist(stream_name).await?;

        Ok(())
    }

    /// 플레이리스트 업데이트 및 S3 업로드
    pub async fn update_playlist(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let streams = self.streams.read().await;
        let state = match streams.get(stream_name) {
            Some(s) => s,
            None => return Ok(()),
        };

        let generator = state.generator.read().await;

        // 렌디션 리포트 (다른 화질 없으므로 빈 배열)
        let rendition_reports: Option<Vec<RenditionReport>> = None;

        // 플레이리스트 생성
        let playlist_content = generator.generate_media_playlist(rendition_reports);

        drop(generator);
        drop(streams);

        // S3에 플레이리스트 업로드
        let segment = MemorySegment {
            stream_name: stream_name.to_string(),
            file_name: "chunklist.m3u8".to_string(),
            data: playlist_content.into_bytes(),
            content_type: "application/vnd.apple.mpegurl".to_string(),
            priority: 255, // 최우선
            created_at: Utc::now(),
            retry_count: 0,
        };
        self.s3_uploader.queue_segment(segment).await;

        Ok(())
    }

    /// 비트레이트 계산
    fn calculate_bitrate(&self, data: &[u8], duration: f64) -> u64 {
        if duration > 0.0 {
            ((data.len() as f64 * 8.0) / duration) as u64
        } else {
            5000000 // 기본값 5Mbps
        }
    }

    /// 현재 플레이리스트 가져오기
    pub async fn get_playlist(&self, stream_name: &str) -> Option<String> {
        let streams = self.streams.read().await;
        let state = streams.get(stream_name)?;

        let generator = state.generator.read().await;
        Some(generator.generate_media_playlist(None))
    }

    /// 스트림 존재 여부 확인
    pub async fn has_stream(&self, stream_name: &str) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(stream_name)
    }

    /// 활성 스트림 수
    pub async fn active_stream_count(&self) -> usize {
        let streams = self.streams.read().await;
        streams.len()
    }

    /// 스트림 상태 정보
    pub async fn get_stream_info(&self, stream_name: &str) -> Option<LLHlsStreamInfo> {
        let streams = self.streams.read().await;
        let state = streams.get(stream_name)?;

        let generator = state.generator.read().await;

        Some(LLHlsStreamInfo {
            stream_name: stream_name.to_string(),
            quality_name: generator.quality_name().to_string(),
            media_sequence: generator.current_media_sequence(),
            part_index: generator.current_part_index(),
            segment_count: generator.segment_count(),
            start_time: state.start_time,
            last_update: state.last_update,
        })
    }
}

/// LL-HLS 스트림 정보
#[derive(Clone, Debug)]
pub struct LLHlsStreamInfo {
    pub stream_name: String,
    pub quality_name: String,
    pub media_sequence: u64,
    pub part_index: usize,
    pub segment_count: usize,
    pub start_time: chrono::DateTime<Utc>,
    pub last_update: chrono::DateTime<Utc>,
}

/// FFmpeg에서 생성된 m3u8 파싱하여 LL-HLS 형식으로 변환
pub fn parse_ffmpeg_playlist(content: &str) -> Vec<ParsedSegment> {
    let mut segments = Vec::new();
    let mut current_duration: f64 = 0.0;
    let mut current_program_time: Option<String> = None;

    for line in content.lines() {
        if line.starts_with("#EXTINF:") {
            // 듀레이션 파싱
            if let Some(duration_str) = line.strip_prefix("#EXTINF:") {
                let duration_str = duration_str.trim_end_matches(',');
                if let Ok(duration) = duration_str.parse::<f64>() {
                    current_duration = duration;
                }
            }
        } else if line.starts_with("#EXT-X-PROGRAM-DATE-TIME:") {
            // 프로그램 시간 파싱
            if let Some(time_str) = line.strip_prefix("#EXT-X-PROGRAM-DATE-TIME:") {
                current_program_time = Some(time_str.to_string());
            }
        } else if !line.starts_with("#") && !line.is_empty() {
            // 세그먼트 파일명
            segments.push(ParsedSegment {
                filename: line.to_string(),
                duration: current_duration,
                program_date_time: current_program_time.take(),
            });
            current_duration = 0.0;
        }
    }

    segments
}

/// 파싱된 세그먼트
#[derive(Clone, Debug)]
pub struct ParsedSegment {
    pub filename: String,
    pub duration: f64,
    pub program_date_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ffmpeg_playlist() {
        let content = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PROGRAM-DATE-TIME:2025-11-26T11:46:56.621Z
#EXTINF:4.167000,
segment_0.m4s
#EXT-X-PROGRAM-DATE-TIME:2025-11-26T11:47:00.788Z
#EXTINF:4.167000,
segment_1.m4s
"#;

        let segments = parse_ffmpeg_playlist(content);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].filename, "segment_0.m4s");
        assert!((segments[0].duration - 4.167).abs() < 0.001);
        assert!(segments[0].program_date_time.is_some());
    }
}
