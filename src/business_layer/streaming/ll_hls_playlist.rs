use chrono::{DateTime, Utc};
use m3u8_rs::{MediaPlaylist, MediaSegment};
use std::collections::VecDeque;

/// LL-HLS 플레이리스트 생성기 (m3u8-rs 기반)
#[derive(Debug)]
pub struct LLHlsPlaylistGenerator {
    /// 스트림 이름
    stream_name: String,
    /// 기본 URL (S3 버킷 URL)
    base_url: String,
    /// 타겟 듀레이션 (초)
    target_duration: f64,
    /// 파트 타겟 듀레이션 (초)
    part_target: f64,
    /// 파트 홀드백 (초)
    part_hold_back: f64,
    /// 미디어 시퀀스 번호
    media_sequence: u64,
    /// 불연속 시퀀스 번호
    discontinuity_sequence: u64,
    /// 스트림 시작 시간
    start_date: DateTime<Utc>,
    /// 세그먼트 목록
    segments: VecDeque<LLHlsSegment>,
    /// 최대 세그먼트 수 (슬라이딩 윈도우)
    max_segments: usize,
    /// init 세그먼트 파일명
    init_segment: Option<String>,
    /// 화질 이름 (예: "1080p")
    quality_name: String,
    /// 비트레이트 (bps)
    bitrate: u64,
}

/// LL-HLS 세그먼트
#[derive(Clone, Debug)]
pub struct LLHlsSegment {
    /// 세그먼트 번호
    pub sequence: u64,
    /// 세그먼트 듀레이션 (초)
    pub duration: f64,
    /// 세그먼트 파일명
    pub filename: String,
    /// 프로그램 시간
    pub program_date_time: DateTime<Utc>,
    /// 파트 목록
    pub parts: Vec<LLHlsPart>,
    /// 비트레이트 (bps)
    pub bitrate: u64,
    /// 완료 여부 (모든 파트가 추가됨)
    pub is_complete: bool,
}

/// LL-HLS 파트 (부분 세그먼트)
#[derive(Clone, Debug)]
pub struct LLHlsPart {
    /// 파트 인덱스 (세그먼트 내에서)
    pub index: usize,
    /// 파트 듀레이션 (초)
    pub duration: f64,
    /// 파트 파일명
    pub filename: String,
    /// 독립 재생 가능 여부 (키프레임 포함)
    pub independent: bool,
}

/// 렌디션 리포트 정보
#[derive(Clone, Debug)]
pub struct RenditionReport {
    /// 상대 URI
    pub uri: String,
    /// 마지막 미디어 시퀀스 번호
    pub last_msn: u64,
    /// 마지막 파트 인덱스
    pub last_part: usize,
}

impl LLHlsPlaylistGenerator {
    /// 새로운 LL-HLS 플레이리스트 생성기 생성
    pub fn new(
        stream_name: String,
        base_url: String,
        quality_name: String,
        bitrate: u64,
    ) -> Self {
        Self {
            stream_name,
            base_url,
            target_duration: 4.0,
            part_target: 1.0,
            part_hold_back: 3.0,
            media_sequence: 0,
            discontinuity_sequence: 0,
            start_date: Utc::now(),
            segments: VecDeque::new(),
            max_segments: 10,
            init_segment: None,
            quality_name,
            bitrate,
        }
    }

    /// 타겟 듀레이션 설정
    pub fn with_target_duration(mut self, duration: f64) -> Self {
        self.target_duration = duration;
        self
    }

    /// 파트 타겟 듀레이션 설정
    pub fn with_part_target(mut self, duration: f64) -> Self {
        self.part_target = duration;
        self.part_hold_back = duration * 3.0;
        self
    }

    /// 최대 세그먼트 수 설정
    pub fn with_max_segments(mut self, max: usize) -> Self {
        self.max_segments = max;
        self
    }

    /// init 세그먼트 설정
    pub fn set_init_segment(&mut self, filename: String) {
        self.init_segment = Some(filename);
    }

    /// 새로운 세그먼트 시작
    pub fn start_segment(&mut self) -> u64 {
        let sequence = self.media_sequence + self.segments.len() as u64;

        let segment = LLHlsSegment {
            sequence,
            duration: 0.0,
            filename: String::new(),
            program_date_time: Utc::now(),
            parts: Vec::new(),
            bitrate: self.bitrate,
            is_complete: false,
        };

        self.segments.push_back(segment);

        // 최대 세그먼트 수 초과시 오래된 것 제거
        while self.segments.len() > self.max_segments + 1 {
            self.segments.pop_front();
            self.media_sequence += 1;
        }

        sequence
    }

    /// 현재 세그먼트에 파트 추가
    pub fn add_part(&mut self, part: LLHlsPart) {
        if let Some(segment) = self.segments.back_mut() {
            segment.duration += part.duration;
            segment.parts.push(part);
        }
    }

    /// 현재 세그먼트 완료
    pub fn complete_segment(&mut self, filename: String, total_duration: f64, bitrate: u64) {
        if let Some(segment) = self.segments.back_mut() {
            segment.filename = filename;
            segment.duration = total_duration;
            segment.bitrate = bitrate;
            segment.is_complete = true;
        }
    }

    /// 미디어 플레이리스트 생성 (m3u8-rs 기반 + 커스텀 LL-HLS 태그)
    pub fn generate_media_playlist(&self, rendition_reports: Option<Vec<RenditionReport>>) -> String {
        let mut output = String::new();

        // 헤더
        output.push_str("#EXTM3U\n");
        output.push_str("#EXT-X-VERSION:10\n");
        output.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");
        output.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", self.target_duration.ceil() as u32));

        // LL-HLS 서버 컨트롤
        output.push_str(&format!(
            "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.6}\n",
            self.part_hold_back
        ));

        // 파트 정보
        output.push_str(&format!(
            "#EXT-X-PART-INF:PART-TARGET={:.6}\n",
            self.part_target
        ));

        // 미디어 시퀀스
        output.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", self.media_sequence));
        output.push_str(&format!("#EXT-X-DISCONTINUITY-SEQUENCE:{}\n", self.discontinuity_sequence));

        // 날짜 범위
        output.push_str(&format!(
            "#EXT-X-DATERANGE:ID=\"stream-start\",START-DATE=\"{}\"\n",
            self.start_date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        ));

        output.push('\n');

        // init 세그먼트
        if let Some(ref init) = self.init_segment {
            output.push_str(&format!("#EXT-X-MAP:URI=\"{}\"\n", init));
        }

        // 세그먼트들
        for segment in &self.segments {
            // 프로그램 시간
            output.push_str(&format!(
                "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                segment.program_date_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ));

            if segment.is_complete {
                // 완료된 세그먼트: 파트들과 전체 세그먼트 모두 표시
                for part in &segment.parts {
                    output.push_str(&format!(
                        "#EXT-X-PART:DURATION={:.6},URI=\"{}\",INDEPENDENT=YES\n",
                        part.duration,
                        part.filename,
                    ));
                }

                // 전체 세그먼트
                output.push_str(&format!("#EXTINF:{:.6},\n", segment.duration));
                output.push_str(&format!("{}\n", segment.filename));
            } else {
                // 진행 중인 세그먼트: 파트들만 표시
                for part in &segment.parts {
                    output.push_str(&format!(
                        "#EXT-X-PART:DURATION={:.6},URI=\"{}\",INDEPENDENT=YES\n",
                        part.duration,
                        part.filename,
                    ));
                }

                // 프리로드 힌트 (다음 파트)
                if !segment.parts.is_empty() {
                    let next_part_index = segment.parts.len();
                    let next_part_filename = self.generate_part_filename(segment.sequence, next_part_index);
                    output.push_str(&format!(
                        "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"{}\"\n",
                        next_part_filename
                    ));
                }
            }
        }

        // 렌디션 리포트
        if let Some(reports) = rendition_reports {
            for report in reports {
                output.push_str(&format!(
                    "#EXT-X-RENDITION-REPORT:URI=\"{}\",LAST-MSN={},LAST-PART={}\n",
                    report.uri,
                    report.last_msn,
                    report.last_part
                ));
            }
        }

        output
    }

    /// m3u8-rs MediaPlaylist 구조체 생성 (호환성용)
    pub fn to_media_playlist(&self) -> MediaPlaylist {
        let mut segments = Vec::new();

        for seg in &self.segments {
            if seg.is_complete {
                // DateTime<Utc>를 DateTime<FixedOffset>으로 변환
                let fixed_offset_dt = seg.program_date_time.with_timezone(&chrono::FixedOffset::east_opt(0).unwrap());

                let media_seg = MediaSegment {
                    uri: format!(
                        "{}?type=hls&bitrate={}&filetype=.m4v",
                        seg.filename, seg.bitrate
                    ),
                    duration: seg.duration as f32,
                    title: None,
                    byte_range: None,
                    discontinuity: false,
                    key: None,
                    map: None,
                    program_date_time: Some(fixed_offset_dt),
                    daterange: None,
                    unknown_tags: vec![],
                };
                segments.push(media_seg);
            }
        }

        MediaPlaylist {
            version: Some(10),
            target_duration: self.target_duration as u64,
            media_sequence: self.media_sequence,
            discontinuity_sequence: self.discontinuity_sequence,
            end_list: false,
            playlist_type: None,
            i_frames_only: false,
            start: None,
            independent_segments: true,
            segments,
            unknown_tags: vec![],
        }
    }

    /// 파트 파일명 생성
    pub fn generate_part_filename(&self, sequence: u64, part_index: usize) -> String {
        format!(
            "{}_{}_{}_{}",
            self.quality_name,
            self.stream_name,
            sequence,
            part_index
        )
    }

    /// 세그먼트 파일명 생성
    pub fn generate_segment_filename(&self, sequence: u64) -> String {
        format!(
            "{}_{}_{}",
            self.quality_name,
            self.stream_name,
            sequence
        )
    }

    /// 현재 미디어 시퀀스 번호 조회
    pub fn current_media_sequence(&self) -> u64 {
        self.media_sequence + self.segments.len().saturating_sub(1) as u64
    }

    /// 현재 파트 인덱스 조회
    pub fn current_part_index(&self) -> usize {
        self.segments
            .back()
            .map(|s| s.parts.len().saturating_sub(1))
            .unwrap_or(0)
    }

    /// 스트림 이름 조회
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// 화질 이름 조회
    pub fn quality_name(&self) -> &str {
        &self.quality_name
    }

    /// 미디어 시퀀스 설정
    pub fn set_media_sequence(&mut self, seq: u64) {
        self.media_sequence = seq;
    }

    /// 세그먼트 수 조회
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// 마지막 세그먼트 조회
    pub fn last_segment(&self) -> Option<&LLHlsSegment> {
        self.segments.back()
    }

    /// 마지막 완료된 세그먼트 조회
    pub fn last_complete_segment(&self) -> Option<&LLHlsSegment> {
        self.segments.iter().rev().find(|s| s.is_complete)
    }
}

/// LL-HLS 세그먼트 정보 (파일명 파싱용)
#[derive(Clone, Debug)]
pub struct SegmentInfo {
    pub quality: String,
    pub hash: String,
    pub timestamp: i64,
    pub frame_number: u64,
    pub stream_index: u32,
    pub sequence: u64,
    pub part_index: Option<usize>,
}

impl SegmentInfo {
    /// 파일명에서 세그먼트 정보 파싱
    /// 예시: 1080p_3513129241_1764157633288_11139_0_2673_0.m4v
    pub fn parse_filename(filename: &str) -> Option<Self> {
        let name = filename.trim_end_matches(".m4v").trim_end_matches(".m4s");
        let parts: Vec<&str> = name.split('_').collect();

        if parts.len() >= 6 {
            let quality = parts[0].to_string();
            let hash = parts[1].to_string();
            let timestamp = parts[2].parse().ok()?;
            let frame_number = parts[3].parse().ok()?;
            let stream_index = parts[4].parse().ok()?;
            let sequence = parts[5].parse().ok()?;
            let part_index = parts.get(6).and_then(|s| s.parse().ok());

            Some(Self {
                quality,
                hash,
                timestamp,
                frame_number,
                stream_index,
                sequence,
                part_index,
            })
        } else {
            None
        }
    }

    /// 파일명 생성
    pub fn to_filename(&self) -> String {
        if let Some(part_idx) = self.part_index {
            format!(
                "{}_{}_{}_{}_{}_{}_{}.m4v",
                self.quality,
                self.hash,
                self.timestamp,
                self.frame_number,
                self.stream_index,
                self.sequence,
                part_idx
            )
        } else {
            format!(
                "{}_{}_{}_{}_{}_{}.m4v",
                self.quality,
                self.hash,
                self.timestamp,
                self.frame_number,
                self.stream_index,
                self.sequence
            )
        }
    }

    /// 파트 여부 확인
    pub fn is_part(&self) -> bool {
        self.part_index.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_info_parse() {
        let filename = "1080p_3513129241_1764157633288_11139_0_2673_0.m4v";
        let info = SegmentInfo::parse_filename(filename).unwrap();

        assert_eq!(info.quality, "1080p");
        assert_eq!(info.hash, "3513129241");
        assert_eq!(info.timestamp, 1764157633288);
        assert_eq!(info.frame_number, 11139);
        assert_eq!(info.stream_index, 0);
        assert_eq!(info.sequence, 2673);
        assert_eq!(info.part_index, Some(0));
    }

    #[test]
    fn test_segment_info_parse_no_part() {
        let filename = "1080p_3513129241_1764157633288_11139_0_2673.m4v";
        let info = SegmentInfo::parse_filename(filename).unwrap();

        assert_eq!(info.part_index, None);
    }

    #[test]
    fn test_playlist_generation() {
        let mut generator = LLHlsPlaylistGenerator::new(
            "test_stream".to_string(),
            "https://bucket.s3.amazonaws.com".to_string(),
            "1080p".to_string(),
            5000000,
        );

        generator.set_init_segment("init.mp4".to_string());

        // 첫 번째 세그먼트
        generator.start_segment();
        generator.add_part(LLHlsPart {
            index: 0,
            duration: 1.0,
            filename: "1080p_0_0.m4v".to_string(),
            independent: true,
        });
        generator.add_part(LLHlsPart {
            index: 1,
            duration: 1.0,
            filename: "1080p_0_1.m4v".to_string(),
            independent: false,
        });
        generator.complete_segment("1080p_0.m4v".to_string(), 2.0, 5000000);

        let playlist = generator.generate_media_playlist(None);

        assert!(playlist.contains("#EXTM3U"));
        assert!(playlist.contains("#EXT-X-VERSION:10"));
        assert!(playlist.contains("#EXT-X-SERVER-CONTROL"));
        assert!(playlist.contains("#EXT-X-PART-INF"));
        assert!(playlist.contains("#EXT-X-PART:DURATION=1.000000"));
    }
}
