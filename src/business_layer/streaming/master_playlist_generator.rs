use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// 마스터 플레이리스트 생성기
pub struct MasterPlaylistGenerator {
    bucket_url: String,
}

/// 스트림 메타데이터
#[derive(Clone, Debug)]
pub struct StreamMetadata {
    pub stream_name: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub is_live: bool,
    pub bitrate: u32,
    pub resolution: String,
    pub codecs: String,
    pub frame_rate: f32,
}

impl MasterPlaylistGenerator {
    pub fn new(bucket_url: String) -> Self {
        Self { bucket_url }
    }

    /// 라이브 스트림용 마스터 플레이리스트 생성
    pub fn generate_live_master(
        &self,
        stream_name: &str,
        variants: Vec<StreamVariant>,
    ) -> String {
        let mut playlist = String::new();

        // HLS 버전
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n\n");

        // 각 변형 스트림 추가
        for variant in variants {
            // EXT-X-STREAM-INF 태그
            playlist.push_str(&format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={},FRAME-RATE={:.2},CODECS=\"{}\",NAME=\"{}\"\n",
                variant.bandwidth,
                variant.resolution,
                variant.frame_rate,
                variant.codecs,
                variant.name
            ));

            // 변형 플레이리스트 URL
            let variant_url = format!(
                "{}/{}/playlist.m3u8",
                self.bucket_url,
                stream_name
            );
            playlist.push_str(&format!("{}\n", variant_url));
        }

        playlist
    }

    /// VOD (리플레이)용 마스터 플레이리스트 생성
    pub fn generate_vod_master(
        &self,
        stream_name: &str,
        metadata: &StreamMetadata,
    ) -> String {
        let mut playlist = String::new();

        // HLS 버전
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");

        // VOD 플레이리스트임을 명시
        playlist.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");

        // 스트림 시작/종료 시간 메타데이터 추가
        if let Some(end_time) = metadata.end_time {
            let duration = (end_time - metadata.start_time).num_seconds();
            playlist.push_str(&format!(
                "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                metadata.start_time.to_rfc3339()
            ));
            playlist.push_str(&format!(
                "# Stream Duration: {} seconds\n",
                duration
            ));
        }

        playlist.push_str("\n");

        // 스트림 정보 추가
        playlist.push_str(&format!(
            "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={},FRAME-RATE={:.2},CODECS=\"{}\",NAME=\"Replay\"\n",
            metadata.bitrate,
            metadata.resolution,
            metadata.frame_rate,
            metadata.codecs
        ));

        // VOD 플레이리스트 URL
        let vod_url = format!(
            "{}/{}/vod_playlist.m3u8",
            self.bucket_url,
            stream_name
        );
        playlist.push_str(&format!("{}\n", vod_url));

        playlist
    }

    /// 여러 품질의 스트림을 위한 마스터 플레이리스트 생성
    pub fn generate_multi_bitrate_master(
        &self,
        stream_name: &str,
        variants: Vec<StreamVariant>,
        is_vod: bool,
    ) -> String {
        let mut playlist = String::new();

        // HLS 버전
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");

        if is_vod {
            playlist.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");
        }

        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n\n");

        // 각 비트레이트 변형 추가
        for variant in variants {
            playlist.push_str(&format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={},FRAME-RATE={:.2},CODECS=\"{}\",NAME=\"{}\"\n",
                variant.bandwidth,
                variant.resolution,
                variant.frame_rate,
                variant.codecs,
                variant.name
            ));

            let playlist_file = if is_vod {
                format!("vod_{}p.m3u8", variant.height)
            } else {
                format!("live_{}p.m3u8", variant.height)
            };

            let variant_url = format!(
                "{}/{}/{}",
                self.bucket_url,
                stream_name,
                playlist_file
            );
            playlist.push_str(&format!("{}\n", variant_url));
        }

        playlist
    }
}

/// 스트림 변형 (다양한 품질)
#[derive(Clone, Debug)]
pub struct StreamVariant {
    pub name: String,
    pub bandwidth: u32,
    pub resolution: String,
    pub width: u32,
    pub height: u32,
    pub frame_rate: f32,
    pub codecs: String,
}

impl StreamVariant {
    /// 1080p 변형 생성
    pub fn create_1080p() -> Self {
        Self {
            name: "1080p".to_string(),
            bandwidth: 5000000,
            resolution: "1920x1080".to_string(),
            width: 1920,
            height: 1080,
            frame_rate: 60.0,
            codecs: "avc1.640028,mp4a.40.2".to_string(),
        }
    }

    /// 720p 변형 생성
    pub fn create_720p() -> Self {
        Self {
            name: "720p".to_string(),
            bandwidth: 3000000,
            resolution: "1280x720".to_string(),
            width: 1280,
            height: 720,
            frame_rate: 60.0,
            codecs: "avc1.64001f,mp4a.40.2".to_string(),
        }
    }

    /// 480p 변형 생성
    pub fn create_480p() -> Self {
        Self {
            name: "480p".to_string(),
            bandwidth: 1500000,
            resolution: "854x480".to_string(),
            width: 854,
            height: 480,
            frame_rate: 30.0,
            codecs: "avc1.64001e,mp4a.40.2".to_string(),
        }
    }
}

/// 리플레이 매니저
pub struct ReplayManager {
    completed_streams: HashMap<String, StreamMetadata>,
}

impl ReplayManager {
    pub fn new() -> Self {
        Self {
            completed_streams: HashMap::new(),
        }
    }

    /// 스트림 완료 처리
    pub fn mark_stream_completed(&mut self, stream_name: String, metadata: StreamMetadata) {
        let mut completed_metadata = metadata;
        completed_metadata.is_live = false;
        completed_metadata.end_time = Some(Utc::now());

        self.completed_streams.insert(stream_name, completed_metadata);
    }

    /// 완료된 스트림 메타데이터 조회
    pub fn get_completed_stream(&self, stream_name: &str) -> Option<&StreamMetadata> {
        self.completed_streams.get(stream_name)
    }

    /// 모든 리플레이 가능한 스트림 목록
    pub fn list_replay_streams(&self) -> Vec<&StreamMetadata> {
        self.completed_streams.values().collect()
    }
}