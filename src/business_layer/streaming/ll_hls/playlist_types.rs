use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// HLS 세그먼트 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub uri: String,
    pub duration: f64,
    pub sequence: u64,
    pub is_independent: bool,
}

/// HLS 파트 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Part {
    pub uri: String,
    pub duration: f64,
    pub is_independent: bool,
}

/// 플레이리스트 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlaylistType {
    Event,
    Live,
}

/// 스트림 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamState {
    pub stream_id: String,
    pub sequence_number: u64,
    pub target_duration: f64,
    pub segments: Vec<Segment>,
    pub last_updated: DateTime<Utc>,
    pub playlist_type: PlaylistType,
}

/// LL-HLS 플레이리스트 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaylistConfig {
    pub target_duration: f64,
    pub part_duration: f64,
    pub max_segments: u32,
    pub max_parts: u32,
    pub enable_server_push: bool,
    pub enable_preload_hint: bool,
}
