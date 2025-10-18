use std::path::PathBuf;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 세그먼트 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub file_path: PathBuf,
    pub duration: f64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

/// 파트 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartInfo {
    pub file_path: PathBuf,
    pub duration: f64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub is_independent: bool,
}

/// 세그먼트 관리 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentConfig {
    pub target_duration: f64,
    pub part_duration: f64,
    pub max_segments: u32,
    pub max_parts: u32,
}
