use chrono::{DateTime, Utc};
use serde::Serialize;

/// 스트림 메트릭 정보
#[derive(Debug, Clone, Serialize)]
pub struct StreamMetrics {
    pub stream_id: String,
    pub start_time: DateTime<Utc>,
    pub total_segments: u64,
    pub total_parts: u64,
    pub average_segment_duration: f64,
    pub average_part_duration: f64,
    pub total_bytes: u64,
    pub current_bitrate: u32,
    pub dropped_segments: u64,
    pub last_segment_time: Option<DateTime<Utc>>,
    pub latency_ms: f64,
}

/// 서버 메트릭 정보
#[derive(Debug, Clone, Serialize)]
pub struct ServerMetrics {
    pub active_streams: u32,
    pub total_connections: u64,
    pub total_bytes_served: u64,
    pub average_latency_ms: f64,
    pub uptime_seconds: u64,
    pub start_time: DateTime<Utc>,
}

/// 지연시간 측정 데이터
#[derive(Debug, Clone)]
pub struct LatencyMeasurement {
    pub timestamp: DateTime<Utc>,
    pub latency_ms: f64,
    pub segment_sequence: u64,
    pub part_sequence: Option<u64>,
}

/// 지연시간 트렌드
#[derive(Debug, Clone, PartialEq)]
pub enum LatencyTrend {
    Increasing,
    Decreasing,
    Stable,
}

/// 최적화 제안
#[derive(Debug, Clone, PartialEq)]
pub enum OptimizationSuggestion {
    ReduceSegmentDuration,
    ReducePartDuration,
    EnableServerPush,
    CheckNetworkConditions,
    LatencyImproving,
}

impl std::fmt::Display for OptimizationSuggestion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizationSuggestion::ReduceSegmentDuration => {
                write!(f, "세그먼트 지속시간을 줄이는 것을 고려하세요")
            }
            OptimizationSuggestion::ReducePartDuration => {
                write!(f, "파트 지속시간을 줄이는 것을 고려하세요")
            }
            OptimizationSuggestion::EnableServerPush => {
                write!(f, "서버 푸시를 활성화하는 것을 고려하세요")
            }
            OptimizationSuggestion::CheckNetworkConditions => {
                write!(f, "네트워크 상태를 확인하세요")
            }
            OptimizationSuggestion::LatencyImproving => {
                write!(f, "지연시간이 개선되고 있습니다")
            }
        }
    }
}
