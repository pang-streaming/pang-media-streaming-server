use super::metrics_types::{StreamMetrics, ServerMetrics, LatencyMeasurement, LatencyTrend, OptimizationSuggestion};
use chrono::{DateTime, Utc};

/// 메트릭 계산 유틸리티
pub struct MetricsCalculator;

impl MetricsCalculator {
    /// 평균 세그먼트 지속시간 계산
    pub fn calculate_average_segment_duration(
        current_avg: f64,
        total_segments: u64,
        new_duration: f64,
    ) -> f64 {
        if total_segments == 0 {
            new_duration
        } else {
            let total_duration = current_avg * (total_segments - 1) as f64 + new_duration;
            total_duration / total_segments as f64
        }
    }

    /// 평균 파트 지속시간 계산
    pub fn calculate_average_part_duration(
        current_avg: f64,
        total_parts: u64,
        new_duration: f64,
    ) -> f64 {
        if total_parts == 0 {
            new_duration
        } else {
            let total_duration = current_avg * (total_parts - 1) as f64 + new_duration;
            total_duration / total_parts as f64
        }
    }

    /// 현재 비트레이트 계산
    pub fn calculate_current_bitrate(
        total_bytes: u64,
        start_time: DateTime<Utc>,
        last_segment_time: Option<DateTime<Utc>>,
    ) -> u32 {
        if let Some(last_time) = last_segment_time {
            let elapsed = (last_time - start_time).num_milliseconds() as f64 / 1000.0;
            if elapsed > 0.0 {
                (total_bytes as f64 / elapsed * 8.0) as u32
            } else {
                0
            }
        } else {
            0
        }
    }

    /// 서버 평균 지연시간 계산
    pub fn calculate_server_average_latency(
        current_avg: f64,
        active_streams: u32,
        new_latency: f64,
    ) -> f64 {
        if active_streams == 0 {
            new_latency
        } else {
            let total_latency = current_avg * active_streams as f64 + new_latency;
            total_latency / (active_streams + 1) as f64
        }
    }

    /// 지연시간 트렌드 분석
    pub fn analyze_latency_trend(measurements: &[LatencyMeasurement]) -> LatencyTrend {
        if measurements.len() < 10 {
            return LatencyTrend::Stable;
        }

        let recent = &measurements[measurements.len() - 5..];
        let older = &measurements[measurements.len() - 10..measurements.len() - 5];

        if recent.is_empty() || older.is_empty() {
            return LatencyTrend::Stable;
        }

        let recent_avg: f64 = recent.iter().map(|m| m.latency_ms).sum::<f64>() / recent.len() as f64;
        let older_avg: f64 = older.iter().map(|m| m.latency_ms).sum::<f64>() / older.len() as f64;

        let change_percent = ((recent_avg - older_avg) / older_avg) * 100.0;

        if change_percent > 10.0 {
            LatencyTrend::Increasing
        } else if change_percent < -10.0 {
            LatencyTrend::Decreasing
        } else {
            LatencyTrend::Stable
        }
    }

    /// 최적화 제안 생성
    pub fn generate_optimization_suggestions(
        avg_latency: f64,
        target_latency: f64,
        trend: LatencyTrend,
    ) -> Vec<OptimizationSuggestion> {
        let mut suggestions = Vec::new();
        
        if avg_latency > target_latency * 1.5 {
            suggestions.push(OptimizationSuggestion::ReduceSegmentDuration);
        }
        
        if avg_latency > target_latency * 2.0 {
            suggestions.push(OptimizationSuggestion::ReducePartDuration);
        }
        
        if avg_latency > target_latency * 3.0 {
            suggestions.push(OptimizationSuggestion::EnableServerPush);
        }

        match trend {
            LatencyTrend::Increasing => {
                suggestions.push(OptimizationSuggestion::CheckNetworkConditions);
            }
            LatencyTrend::Decreasing => {
                suggestions.push(OptimizationSuggestion::LatencyImproving);
            }
            LatencyTrend::Stable => {
                // 안정적인 상태
            }
        }

        suggestions
    }

    /// 최근 측정값 필터링
    pub fn filter_recent_measurements(
        measurements: &[LatencyMeasurement],
        window_seconds: u64,
    ) -> Vec<&LatencyMeasurement> {
        let cutoff_time = Utc::now() - chrono::Duration::seconds(window_seconds as i64);
        measurements
            .iter()
            .filter(|m| m.timestamp > cutoff_time)
            .collect()
    }

    /// 평균 지연시간 계산
    pub fn calculate_average_latency(measurements: &[&LatencyMeasurement]) -> Option<f64> {
        if measurements.is_empty() {
            None
        } else {
            let total_latency: f64 = measurements.iter().map(|m| m.latency_ms).sum();
            Some(total_latency / measurements.len() as f64)
        }
    }
}
