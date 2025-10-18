use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::Utc;

use super::metrics_types::{LatencyMeasurement, LatencyTrend, OptimizationSuggestion};
use super::metrics_calculator::MetricsCalculator;
use super::metrics_collector::MetricsCollector;

/// 지연시간 모니터
pub struct LatencyMonitor {
    measurements: Arc<RwLock<HashMap<String, Vec<LatencyMeasurement>>>>,
    metrics_collector: Arc<MetricsCollector>,
    target_latency_ms: f64,
}

impl LatencyMonitor {
    /// 새로운 지연시간 모니터 생성
    pub fn new(metrics_collector: Arc<MetricsCollector>, target_latency_ms: f64) -> Self {
        Self {
            measurements: Arc::new(RwLock::new(HashMap::new())),
            metrics_collector,
            target_latency_ms,
        }
    }

    /// 세그먼트 지연시간 기록
    pub async fn record_segment_latency(
        &self,
        stream_id: &str,
        segment_sequence: u64,
        latency_ms: f64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let measurement = LatencyMeasurement {
            timestamp: Utc::now(),
            latency_ms,
            segment_sequence,
            part_sequence: None,
        };

        self.add_measurement(stream_id, measurement).await?;

        // 메트릭 수집기에 지연시간 기록
        self.metrics_collector.record_latency(stream_id, latency_ms).await?;
        
        Ok(())
    }

    /// 파트 지연시간 기록
    pub async fn record_part_latency(
        &self,
        stream_id: &str,
        segment_sequence: u64,
        part_sequence: u64,
        latency_ms: f64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let measurement = LatencyMeasurement {
            timestamp: Utc::now(),
            latency_ms,
            segment_sequence,
            part_sequence: Some(part_sequence),
        };

        self.add_measurement(stream_id, measurement).await?;
        
        Ok(())
    }

    /// 측정값 추가 (내부 메서드)
    async fn add_measurement(
        &self,
        stream_id: &str,
        measurement: LatencyMeasurement,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut measurements = self.measurements.write().await;
        if let Some(stream_measurements) = measurements.get_mut(stream_id) {
            stream_measurements.push(measurement);
            
            // 최대 100개의 측정값만 유지
            if stream_measurements.len() > 100 {
                stream_measurements.remove(0);
            }
        } else {
            measurements.insert(stream_id.to_string(), vec![measurement]);
        }
        Ok(())
    }

    /// 평균 지연시간 조회
    pub async fn get_average_latency(&self, stream_id: &str, window_seconds: u64) -> Option<f64> {
        let measurements = self.measurements.read().await;
        if let Some(stream_measurements) = measurements.get(stream_id) {
            let recent_measurements = MetricsCalculator::filter_recent_measurements(
                stream_measurements,
                window_seconds,
            );
            MetricsCalculator::calculate_average_latency(&recent_measurements)
        } else {
            None
        }
    }

    /// 지연시간이 허용 가능한지 확인
    pub async fn is_latency_acceptable(&self, stream_id: &str) -> bool {
        if let Some(avg_latency) = self.get_average_latency(stream_id, 10).await {
            avg_latency <= self.target_latency_ms
        } else {
            true // 측정값이 없으면 허용 가능한 것으로 간주
        }
    }

    /// 지연시간 트렌드 조회
    pub async fn get_latency_trend(&self, stream_id: &str) -> LatencyTrend {
        let measurements = self.measurements.read().await;
        if let Some(stream_measurements) = measurements.get(stream_id) {
            MetricsCalculator::analyze_latency_trend(stream_measurements)
        } else {
            LatencyTrend::Stable
        }
    }

    /// 최적화 제안 조회
    pub async fn get_optimization_suggestions(&self, stream_id: &str) -> Vec<OptimizationSuggestion> {
        if let Some(avg_latency) = self.get_average_latency(stream_id, 30).await {
            let trend = self.get_latency_trend(stream_id).await;
            MetricsCalculator::generate_optimization_suggestions(
                avg_latency,
                self.target_latency_ms,
                trend,
            )
        } else {
            Vec::new()
        }
    }

    /// 스트림 측정값 제거
    pub async fn remove_stream_measurements(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut measurements = self.measurements.write().await;
        measurements.remove(stream_id);
        Ok(())
    }
}