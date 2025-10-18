use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use crate::business_layer::streaming::hls_convertor::HlsConvertor;
use crate::business_layer::monitoring::{MetricsCollector, LatencyMonitor};

use super::hls_handlers::{
    get_master_playlist,
    get_segment_playlist,
    get_segment_file,
    get_init_mp4,
};

use super::s3_handlers::{
    upload_stream_to_s3,
    delete_stream_from_s3,
    get_s3_upload_status,
};

/// M3U8 서버 라우터 생성
pub fn create_m3u8_router(
    hls_convertor: Arc<HlsConvertor>,
    metrics_collector: Arc<MetricsCollector>,
    latency_monitor: Arc<LatencyMonitor>,
) -> Router {
    Router::new()
        // HLS 플레이리스트 및 세그먼트 라우트
        .route("/hls/{stream_key}/master.m3u8", get(get_master_playlist))
        .route("/hls/{stream_key}/playlist.m3u8", get(get_segment_playlist))
        .route("/hls/{stream_key}/{segment}", get(get_segment_file))
        .route("/hls/{stream_key}/init.mp4", get(get_init_mp4))
        
        // S3 관련 라우트
        .route("/s3/upload/{stream_key}", get(upload_stream_to_s3))
        .route("/s3/delete/{stream_key}", get(delete_stream_from_s3))
        .route("/s3/status/{stream_key}", get(get_s3_upload_status))
        
        // 상태 공유
        .with_state((hls_convertor, metrics_collector, latency_monitor))
}
