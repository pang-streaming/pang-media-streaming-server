use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use axum::body::Body;
use crate::business_layer::streaming::hls_convertor::HlsConvertor;
use crate::business_layer::monitoring::{MetricsCollector, LatencyMonitor};

use super::m3u8_router::create_m3u8_router;

/// M3U8 서버 시작
pub async fn start_m3u8_server(
    hls_convertor: Arc<HlsConvertor>,
    metrics_collector: Arc<MetricsCollector>,
    latency_monitor: Arc<LatencyMonitor>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = create_m3u8_router(hls_convertor, metrics_collector, latency_monitor);

    // HLS 서버는 8081 포트 사용
    let address = "0.0.0.0:8081";
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("🌐 HLS Server: http://{}", address);
    
    axum::serve(listener, app).await?;
    Ok(())
}

/// M3U8 서버를 백그라운드에서 시작
pub fn start_m3u8_server_background(
    hls_convertor: Arc<HlsConvertor>,
    metrics_collector: Arc<MetricsCollector>,
    latency_monitor: Arc<LatencyMonitor>,
) {
    tokio::spawn(async move {
        if let Err(e) = start_m3u8_server(hls_convertor, metrics_collector, latency_monitor).await {
            eprintln!("❌ M3U8 server error: {}", e);
        }
    });
}