use std::sync::Arc;
use tokio;
use tracing_subscriber;

mod config;
mod presentation_layer;
mod business_layer;
mod data_layer;
mod authentication_layer;
mod utils;

use config::Config;
use business_layer::streaming::hls_convertor::HlsConvertor;
use business_layer::monitoring::metrics_collector::MetricsCollector;
use business_layer::monitoring::latency_monitor::LatencyMonitor;
use presentation_layer::api_handlers::rtmp_handler::Handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // 설정 로드
    let config = Config::load()?;
    
    println!("🚀 LL-HLS Streaming Server Starting...");
    println!("📡 RTMP Server: rtmp://localhost:1935/live");
    println!("🌐 HLS Server: http://localhost:8081/hls");
    println!("📊 Metrics API: http://localhost:8081/metrics");
    println!("⚡ LL-HLS Features: Enabled");
    println!("🎯 Target Latency: {}s", config.hls.target_latency);

    // LL-HLS 컴포넌트 초기화
    let metrics_collector = Arc::new(MetricsCollector::new());
    let latency_monitor = Arc::new(LatencyMonitor::new(metrics_collector.clone(), config.hls.target_latency * 1000.0));
    let hls_convertor = Arc::new(HlsConvertor::new(config.clone(), metrics_collector.clone(), latency_monitor.clone()).await.map_err(|e| format!("Failed to initialize HLS convertor: {}", e))?);

    // M3U8 서버 시작 (백그라운드)
    crate::presentation_layer::http_server::m3u8_server::start_m3u8_server_background(
        hls_convertor.clone(),
        metrics_collector.clone(),
        latency_monitor.clone(),
    );

    println!("✅ LL-HLS Streaming Server Started Successfully!");
    println!("🎬 Ready to receive RTMP streams and serve LL-HLS content");
    
    // RTMP 서버 시작
    let rtmp_address = format!("{}:{}", config.server.host, config.server.port);
    let handler = Handler::new(hls_convertor.clone());
    handler.start_rtmp_server(&rtmp_address).await.map_err(|e| format!("RTMP server error: {}", e))?;
    
    Ok(())
}