use std::sync::Arc;
use tokio;
use log::{info, error};

mod config;
mod presentation_layer;
mod business_layer;
mod data_layer;
mod authentication_layer;
mod utils;

use config::Config;
use business_layer::streaming::hls_convertor::HlsConvertor;
use presentation_layer::api_handlers::rtmp_handler::Handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 초기화
    env_logger::Builder::from_default_env()
        .format_timestamp_secs()
        .init();

    // 설정 로드
    let config = Config::load()?;
    
    info!("LL-HLS Streaming Server Starting...");
    info!("RTMP Server: rtmp://localhost:1935/live");
    info!("Memory-based Streaming: Enabled");
    info!("Target Latency: {}s", config.streaming.target_latency);
    info!("Memory Buffer: {}MB", config.streaming.memory_buffer_mb);
    info!("Upload Workers: {}", config.streaming.upload_workers);

    // LL-HLS 컴포넌트 초기화
    let hls_convertor = Arc::new(HlsConvertor::new(config.clone()).await.map_err(|e| format!("Failed to initialize HLS convertor: {}", e))?);

    info!("LL-HLS Streaming Server Started Successfully!");
    info!("Ready to receive RTMP streams and serve LL-HLS content");
    
    // RTMP 서버 시작
    let rtmp_address = format!("{}:{}", config.server.host, config.server.port);
    let handler = Handler::new(hls_convertor.clone());
    handler.start_rtmp_server(&rtmp_address).await.map_err(|e| format!("RTMP server error: {}", e))?;
    
    Ok(())
}