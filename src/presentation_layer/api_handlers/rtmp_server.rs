use std::sync::Arc;
use tokio::net::TcpListener;
use scuffle_rtmp::session::server::ServerSession;
use log::{info, error};
use crate::business_layer::streaming::hls_convertor::HlsConvertor;
use super::rtmp_session_handler::RtmpSessionHandler;

/// RTMP 서버
pub struct RtmpServer {
    hls_convertor: Arc<HlsConvertor>,
}

impl RtmpServer {
    /// 새로운 RTMP 서버 생성
    pub fn new(hls_convertor: Arc<HlsConvertor>) -> Self {
        Self { hls_convertor }
    }

    /// RTMP 서버 시작
    pub async fn start(&self, address: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(address).await?;
        info!("RTMP Server listening on {}", address);

        while let Ok((stream, addr)) = listener.accept().await {
            info!("New RTMP connection from: {}", addr);
            
            // 각 연결마다 새로운 핸들러 생성
            let handler = RtmpSessionHandler::new(self.hls_convertor.clone());
            let session = ServerSession::new(stream, handler);

            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!("RTMP session error: {}", err);
                }
            });
        }
        
        Ok(())
    }
}
