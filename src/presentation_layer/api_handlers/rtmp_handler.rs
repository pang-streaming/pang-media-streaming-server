use std::sync::Arc;
use reqwest::Client;
use crate::business_layer::streaming::hls_convertor::HlsConvertor;

use super::rtmp_server::RtmpServer;

/// RTMP 핸들러 (기존 호환성을 위한 래퍼)
pub struct Handler {
    hls_convertor: Arc<HlsConvertor>,
    _http_client: Arc<Client>,
}

impl Handler {
    /// 새로운 핸들러 생성
    pub fn new(hls_convertor: Arc<HlsConvertor>) -> Self {
        Self {
            hls_convertor,
            _http_client: Arc::new(Client::new()),
        }
    }

    /// RTMP 서버 시작
    pub async fn start_rtmp_server(&self, address: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let server = RtmpServer::new(self.hls_convertor.clone());
        server.start(address).await
    }
}