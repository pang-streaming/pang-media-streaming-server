use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use reqwest::Client;
use scuffle_rtmp::session::server::{ServerSessionError, SessionHandler, SessionData};
use crate::authentication_layer::auth::{authenticate_and_get_stream_id, stop_stream};
use crate::business_layer::streaming::hls_convertor::HlsConvertor;

/// RTMP 세션 핸들러
pub struct RtmpSessionHandler {
    hls_convertor: Arc<HlsConvertor>,
    http_client: Client,
    internal_stream_id: Option<u32>,
    authed_stream_id: Option<String>,
    stream_key: Option<String>,
}

impl RtmpSessionHandler {
    /// 새로운 RTMP 세션 핸들러 생성
    pub fn new(hls_convertor: Arc<HlsConvertor>) -> Self {
        Self {
            hls_convertor,
            http_client: Client::new(),
            internal_stream_id: None,
            authed_stream_id: None,
            stream_key: None,
        }
    }

    /// 스트림키를 가공하여 파라미터를 제거하고 깔끔한 경로로 변환
    fn sanitize_stream_key(&mut self, stream_key: &str) -> String {
        // URL 파라미터 제거 (? 이후의 모든 내용)
        let user_key = if let Some(query_pos) = stream_key.find('?') {
            &stream_key[..query_pos]
        } else {
            stream_key
        }.to_string();
        self.stream_key = Option::from(user_key.clone());
        user_key
    }

    /// 스트림키(정제된)를 바탕으로 내부 스트림 ID(u32)를 결정적으로 생성
    fn generate_stream_id(stream_key: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        stream_key.hash(&mut hasher);
        let hash64 = hasher.finish();
        // 하위 32비트 사용 (0이 나올 확률은 매우 낮지만, 0일 경우 1로 치환)
        let id = (hash64 & 0xFFFF_FFFF) as u32;
        if id == 0 { 1 } else { id }
    }
}

impl SessionHandler for RtmpSessionHandler {
    async fn on_publish(
        &mut self,
        _stream_id: u32,
        _app_name: &str,
        stream_key: &str,
    ) -> Result<(), ServerSessionError> {
        println!("📡 RTMP publish request: stream_key={}", stream_key);
        let encoded_stream_key = self.sanitize_stream_key(stream_key);

        let api_config = self.hls_convertor.api_config();
        let authed_stream_id = authenticate_and_get_stream_id(&encoded_stream_key, &self.http_client, api_config).await?;
        
        let internal_stream_id = Self::generate_stream_id(&authed_stream_id);
        self.internal_stream_id = Some(internal_stream_id);
        self.authed_stream_id = Some(authed_stream_id.clone());

        println!("📡 Processed stream key -> name: {}, id: {}", authed_stream_id, internal_stream_id);

        if let Err(e) = self.hls_convertor.start_hls_conversion(internal_stream_id, &authed_stream_id).await {
            eprintln!("Failed to start HLS conversion: {}", e);
            return Err(ServerSessionError::InvalidChunkSize(0));
        }

        let mut header = Vec::new();
        header.extend_from_slice(b"FLV"); // Signature
        header.push(1); // Version
        header.push(0x05); // Flags (audio + video)
        header.extend_from_slice(&9u32.to_be_bytes()); // DataOffset
        header.extend_from_slice(&0u32.to_be_bytes()); // PreviousTagSize0
        // FLV 헤더를 스트림 데이터로 처리
        if let Err(e) = self.hls_convertor.process_stream_data(internal_stream_id, &header).await {
            eprintln!("Failed to process FLV header: {}", e);
        }

        Ok(())
    }

    async fn on_unpublish(&mut self, _stream_id: u32) -> Result<(), ServerSessionError> {
        let internal_id = match self.internal_stream_id {
            Some(id) => id,
            None => {
                // 이미 중지되었거나 publish 이전 상태
                return Ok(());
            }
        };
        let name = self.authed_stream_id.clone().unwrap_or_else(|| "unknown".to_string());

        if let Err(e) = self.hls_convertor.stop_hls_conversion(internal_id, &name).await {
            eprintln!("Failed to stop HLS conversion: {}", e);
        }

        stop_stream(self.stream_key.clone(), &self.http_client, self.hls_convertor.api_config()).await;
        Ok(())
    }

    async fn on_data(
        &mut self,
        _stream_id: u32,
        data: SessionData,
    ) -> Result<(), ServerSessionError> {
        let internal_id = match self.internal_stream_id {
            Some(id) => id,
            None => {
                // publish 이전 데이터는 무시
                return Ok(());
            }
        };
        let (tag_type, timestamp, payload) = match data {
            SessionData::Video { timestamp, data } => (9, timestamp, data),
            SessionData::Audio { timestamp, data } => (8, timestamp, data),
            SessionData::Amf0 { timestamp, data } => (18, timestamp, data),
        };

        let data_size = payload.len() as u32;
        let mut flv_tag = Vec::new();
        flv_tag.push(tag_type); // TagType
        flv_tag.extend_from_slice(&(data_size.to_be_bytes()[1..])); // DataSize
        flv_tag.extend_from_slice(&(timestamp.to_be_bytes()[1..])); // Timestamp
        flv_tag.push((timestamp >> 24) as u8); // TimestampExtended
        flv_tag.extend_from_slice(&[0, 0, 0]); // StreamID
        flv_tag.extend_from_slice(&payload);
        flv_tag.extend_from_slice(&(data_size + 11).to_be_bytes()); // PreviousTagSize

        if let Err(e) = self.hls_convertor.process_stream_data(internal_id, &flv_tag).await {
            // 파이프라인이 깨진 경우 더 이상 데이터를 전송하지 않음
            if e.to_string().contains("Pipeline broken") {
                eprintln!("Pipeline broken for stream {}, stopping data processing", internal_id);
                return Ok(()); // 더 이상 데이터 처리 중단
            }
            eprintln!("Failed to process stream data: {}", e);
        }

        Ok(())
    }
}
