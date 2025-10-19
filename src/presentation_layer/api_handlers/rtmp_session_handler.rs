use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use scuffle_rtmp::session::server::{ServerSession, ServerSessionError, SessionHandler, SessionData};
use crate::business_layer::streaming::hls_convertor::HlsConvertor;

/// RTMP 세션 핸들러
pub struct RtmpSessionHandler {
    hls_convertor: Arc<HlsConvertor>,
    // stream_key 기반으로 생성한 내부 스트림 ID와 이름을 세션 수명 동안 유지
    internal_stream_id: Option<u32>,
    stream_name: Option<String>,
}

impl RtmpSessionHandler {
    /// 새로운 RTMP 세션 핸들러 생성
    pub fn new(hls_convertor: Arc<HlsConvertor>) -> Self {
        Self { hls_convertor, internal_stream_id: None, stream_name: None }
    }

    /// 스트림키를 가공하여 파라미터를 제거하고 깔끔한 경로로 변환
    fn sanitize_stream_key(&self, stream_key: &str) -> String {
        // URL 파라미터 제거 (? 이후의 모든 내용)
        let clean_key = if let Some(query_pos) = stream_key.find('?') {
            &stream_key[..query_pos]
        } else {
            stream_key
        };

        // 특수문자 제거 및 안전한 파일명으로 변환
        clean_key
            .chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
                _ => c,
            })
            .collect::<String>()
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

        // let authed_stream_id: &str = &authenticate_and_get_stream_id(stream_key, &self.http_client).await?;
        let stream_name = self.sanitize_stream_key(stream_key);
        let internal_stream_id = Self::generate_stream_id(&stream_name);
        self.internal_stream_id = Some(internal_stream_id);
        self.stream_name = Some(stream_name.clone());

        println!("📡 Processed stream key -> name: {}, id: {}", stream_name, internal_stream_id);

        if let Err(e) = self.hls_convertor.start_hls_conversion(internal_stream_id, &stream_name).await {
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
        let name = self.stream_name.clone().unwrap_or_else(|| "unknown".to_string());

        if let Err(e) = self.hls_convertor.stop_hls_conversion(internal_id, &name).await {
            eprintln!("Failed to stop HLS conversion: {}", e);
        }
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
