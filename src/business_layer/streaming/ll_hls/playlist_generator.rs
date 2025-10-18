use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

use super::playlist_types::{StreamState, Segment, Part, PlaylistType, PlaylistConfig};
use super::playlist_builder::PlaylistBuilder;

/// LL-HLS 플레이리스트 생성기
pub struct LLHLSPlaylistGenerator {
    streams: Arc<RwLock<HashMap<String, StreamState>>>,
    config: PlaylistConfig,
}

impl LLHLSPlaylistGenerator {
    /// 새로운 플레이리스트 생성기 생성
    pub fn new(hls_config: crate::config::HlsConfig) -> Self {
        let config = PlaylistConfig {
            target_duration: hls_config.segment_duration,
            part_duration: hls_config.part_duration,
            max_segments: hls_config.max_segments,
            max_parts: hls_config.max_parts,
            enable_server_push: hls_config.enable_server_push,
            enable_preload_hint: hls_config.enable_preload_hint,
        };

        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// 스트림 생성
    pub async fn create_stream(&self, stream_id: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stream_state = StreamState {
            stream_id: stream_id.clone(),
            sequence_number: 0,
            target_duration: self.config.target_duration,
            segments: Vec::new(),
            last_updated: chrono::Utc::now(),
            playlist_type: PlaylistType::Live,
        };

        let mut streams = self.streams.write().await;
        streams.insert(stream_id, stream_state);
        Ok(())
    }

    /// 세그먼트 추가
    pub async fn add_segment(
        &self,
        stream_id: &str,
        uri: String,
        duration: f64,
        is_independent: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut streams = self.streams.write().await;
        if let Some(stream_state) = streams.get_mut(stream_id) {
            let segment = Segment {
                uri,
                duration,
                sequence: stream_state.sequence_number,
                is_independent,
            };

            PlaylistBuilder::add_segment(stream_state, segment);
            PlaylistBuilder::cleanup_old_segments(stream_state, self.config.max_segments);
        }
        Ok(())
    }

    /// 플레이리스트 생성
    pub async fn generate_playlist(&self, stream_id: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let streams = self.streams.read().await;
        if let Some(stream_state) = streams.get(stream_id) {
            Ok(PlaylistBuilder::build_basic_playlist(stream_state))
        } else {
            Err("Stream not found".into())
        }
    }

    /// 마스터 플레이리스트 생성
    pub async fn generate_master_playlist(
        &self,
        stream_id: &str,
        variants: &[crate::config::BitrateVariant],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PlaylistBuilder::build_master_playlist(variants))
    }

    /// 스트림 제거
    pub async fn remove_stream(&self, stream_id: &str) {
        let mut streams = self.streams.write().await;
        streams.remove(stream_id);
    }
}