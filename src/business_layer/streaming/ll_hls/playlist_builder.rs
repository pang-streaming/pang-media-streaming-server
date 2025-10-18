use super::playlist_types::{StreamState, Segment, Part, PlaylistType};

/// LL-HLS 플레이리스트 빌더
pub struct PlaylistBuilder;

impl PlaylistBuilder {
    /// 기본 LL-HLS 플레이리스트 생성
    pub fn build_basic_playlist(stream_state: &StreamState) -> String {
        let mut playlist = String::new();
        
        // 헤더
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:9\n");
        playlist.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", stream_state.target_duration as u32));
        playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", stream_state.sequence_number));
        
        // 플레이리스트 타입
        match stream_state.playlist_type {
            PlaylistType::Event => playlist.push_str("#EXT-X-PLAYLIST-TYPE:EVENT\n"),
            PlaylistType::Live => playlist.push_str("#EXT-X-PLAYLIST-TYPE:LIVE\n"),
        }
        
        // 서버 제어
        playlist.push_str("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=1.0,CAN-SKIP-UNTIL=0\n");
        
        // 세그먼트들
        for segment in &stream_state.segments {
            if segment.is_independent {
                playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");
            }
            playlist.push_str(&format!("#EXTINF:{:.3},\n", segment.duration));
            playlist.push_str(&format!("{}\n", segment.uri));
        }
        
        playlist
    }

    /// 마스터 플레이리스트 생성
    pub fn build_master_playlist(variants: &[crate::config::BitrateVariant]) -> String {
        let mut playlist = String::new();
        
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:9\n");
        
        for variant in variants {
            playlist.push_str(&format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}\n",
                variant.bandwidth, variant.resolution
            ));
            playlist.push_str(&format!("{}.m3u8\n", variant.name));
        }
        
        playlist
    }

    /// 세그먼트 추가
    pub fn add_segment(stream_state: &mut StreamState, segment: Segment) {
        stream_state.segments.push(segment);
        stream_state.sequence_number += 1;
        stream_state.last_updated = chrono::Utc::now();
    }

    /// 오래된 세그먼트 정리
    pub fn cleanup_old_segments(stream_state: &mut StreamState, max_segments: u32) {
        if stream_state.segments.len() > max_segments as usize {
            let remove_count = stream_state.segments.len() - max_segments as usize;
            stream_state.segments.drain(0..remove_count);
        }
    }
}
