use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use bytes::{Bytes, BytesMut};

/// 스트림별 메모리 버퍼
pub struct StreamBuffer {
    pub stream_name: String,
    pub segments: HashMap<String, Bytes>,
    pub playlist: Option<Bytes>,
    pub init_segment: Option<Bytes>,
    pub total_size: usize,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl StreamBuffer {
    pub fn new(stream_name: String) -> Self {
        Self {
            stream_name,
            segments: HashMap::new(),
            playlist: None,
            init_segment: None,
            total_size: 0,
            last_updated: chrono::Utc::now(),
        }
    }

    /// 세그먼트 추가
    pub fn add_segment(&mut self, name: String, data: Bytes) {
        self.total_size += data.len();
        self.segments.insert(name, data);
        self.last_updated = chrono::Utc::now();
    }

    /// 플레이리스트 업데이트
    pub fn update_playlist(&mut self, data: Bytes) {
        if let Some(old) = &self.playlist {
            self.total_size -= old.len();
        }
        self.total_size += data.len();
        self.playlist = Some(data);
        self.last_updated = chrono::Utc::now();
    }

    /// 초기화 세그먼트 설정
    pub fn set_init_segment(&mut self, data: Bytes) {
        if let Some(old) = &self.init_segment {
            self.total_size -= old.len();
        }
        self.total_size += data.len();
        self.init_segment = Some(data);
        self.last_updated = chrono::Utc::now();
    }

    /// 오래된 세그먼트 정리
    pub fn cleanup_old_segments(&mut self, keep_count: usize) {
        if self.segments.len() <= keep_count {
            return;
        }

        // 세그먼트 이름으로 정렬 (보통 시간순)
        let mut segment_names: Vec<String> = self.segments.keys().cloned().collect();
        segment_names.sort();

        // 오래된 세그먼트 제거
        let remove_count = self.segments.len() - keep_count;
        for name in segment_names.iter().take(remove_count) {
            if let Some(data) = self.segments.remove(name) {
                self.total_size -= data.len();
            }
        }
    }
}

/// 메모리 버퍼 관리자
pub struct MemoryBufferManager {
    buffers: Arc<RwLock<HashMap<String, StreamBuffer>>>,
    max_buffer_size: usize,
    max_segments_per_stream: usize,
}

impl MemoryBufferManager {
    /// 새로운 메모리 버퍼 관리자 생성
    pub fn new(max_buffer_size_mb: usize, max_segments_per_stream: usize) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            max_buffer_size: max_buffer_size_mb * 1024 * 1024,
            max_segments_per_stream,
        }
    }

    /// 스트림 버퍼 생성
    pub async fn create_stream(&self, stream_name: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffers = self.buffers.write().await;
        buffers.insert(stream_name.clone(), StreamBuffer::new(stream_name));
        Ok(())
    }

    /// 세그먼트를 메모리에 저장
    pub async fn store_segment(
        &self,
        stream_name: &str,
        segment_name: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffers = self.buffers.write().await;

        // 메모리 크기 체크
        let total_size = self.get_total_size(&buffers);
        if total_size + data.len() > self.max_buffer_size {
            // 가장 오래된 스트림의 세그먼트 정리
            Self::cleanup_oldest_segments_static(&mut buffers);
        }

        let buffer = buffers
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream buffer not found: {}", stream_name))?;

        buffer.add_segment(segment_name.to_string(), Bytes::from(data));

        // 세그먼트 수 제한
        buffer.cleanup_old_segments(self.max_segments_per_stream);

        Ok(())
    }

    /// 플레이리스트를 메모리에 저장
    pub async fn store_playlist(
        &self,
        stream_name: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffers = self.buffers.write().await;

        let buffer = buffers
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream buffer not found: {}", stream_name))?;

        buffer.update_playlist(Bytes::from(data));
        Ok(())
    }

    /// 초기화 세그먼트를 메모리에 저장
    pub async fn store_init_segment(
        &self,
        stream_name: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffers = self.buffers.write().await;

        let buffer = buffers
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream buffer not found: {}", stream_name))?;

        buffer.set_init_segment(Bytes::from(data));
        Ok(())
    }

    /// 스트림의 모든 데이터 가져오기 (S3 업로드용)
    pub async fn get_stream_data(&self, stream_name: &str) -> Option<HashMap<String, Vec<u8>>> {
        let buffers = self.buffers.read().await;
        let buffer = buffers.get(stream_name)?;

        let mut data = HashMap::new();

        // 플레이리스트
        if let Some(playlist) = &buffer.playlist {
            data.insert("playlist.m3u8".to_string(), playlist.to_vec());
        }

        // 초기화 세그먼트
        if let Some(init) = &buffer.init_segment {
            data.insert("init.mp4".to_string(), init.to_vec());
        }

        // 모든 세그먼트
        for (name, segment) in &buffer.segments {
            data.insert(name.clone(), segment.to_vec());
        }

        Some(data)
    }

    /// 특정 세그먼트 가져오기
    pub async fn get_segment(&self, stream_name: &str, segment_name: &str) -> Option<Bytes> {
        let buffers = self.buffers.read().await;
        let buffer = buffers.get(stream_name)?;
        buffer.segments.get(segment_name).cloned()
    }

    /// 플레이리스트 가져오기
    pub async fn get_playlist(&self, stream_name: &str) -> Option<Bytes> {
        let buffers = self.buffers.read().await;
        let buffer = buffers.get(stream_name)?;
        buffer.playlist.clone()
    }

    /// 스트림 버퍼 제거
    pub async fn remove_stream(&self, stream_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffers = self.buffers.write().await;
        buffers.remove(stream_name);
        Ok(())
    }

    /// 전체 버퍼 크기 계산
    fn get_total_size(&self, buffers: &HashMap<String, StreamBuffer>) -> usize {
        buffers.values().map(|b| b.total_size).sum()
    }

    /// 가장 오래된 세그먼트 정리 (static 버전)
    fn cleanup_oldest_segments_static(buffers: &mut HashMap<String, StreamBuffer>) {
        // 가장 오래 업데이트된 스트림 찾기
        if let Some((_, buffer)) = buffers
            .iter_mut()
            .min_by_key(|(_, b)| b.last_updated)
        {
            // 절반의 세그먼트 제거
            let keep_count = buffer.segments.len() / 2;
            buffer.cleanup_old_segments(keep_count.max(1));
        }
    }

    /// 버퍼 상태 정보
    pub async fn get_stats(&self) -> HashMap<String, (usize, usize)> {
        let buffers = self.buffers.read().await;
        buffers
            .iter()
            .map(|(name, buffer)| {
                (name.clone(), (buffer.segments.len(), buffer.total_size))
            })
            .collect()
    }

    /// 메모리 사용량 정보
    pub async fn get_memory_usage(&self) -> (usize, usize) {
        let buffers = self.buffers.read().await;
        let used = self.get_total_size(&buffers);
        (used, self.max_buffer_size)
    }
}