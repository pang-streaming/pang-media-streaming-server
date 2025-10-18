use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct PreloadHint {
    pub uri: String,
    pub hint_type: HintType,
    pub duration: Option<f64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum HintType {
    Part,
    Segment,
}

pub struct LLHLSPreloadHintManager {
    hints: Arc<RwLock<HashMap<String, Vec<PreloadHint>>>>,
}

impl LLHLSPreloadHintManager {
    pub fn new() -> Self {
        Self {
            hints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_hint(
        &self,
        stream_id: &str,
        uri: String,
        hint_type: HintType,
        duration: Option<f64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let hint = PreloadHint {
            uri,
            hint_type,
            duration,
            created_at: Utc::now(),
        };

        let mut hints = self.hints.write().await;
        if let Some(stream_hints) = hints.get_mut(stream_id) {
            stream_hints.push(hint);
            
            // 최대 힌트 수 제한 (메모리 관리)
            if stream_hints.len() > 10 {
                stream_hints.remove(0);
            }
        } else {
            hints.insert(stream_id.to_string(), vec![hint]);
        }

        Ok(())
    }

    pub async fn get_latest_hint(&self, stream_id: &str) -> Option<PreloadHint> {
        let hints = self.hints.read().await;
        if let Some(stream_hints) = hints.get(stream_id) {
            stream_hints.last().cloned()
        } else {
            None
        }
    }

    pub async fn generate_preload_hint_tag(&self, stream_id: &str) -> Option<String> {
        if let Some(hint) = self.get_latest_hint(stream_id).await {
            match hint.hint_type {
                HintType::Part => {
                    if let Some(duration) = hint.duration {
                        Some(format!("#EXT-X-PRELOAD-HINT:TYPE=PART,URI={},DURATION={}", 
                            hint.uri, duration))
                    } else {
                        Some(format!("#EXT-X-PRELOAD-HINT:TYPE=PART,URI={}", hint.uri))
                    }
                }
                HintType::Segment => {
                    if let Some(duration) = hint.duration {
                        Some(format!("#EXT-X-PRELOAD-HINT:TYPE=SEGMENT,URI={},DURATION={}", 
                            hint.uri, duration))
                    } else {
                        Some(format!("#EXT-X-PRELOAD-HINT:TYPE=SEGMENT,URI={}", hint.uri))
                    }
                }
            }
        } else {
            None
        }
    }

    pub async fn cleanup_old_hints(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut hints = self.hints.write().await;
        if let Some(stream_hints) = hints.get_mut(stream_id) {
            let cutoff_time = Utc::now() - chrono::Duration::seconds(30);
            stream_hints.retain(|hint| hint.created_at > cutoff_time);
        }
        Ok(())
    }

    pub async fn remove_stream_hints(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut hints = self.hints.write().await;
        hints.remove(stream_id);
        Ok(())
    }
}

// 프리로드 힌트 생성 헬퍼 함수들
pub fn create_part_hint(part_uri: &str, duration: f64) -> String {
    format!("#EXT-X-PRELOAD-HINT:TYPE=PART,URI={},DURATION={}", part_uri, duration)
}

pub fn create_segment_hint(segment_uri: &str, duration: f64) -> String {
    format!("#EXT-X-PRELOAD-HINT:TYPE=SEGMENT,URI={},DURATION={}", segment_uri, duration)
}

// HTTP 헤더에 프리로드 힌트 추가
pub fn add_preload_header(headers: &mut axum::http::HeaderMap, hint_uri: &str) {
    use axum::http::HeaderValue;
    headers.insert("link", HeaderValue::from_str(&format!("<{}>; rel=preload", hint_uri)).unwrap());
}
