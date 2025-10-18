use axum::{
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct PushResource {
    pub uri: String,
    pub content_type: String,
    pub data: Vec<u8>,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

pub struct LLHLSServerPush {
    resources: Arc<RwLock<HashMap<String, PushResource>>>,
    push_headers: HeaderMap,
}

impl LLHLSServerPush {
    pub fn new() -> Self {
        let mut push_headers = HeaderMap::new();
        push_headers.insert("cache-control", HeaderValue::from_static("no-cache"));
        push_headers.insert("access-control-allow-origin", HeaderValue::from_static("*"));
        push_headers.insert("access-control-allow-methods", HeaderValue::from_static("GET, HEAD, OPTIONS"));
        push_headers.insert("access-control-allow-headers", HeaderValue::from_static("*"));

        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
            push_headers,
        }
    }

    pub async fn register_resource(
        &self,
        stream_id: &str,
        resource_name: &str,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = format!("{}/{}", stream_id, resource_name);
        let resource = PushResource {
            uri: resource_name.to_string(),
            content_type: content_type.to_string(),
            data,
            last_modified: chrono::Utc::now(),
        };

        let mut resources = self.resources.write().await;
        resources.insert(key, resource);
        
        Ok(())
    }

    pub async fn get_resource(
        &self,
        stream_id: &str,
        resource_name: &str,
    ) -> Result<Response<axum::body::Body>, StatusCode> {
        let key = format!("{}/{}", stream_id, resource_name);
        let resources = self.resources.read().await;
        
        if let Some(resource) = resources.get(&key) {
            let mut headers = self.push_headers.clone();
            headers.insert("content-type", HeaderValue::from_str(&resource.content_type).unwrap());
            headers.insert("last-modified", HeaderValue::from_str(&resource.last_modified.to_rfc2822()).unwrap());
            headers.insert("content-length", HeaderValue::from_str(&resource.data.len().to_string()).unwrap());

            let body = axum::body::Body::from(resource.data.clone());
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .unwrap();
            
            *response.headers_mut() = headers;
            Ok(response)
        } else {
            Err(StatusCode::NOT_FOUND)
        }
    }

    pub async fn push_segment(
        &self,
        stream_id: &str,
        segment_name: &str,
        segment_data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let content_type = if segment_name.ends_with(".m4s") {
            "video/iso.segment"
        } else if segment_name.ends_with(".m3u8") {
            "application/vnd.apple.mpegurl"
        } else if segment_name.ends_with(".mp4") {
            "video/mp4"
        } else {
            "application/octet-stream"
        };

        self.register_resource(stream_id, segment_name, content_type, segment_data).await?;
        Ok(())
    }

    pub async fn push_playlist(
        &self,
        stream_id: &str,
        playlist_content: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let data = playlist_content.into_bytes();
        self.register_resource(stream_id, "playlist.m3u8", "application/vnd.apple.mpegurl", data).await?;
        Ok(())
    }

    pub async fn cleanup_old_resources(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut resources = self.resources.write().await;
        let cutoff_time = chrono::Utc::now() - chrono::Duration::seconds(60);
        
        resources.retain(|key, resource| {
            if key.starts_with(&format!("{}/", stream_id)) && resource.last_modified < cutoff_time {
                false
            } else {
                true
            }
        });
        
        Ok(())
    }

    pub async fn remove_stream_resources(&self, stream_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut resources = self.resources.write().await;
        resources.retain(|key, _| !key.starts_with(&format!("{}/", stream_id)));
        Ok(())
    }
}

// HTTP/2 Server Push를 위한 헬퍼 함수들
pub fn add_push_headers(headers: &mut HeaderMap, push_uri: &str) {
    headers.insert("link", HeaderValue::from_str(&format!("<{}>; rel=preload", push_uri)).unwrap());
}

pub fn create_push_response(
    content: Vec<u8>,
    content_type: &str,
    push_uris: Vec<&str>,
) -> Response<axum::body::Body> {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_str(content_type).unwrap());
    headers.insert("cache-control", HeaderValue::from_static("no-cache"));
    
    // Server Push 힌트 추가
    for uri in push_uris {
        add_push_headers(&mut headers, uri);
    }

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(axum::body::Body::from(content))
        .unwrap();
    
    *response.headers_mut() = headers;
    response
}
