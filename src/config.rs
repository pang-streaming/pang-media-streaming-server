use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub streaming: StreamingConfig,
    pub api: ApiConfig,
    pub s3: S3Config,
    pub thumbnail: ThumbnailConfig,
    pub bunny: BunnyConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StreamingConfig {
    pub segment_duration: f64,      // 세그먼트 길이 (초)
    pub target_latency: f64,        // 목표 지연시간 (초)
    pub memory_buffer_mb: usize,    // 메모리 버퍼 크기 (MB)
    pub max_segments_per_stream: usize,  // 스트림당 최대 세그먼트 수
    pub upload_workers: usize,      // S3 업로드 워커 수
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    pub host: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_access_key: String,
    pub endpoint_uri: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ThumbnailConfig {
    pub enabled: bool,
    pub interval_seconds: u32,  // 썸네일 생성 간격 (초)
    pub width: u32,             // 썸네일 너비
    pub height: u32,            // 썸네일 높이
    pub quality: u32,           // FFmpeg -q:v 값 (1=최고, 5=보통)
}

#[derive(Debug, Deserialize, Clone)]
pub struct BunnyConfig {
    pub storage_zone: String,
    pub api_key: String,
    pub pull_zone_id: Option<String>,
    pub cdn_hostname: Option<String>,  // CDN URL용 (예: myzone.b-cdn.net)
}

impl Default for ThumbnailConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 5,
            width: 320,
            height: 180,
            quality: 3,
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let toml_str = fs::read_to_string("config.toml")?;
        let config: Config = toml::from_str(&toml_str)?;
        Ok(config)
    }
}

// 기본 설정값 제공
impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            segment_duration: 0.5,
            target_latency: 2.0,
            memory_buffer_mb: 500,
            max_segments_per_stream: 20,
            upload_workers: 10,
        }
    }
}