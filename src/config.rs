use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub hls: HlsConfig,
    pub adaptive_bitrate: AdaptiveBitrateConfig,
    pub api: ApiConfig,
    pub s3: S3Config,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub segment_delay: u32,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HlsConfig {
    pub save_dir: String,
    pub segment_duration: f64,
    pub part_duration: f64,
    pub max_segments: u32,
    pub max_parts: u32,
    pub enable_server_push: bool,
    pub enable_preload_hint: bool,
    pub target_latency: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AdaptiveBitrateConfig {
    pub enabled: bool,
    pub variants: Vec<BitrateVariant>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BitrateVariant {
    pub bandwidth: u32,
    pub resolution: String,
    pub name: String,
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

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let toml_str = fs::read_to_string("config.toml")?;
        let config: Config = toml::from_str(&toml_str)?;
        Ok(config)
    }
}
