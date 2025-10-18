use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use axum::body::Body;
use crate::business_layer::streaming::hls_convertor::HlsConvertor;
use crate::business_layer::monitoring::{MetricsCollector, LatencyMonitor};

/// HLS 마스터 플레이리스트 핸들러
pub async fn get_master_playlist(
    Path(stream_key): Path<String>,
    State((_hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    let config = crate::config::Config::load().expect("Failed to load config");
    
    if config.adaptive_bitrate.enabled {
        // 적응형 비트레이트 마스터 플레이리스트 생성
        // 적응형 비트레이트는 현재 지원하지 않음
        Err(StatusCode::NOT_FOUND)
    } else {
        // 단일 비트레이트 마스터 플레이리스트
        let master_playlist = format!(
            "#EXTM3U\n\
             #EXT-X-VERSION:9\n\
             #EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1920x1080,FRAME-RATE=60\n\
             {}/playlist.m3u8\n",
            stream_key
        );

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/vnd.apple.mpegurl"));
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        
        Ok((headers, master_playlist))
    }
}

/// HLS 세그먼트 플레이리스트 핸들러
pub async fn get_segment_playlist(
    Path(stream_key): Path<String>,
    State((_hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    let playlist_path = PathBuf::from("hls_output").join(&stream_key).join("playlist.m3u8");
    
    match File::open(playlist_path).await {
        Ok(file) => {
            let stream = ReaderStream::new(file);
            let body = Body::from_stream(stream);
            
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/vnd.apple.mpegurl"));
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            
            // 프리로드 힌트 헤더 추가
            // Preload hint는 현재 지원하지 않음
            
            Ok((headers, body))
        }
        Err(_) => {
            // 기본 LL-HLS 플레이리스트
            let default_playlist = format!(
                "#EXTM3U\n\
                 #EXT-X-VERSION:9\n\
                 #EXT-X-TARGETDURATION:1\n\
                 #EXT-X-MEDIA-SEQUENCE:0\n\
                 #EXT-X-PLAYLIST-TYPE:EVENT\n\
                 #EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=1.0,CAN-SKIP-UNTIL=0\n"
            );
            
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/vnd.apple.mpegurl"));
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            
            Ok((headers, default_playlist.into()))
        }
    }
}

/// HLS 세그먼트 파일 핸들러
pub async fn get_segment_file(
    Path((stream_key, segment)): Path<(String, String)>,
    State((_hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    let allowed_extensions = [".ts", ".m4s", ".mp4"];
    if !allowed_extensions.iter().any(|ext| segment.ends_with(ext)) {
        return Err(StatusCode::NOT_FOUND);
    }

    // Server push는 현재 지원하지 않음

    // 파일 시스템에서 직접 읽기
    let file_path = PathBuf::from("hls_output").join(&stream_key).join(&segment);

    let file = File::open(file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let content_type = if segment.ends_with(".m4s") {
        "video/iso.segment"
    } else if segment.ends_with(".mp4") {
        "video/mp4"
    } else {
        "video/mp2t"
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .unwrap();
    
    *response.headers_mut() = headers;
    Ok(response)
}

/// HLS 초기화 파일 핸들러
pub async fn get_init_mp4(
    Path(stream_key): Path<String>,
    State((_hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    let file_path = PathBuf::from("hls_output").join(&stream_key).join("init.mp4");
    
    let file = File::open(file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("video/mp4"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("public, max-age=3600"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .unwrap();
    
    *response.headers_mut() = headers;
    Ok(response)
}
