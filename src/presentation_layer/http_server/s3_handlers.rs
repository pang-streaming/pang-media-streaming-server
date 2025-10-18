use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use std::sync::Arc;
use crate::business_layer::streaming::hls_convertor::HlsConvertor;
use crate::business_layer::monitoring::{MetricsCollector, LatencyMonitor};

/// S3 업로드 핸들러
pub async fn upload_stream_to_s3(
    Path(stream_key): Path<String>,
    State((hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    match hls_convertor.upload_stream_to_s3(&stream_key).await {
        Ok(_) => {
            let response = serde_json::json!({
                "success": true,
                "stream_key": stream_key,
                "message": "Stream queued for background upload to S3"
            });

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            
            Ok((headers, response.to_string()))
        }
        Err(e) => {
            eprintln!("❌ Failed to upload stream to S3: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// S3 삭제 핸들러
pub async fn delete_stream_from_s3(
    Path(stream_key): Path<String>,
    State((hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    match hls_convertor.delete_stream_from_s3(&stream_key).await {
        Ok(_) => {
            let response = serde_json::json!({
                "success": true,
                "stream_key": stream_key,
                "message": "Stream deleted from S3"
            });

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            
            Ok((headers, response.to_string()))
        }
        Err(e) => {
            eprintln!("❌ Failed to delete stream from S3: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// S3 업로드 상태 조회 핸들러
pub async fn get_s3_upload_status(
    Path(stream_key): Path<String>,
    State((hls_convertor, _, _)): State<(Arc<HlsConvertor>, Arc<MetricsCollector>, Arc<LatencyMonitor>)>,
) -> Result<impl IntoResponse, StatusCode> {
    match hls_convertor.get_s3_upload_status(&stream_key).await {
        Some(status) => {
            let response = serde_json::json!({
                "success": true,
                "stream_key": stream_key,
                "status": {
                    "total_files": status.total_files,
                    "uploaded_files": status.uploaded_files,
                    "failed_files": status.failed_files,
                    "is_complete": status.is_complete,
                    "progress_percentage": if status.total_files > 0 {
                        (status.uploaded_files as f64 / status.total_files as f64 * 100.0).round() as u32
                    } else {
                        0
                    }
                }
            });

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            
            Ok((headers, response.to_string()))
        }
        None => {
            let response = serde_json::json!({
                "success": false,
                "stream_key": stream_key,
                "message": "No upload status found for this stream"
            });

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            
            Ok((headers, response.to_string()))
        }
    }
}
