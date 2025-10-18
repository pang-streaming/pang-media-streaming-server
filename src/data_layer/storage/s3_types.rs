/// 업로드할 파일 정보
#[derive(Debug, Clone)]
pub struct UploadTask {
    pub stream_key: String,
    pub file_path: String,
    pub s3_key: String,
    pub content_type: String,
}

/// 스트리밍 업로드 상태
#[derive(Debug, Clone)]
pub struct UploadStatus {
    pub stream_key: String,
    pub total_files: usize,
    pub uploaded_files: usize,
    pub failed_files: usize,
    pub is_complete: bool,
}

/// S3 업로드 결과
#[derive(Debug, Clone)]
pub struct UploadResult {
    pub success: bool,
    pub s3_url: Option<String>,
    pub error_message: Option<String>,
}

/// S3 업로드 설정
#[derive(Debug, Clone)]
pub struct S3UploadConfig {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub chunk_size: usize,
}
