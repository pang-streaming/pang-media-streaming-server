use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use aws_credential_types::Credentials;
use crate::config::S3Config;
use super::s3_types::{UploadTask, UploadResult, S3UploadConfig};

/// S3 클라이언트 래퍼
pub struct S3ClientWrapper {
    client: S3Client,
    bucket: String,
    region: String,
    config: S3UploadConfig,
}

impl S3ClientWrapper {
    /// 새로운 S3 클라이언트 생성
    pub async fn new(s3_config: &S3Config) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let region = Region::new(s3_config.region.clone());
        
        // AWS 자격 증명 설정
        let credentials = Credentials::new(
            &s3_config.access_key,
            &s3_config.secret_access_key,
            None, // session token
            None, // expires after
            "pang-streaming-server"
        );
        
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .credentials_provider(credentials)
            .load()
            .await;

        let client = S3Client::new(&aws_config);
        
        let config = S3UploadConfig {
            max_retries: 3,
            retry_delay_ms: 1000,
            chunk_size: 1024 * 1024, // 1MB
        };

        Ok(Self {
            client,
            bucket: s3_config.bucket.clone(),
            region: s3_config.region.clone(),
            config,
        })
    }

    /// 파일을 S3에 업로드
    pub async fn upload_file(&self, task: &UploadTask) -> Result<UploadResult, Box<dyn std::error::Error + Send + Sync>> {
        // 파일이 존재하는지 확인
        if !std::path::Path::new(&task.file_path).exists() {
            return Ok(UploadResult {
                success: false,
                s3_url: None,
                error_message: Some(format!("File does not exist: {}", task.file_path)),
            });
        }

        // 파일을 메모리로 읽기
        let file_content = match tokio::fs::read(&task.file_path).await {
            Ok(content) => content,
            Err(e) => {
                return Ok(UploadResult {
                    success: false,
                    s3_url: None,
                    error_message: Some(format!("Failed to read file: {}", e)),
                });
            }
        };

        // S3에 업로드 (재시도 로직 포함)
        let mut retry_count = 0;
        
        loop {
            let retry_body = ByteStream::from(file_content.clone());
            
            match self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&task.s3_key)
                .body(retry_body)
                .content_type(&task.content_type)
                .cache_control("no-cache, no-store, must-revalidate") // 실시간 스트리밍을 위한 캐시 비활성화
                .metadata("upload-timestamp", &chrono::Utc::now().to_rfc3339())
                .send()
                .await
            {
                Ok(_response) => {
                    let s3_url = format!("https://{}.s3.{}.amazonaws.com/{}", 
                                        self.bucket, self.region, task.s3_key);
                    
                    return Ok(UploadResult {
                        success: true,
                        s3_url: Some(s3_url),
                        error_message: None,
                    });
                }
                Err(e) => {
                    retry_count += 1;
                    
                    if retry_count >= self.config.max_retries {
                        return Ok(UploadResult {
                            success: false,
                            s3_url: None,
                            error_message: Some(format!("Upload failed after {} retries: {}", self.config.max_retries, e)),
                        });
                    }
                    
                    // 재시도 전 잠시 대기
                    tokio::time::sleep(tokio::time::Duration::from_millis(self.config.retry_delay_ms * retry_count as u64)).await;
                }
            }
        }
    }

    /// S3에서 파일 삭제
    pub async fn delete_file(&self, s3_key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .send()
            .await?;

        Ok(())
    }

    /// S3에서 디렉토리의 모든 파일 삭제
    pub async fn delete_directory(&self, s3_prefix: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let list_response = self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(s3_prefix)
            .send()
            .await?;

        if let Some(objects) = list_response.contents {
            for object in objects {
                if let Some(key) = object.key {
                    if let Err(e) = self.delete_file(&key).await {
                        eprintln!("❌ Failed to delete {}: {}", key, e);
                    }
                }
            }
        }

        Ok(())
    }
}
