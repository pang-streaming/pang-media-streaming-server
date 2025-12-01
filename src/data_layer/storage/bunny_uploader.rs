use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use log::{info, warn, error};

/// BunnyCDN Storage 업로더 (비동기 동시 업로드)
pub struct BunnyUploader {
    client: reqwest::Client,
    storage_zone: String,
    api_key: String,
    cdn_hostname: Option<String>,
    active_count: Arc<AtomicUsize>,
}

impl BunnyUploader {
    pub fn new(
        storage_zone: String,
        api_key: String,
        cdn_hostname: Option<String>,
        _worker_count: usize,
    ) -> Self {
        // 동시 업로드 최적화 클라이언트
        let client = reqwest::Client::builder()
            .http1_only()
            .pool_max_idle_per_host(100)       // 충분한 커넥션 풀
            .pool_idle_timeout(std::time::Duration::from_secs(5))  // 짧은 idle 타임아웃
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(5))
            .tcp_nodelay(true)
            .tcp_keepalive(std::time::Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        info!("[BunnyUploader] Initialized - zone: {}", storage_zone);

        Self {
            client,
            storage_zone,
            api_key,
            cdn_hostname,
            active_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// CDN URL 반환
    pub fn get_cdn_url(&self, path: &str) -> String {
        if let Some(ref hostname) = self.cdn_hostname {
            format!("https://{}/{}", hostname, path)
        } else {
            format!("https://{}.b-cdn.net/{}", self.storage_zone, path)
        }
    }

    /// 비동기 동시 업로드 (제한 없음)
    pub async fn queue_data(
        &self,
        stream_name: &str,
        file_name: &str,
        data: Vec<u8>,
        content_type: &str,
        _priority: u8,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = format!("{}/{}", stream_name, file_name);
        let client = self.client.clone();
        let storage_zone = self.storage_zone.clone();
        let api_key = self.api_key.clone();
        let active_count = Arc::clone(&self.active_count);
        let content_type = content_type.to_string();

        // 즉시 spawn - 제한 없이 동시 실행
        tokio::spawn(async move {
            active_count.fetch_add(1, Ordering::Relaxed);

            let result = upload_with_retry(
                &client,
                &storage_zone,
                &api_key,
                &key,
                data,
                &content_type,
            ).await;

            if let Err(e) = result {
                error!("[Bunny] FAILED {}: {}", key, e);
            }

            active_count.fetch_sub(1, Ordering::Relaxed);
        });

        Ok(())
    }

    /// 파일 삭제
    pub async fn delete_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("https://sg.storage.bunnycdn.com/{}/{}", self.storage_zone, path);

        let resp = self.client.delete(&url)
            .header("AccessKey", &self.api_key)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("Delete failed: {}", resp.status()).into())
        }
    }

    /// 파일 목록 조회
    pub async fn list_files(&self, path: &str) -> Result<Vec<StorageObject>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("https://sg.storage.bunnycdn.com/{}/{}/", self.storage_zone, path);

        let resp = self.client.get(&url)
            .header("AccessKey", &self.api_key)
            .header("Accept", "application/json")
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            Err(format!("List failed: {}", resp.status()).into())
        }
    }

    pub async fn purge_cache(&self, pull_zone_id: &str, account_api_key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("https://api.bunny.net/pullzone/{}/purgeCache", pull_zone_id);

        let resp = self.client.post(&url)
            .header("AccessKey", account_api_key)
            .send()
            .await?;

        if resp.status().as_u16() == 204 {
            Ok(())
        } else {
            Err(format!("Purge failed: {}", resp.status()).into())
        }
    }

    pub async fn pending_count(&self) -> usize {
        self.active_count.load(Ordering::Relaxed)
    }

    pub async fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Relaxed)
    }
}

/// 재시도 포함 업로드
async fn upload_with_retry(
    client: &reqwest::Client,
    storage_zone: &str,
    api_key: &str,
    key: &str,
    data: Vec<u8>,
    content_type: &str,
) -> Result<(), String> {
    let url = format!("https://sg.storage.bunnycdn.com/{}/{}", storage_zone, key);

    // 플레이리스트는 캐시 비활성화
    let cache_control = if key.ends_with(".m3u8") {
        "no-cache, no-store, must-revalidate, max-age=0"
    } else {
        "public, max-age=31536000"  // 세그먼트는 1년 캐시
    };

    for attempt in 0..5 {
        let result = client
            .put(&url)
            .header("AccessKey", api_key)
            .header("Content-Type", content_type)
            .header("Cache-Control", cache_control)
            .body(data.clone())
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() || status.as_u16() == 201 {
                    info!("[Bunny] OK {}", key);
                    return Ok(());
                } else {
                    let msg = format!("HTTP {}", status.as_u16());
                    if attempt < 4 {
                        warn!("[Bunny] {} retry {}: {}", key, attempt + 1, msg);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * (attempt + 1))).await;
                    } else {
                        return Err(msg);
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if attempt < 4 {
                    warn!("[Bunny] {} retry {}: {}", key, attempt + 1, msg);
                    // 연결 에러시 좀 더 대기
                    tokio::time::sleep(tokio::time::Duration::from_millis(200 * (attempt + 1))).await;
                } else {
                    return Err(msg);
                }
            }
        }
    }

    Err("Max retries exceeded".to_string())
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StorageObject {
    pub guid: String,
    pub storage_zone_name: String,
    pub path: String,
    pub object_name: String,
    pub length: i64,
    pub last_changed: String,
    pub server_id: i32,
    pub is_directory: bool,
    pub user_id: String,
    pub date_created: String,
    pub storage_zone_id: i64,
    pub checksum: Option<String>,
    pub replicated_zones: Option<String>,
}
