use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::io::Write;
use tokio::fs;
use log::{info, error};
use crate::config::Config;
use crate::data_layer::storage::cli_s3_uploader::CliS3Uploader;
use super::pipeline::MemoryFfmpegPipeline;
use super::file_watcher::start_file_watcher;

/// 파일 기반 FFmpeg 파이프라인 관리자 (AWS CLI S3 업로드)
pub struct MemoryFfmpegPipelineManager {
    pipelines: Arc<RwLock<HashMap<u32, MemoryFfmpegPipeline>>>,
    config: Config,
    s3_uploader: Arc<CliS3Uploader>,
}

impl MemoryFfmpegPipelineManager {
    /// 새로운 파일 기반 FFmpeg 파이프라인 관리자 생성
    pub fn new(
        config: Config,
        s3_uploader: Arc<CliS3Uploader>,
    ) -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            config,
            s3_uploader,
        }
    }

    /// FFmpeg 파이프라인 시작
    pub async fn start_pipeline(
        &self,
        stream_id: u32,
        stream_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[Pipeline] Starting for stream {} (id={})", stream_name, stream_id);

        // 출력 디렉토리
        let temp_dir = format!("/tmp/hls_temp/{}", stream_name);

        // 기존 디렉토리가 있으면 삭제 (클린 스타트)
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        tokio::fs::create_dir_all(&temp_dir).await?;
        info!("[Pipeline] Output directory created: {}", temp_dir);

        // FFmpeg 명령어 구성
        let mut cmd = self.build_ffmpeg_command(stream_name, &temp_dir);

        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());

        info!("[Pipeline] Spawning FFmpeg process...");
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().ok_or("Failed to get stdin")?;
        let stderr = child.stderr.take().ok_or("Failed to get stderr")?;
        info!("[Pipeline] FFmpeg process started");

        // 파일 변경 감지 및 S3 업로드
        info!("[Pipeline] Starting file watcher for: {}", temp_dir);
        let watcher_handle = start_file_watcher(
            stream_name.to_string(),
            temp_dir.clone(),
            Arc::clone(&self.s3_uploader),
        ).await?;
        info!("[Pipeline] File watcher started");

        let pipeline = MemoryFfmpegPipeline {
            stdin,
            stream_id,
            stream_name: stream_name.to_string(),
            watcher_handle: Some(watcher_handle),
        };

        // 파이프라인 저장
        {
            let mut pipelines = self.pipelines.write().await;
            pipelines.insert(stream_id, pipeline);
        }

        // FFmpeg stderr 모니터링 (백그라운드)
        self.monitor_ffmpeg_stderr(stream_id, stderr);

        info!("Memory-based FFmpeg pipeline started for stream {} ({})", stream_id, stream_name);
        Ok(())
    }

    /// FFmpeg 파이프라인 빌드 (LL-HLS 파트 직접 생성)
    fn build_ffmpeg_command(&self, _stream_name: &str, temp_dir: &str) -> Command {
        // datetime 기반 세그먼트 번호
        let segment_filename_pattern = format!(
            "{}/segment_%d.m4s",
            temp_dir
        );
        let playlist_path = format!("{}/playlist.m3u8", temp_dir);
        let mut cmd = Command::new("ffmpeg");

        cmd.args([
            "-y",
            "-f", "flv",
            "-i", "pipe:0",

            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",

            "-b:v", "5000k",
            "-maxrate", "5000k",
            "-bufsize", "10000k",

            "-g", "60",
            "-keyint_min", "60",
            "-sc_threshold", "0",

            "-c:a", "aac",
            "-b:a", "160k",
            "-ar", "44100",
            "-ac", "2",

            "-f", "hls",
            "-hls_time", "2",
            "-hls_list_size", "10",
            "-hls_flags", "independent_segments+program_date_time",

            "-hls_segment_type", "fmp4",
            "-hls_fmp4_init_filename", "init.mp4",
            "-hls_segment_filename", &segment_filename_pattern,

            "-hls_playlist_type", "event",
            "-hls_allow_cache", "0",
            "-hls_start_number_source", "datetime",
            "-movflags", "+frag_keyframe+empty_moov+faststart+default_base_moof",
            &playlist_path,
        ]);

        cmd
    }
    /// FFmpeg stderr 모니터링
    fn monitor_ffmpeg_stderr(&self, stream_id: u32, mut stderr: std::process::ChildStderr) {
        tokio::spawn(async move {
            use std::io::Read;
            let mut buffer = [0; 4096];
            loop {
                match stderr.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        let output = String::from_utf8_lossy(&buffer[..n]);
                        // 모든 출력을 로깅 (디버깅용)
                        for line in output.lines() {
                            if line.contains("error") || line.contains("Error") {
                                error!("FFmpeg[{}]: {}", stream_id, line);
                            } else if !line.trim().is_empty() {
                                info!("FFmpeg[{}]: {}", stream_id, line);
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }

    /// 파이프라인에 데이터 전송
    pub async fn send_data(&self, stream_id: u32, data: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pipelines = self.pipelines.write().await;
        if let Some(pipeline) = pipelines.get_mut(&stream_id) {
            match pipeline.stdin.write_all(data) {
                Ok(_) => {
                    if let Err(e) = pipeline.stdin.flush() {
                        pipelines.remove(&stream_id);
                        return Err(format!("Pipeline broken for stream {}: {}", stream_id, e).into());
                    }
                }
                Err(e) => {
                    pipelines.remove(&stream_id);
                    return Err(format!("Pipeline broken for stream {}: {}", stream_id, e).into());
                }
            }
        }
        Ok(())
    }

    /// 파이프라인 종료
    pub async fn stop_pipeline(&self, stream_id: u32) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pipelines = self.pipelines.write().await;
        if let Some(mut pipeline) = pipelines.remove(&stream_id) {
            // Watcher 종료
            if let Some(handle) = pipeline.watcher_handle.take() {
                handle.abort();
            }

            // 출력 디렉토리 정리
            let temp_dir = format!("/tmp/hls_temp/{}", pipeline.stream_name);
            let _ = fs::remove_dir_all(&temp_dir).await;

            info!("FFmpeg pipeline stopped for stream {}", stream_id);
        }
        Ok(())
    }

    /// 모든 파이프라인 종료
    pub async fn stop_all_pipelines(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pipelines = self.pipelines.write().await;
        for (_, mut pipeline) in pipelines.drain() {
            if let Some(handle) = pipeline.watcher_handle.take() {
                handle.abort();
            }
            let temp_dir = format!("/tmp/hls_temp/{}", pipeline.stream_name);
            let _ = fs::remove_dir_all(&temp_dir).await;
        }
        info!("All FFmpeg pipelines stopped");
        Ok(())
    }

    /// 파이프라인 존재 여부 확인
    pub async fn has_pipeline(&self, stream_id: u32) -> bool {
        let pipelines = self.pipelines.read().await;
        pipelines.contains_key(&stream_id)
    }

    /// 활성 파이프라인 수 조회
    pub async fn active_pipeline_count(&self) -> usize {
        let pipelines = self.pipelines.read().await;
        pipelines.len()
    }
}