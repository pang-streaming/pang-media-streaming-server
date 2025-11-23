use std::process::ChildStdin;
use tokio::task::JoinHandle;

/// 메모리 기반 FFmpeg 파이프라인
pub struct MemoryFfmpegPipeline {
    pub stdin: ChildStdin,
    pub stream_id: u32,
    pub stream_name: String,
    pub watcher_handle: Option<JoinHandle<()>>,
}