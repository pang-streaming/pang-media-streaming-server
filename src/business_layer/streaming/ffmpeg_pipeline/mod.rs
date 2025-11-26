pub mod pipeline;
pub mod pipeline_manager;
pub mod file_watcher;

// Re-export the main public types
pub use pipeline_manager::MemoryFfmpegPipelineManager;