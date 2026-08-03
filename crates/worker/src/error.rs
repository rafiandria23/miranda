use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerError {
    #[error("task execution failed: {message}")]
    ExecutionFailed { message: String },

    #[error("task type not supperted: {task_type}")]
    UnsupportedTaskType { task_type: String },

    #[error("task timeout after {duration}ms")]
    Timeout { duration: u64 },
}
