use miranda_core::{ExecutionError, id::WorkflowTaskId};
use miranda_storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Domain(#[from] ExecutionError),

    #[error("task execution failed: {0}")]
    ExecutionFailed(String),

    #[error("max retry attempts ({max_attempts}) reached for task '{task_id}'")]
    MaxAttemptsReached {
        task_id: WorkflowTaskId,
        max_attempts: u64,
    },

    #[error("scheduler error: {0}")]
    Scheduler(String),

    #[error("timer error: {0}")]
    Timer(String),

    #[error("worker error: {0}")]
    Worker(String),

    #[error(transparent)]
    Storage(#[from] StorageError),
}
