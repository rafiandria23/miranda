use miranda_core::ExecutionError;
use miranda_storage::StorageError;
use miranda_worker::WorkerError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Domain(#[from] ExecutionError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Worker(#[from] WorkerError),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("no ready tasks and execution not finished")]
    Deadlock,
}
