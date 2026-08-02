use miranda_core::id::{ExecutionId, WorkflowId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),

    #[error("execution not found: {0}")]
    ExecutionNotFound(ExecutionId),

    #[error("concurrency conflict for execution {id}: expected version {expected}, found {actual}")]
    OptimisticLockFailed {
        id: ExecutionId,
        expected: u64,
        actual: u64,
    },

    #[error("database error: {0}")]
    Database(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}
