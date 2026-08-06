use miranda_core::id::{ExecutionId, WorkflowId, WorkflowVersionId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),

    #[error("workflow version not found: {0}")]
    WorkflowVersionNotFound(WorkflowVersionId),

    #[error("execution not found: {0}")]
    ExecutionNotFound(ExecutionId),

    #[error("concurrency conflict for execution {id}: expected version {expected}, found {actual}")]
    OptimisticLockFailed {
        id: ExecutionId,
        expected: u64,
        actual: u64,
    },

    #[error("backend error: {0}")]
    Backend(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}
