use miranda_core::id::{ExecutionId, WorkflowId, WorkflowTaskId, WorkflowVersionId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),

    #[error("workflow version not found: {0}")]
    WorkflowVersionNotFound(WorkflowVersionId),

    #[error("execution not found: {0}")]
    ExecutionNotFound(ExecutionId),

    #[error("snapshot not found for execution {execution_id}, version {version}")]
    SnapshotNotFound {
        execution_id: ExecutionId,
        version: u64,
    },

    #[error("artifact not found for execution {execution_id}, task {task_id}, path {path}")]
    ArtifactNotFound {
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: String,
    },

    #[error(
        "artifact at path {path} for execution {execution_id}, task {task_id} is a directory, not a single file"
    )]
    ArtifactIsDirectory {
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: String,
    },

    #[error("concurrency conflict for execution {id}: expected version {expected}, found {actual}")]
    OptimisticLockFailed {
        id: ExecutionId,
        expected: u64,
        actual: u64,
    },

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}
