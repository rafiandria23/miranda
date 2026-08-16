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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_message_for_workflow_not_found() {
        let id = WorkflowId::new();
        let err = StorageError::WorkflowNotFound(id);
        assert_eq!(err.to_string(), format!("workflow not found: {id}"));
    }

    #[test]
    fn display_message_for_workflow_version_not_found() {
        let id = WorkflowVersionId::new();
        let err = StorageError::WorkflowVersionNotFound(id);
        assert_eq!(err.to_string(), format!("workflow version not found: {id}"));
    }

    #[test]
    fn display_message_for_execution_not_found() {
        let id = ExecutionId::new();
        let err = StorageError::ExecutionNotFound(id);
        assert_eq!(err.to_string(), format!("execution not found: {id}"));
    }

    #[test]
    fn display_message_for_snapshot_not_found() {
        let execution_id = ExecutionId::new();
        let err = StorageError::SnapshotNotFound {
            execution_id,
            version: 3,
        };
        assert_eq!(
            err.to_string(),
            format!("snapshot not found for execution {execution_id}, version 3")
        );
    }

    #[test]
    fn display_message_for_artifact_not_found() {
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();
        let err = StorageError::ArtifactNotFound {
            execution_id,
            task_id,
            path: "out.txt".to_string(),
        };
        assert_eq!(
            err.to_string(),
            format!(
                "artifact not found for execution {execution_id}, task {task_id}, path out.txt"
            )
        );
    }

    #[test]
    fn display_message_for_artifact_is_directory() {
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();
        let err = StorageError::ArtifactIsDirectory {
            execution_id,
            task_id,
            path: "out".to_string(),
        };
        assert_eq!(
            err.to_string(),
            format!(
                "artifact at path out for execution {execution_id}, task {task_id} is a directory, not a single file"
            )
        );
    }

    #[test]
    fn display_message_for_optimistic_lock_failed() {
        let id = ExecutionId::new();
        let err = StorageError::OptimisticLockFailed {
            id,
            expected: 1,
            actual: 2,
        };
        assert_eq!(
            err.to_string(),
            format!("concurrency conflict for execution {id}: expected version 1, found 2")
        );
    }

    #[test]
    fn display_message_for_conflict() {
        let err = StorageError::Conflict("duplicate".to_string());
        assert_eq!(err.to_string(), "conflict: duplicate");
    }

    #[test]
    fn display_message_for_backend() {
        let err = StorageError::Backend("connection refused".to_string());
        assert_eq!(err.to_string(), "backend error: connection refused");
    }

    #[test]
    fn display_message_for_serialization() {
        let err = StorageError::Serialization("invalid json".to_string());
        assert_eq!(err.to_string(), "serialization error: invalid json");
    }
}
