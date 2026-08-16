use thiserror::Error;

use crate::{
    execution::{AttemptStatus, ExecutionStatus, TaskStatus},
    id::WorkflowTaskId,
};

// Unified domain error type representing all invariant and validation failures in `miranda-core`.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ExecutionError {
    // =========================================================================
    // Workflow & Definition Invariants (Static DAG Templates)
    // =========================================================================
    #[error("workflow name is invalid")]
    InvalidWorkflowName,

    #[error("workflow version is invalid")]
    InvalidWorkflowVersion,

    #[error("workflow task type is invalid")]
    InvalidTaskType,

    #[error("workflow task '{0}' id is duplicated in definition")]
    DuplicateTaskId(WorkflowTaskId),

    #[error("workflow task '{0}' dependency is duplicated")]
    DuplicateDependency(WorkflowTaskId),

    #[error("workflow task '{0}' cannot depend on itself")]
    SelfDependency(WorkflowTaskId),

    #[error("workflow task dependency does not exist")]
    UnknownDependency,

    #[error("workflow definition contains a cycle")]
    CyclicDependency,

    #[error("workflow task {0:?} does not exist in the workflow definition")]
    UnknownWorkflowTask(WorkflowTaskId),

    // =========================================================================
    // Dynamic Execution & Instance State Machine Invariants
    // =========================================================================
    #[error("invalid execution transition from {from:?} to {to:?}")]
    InvalidExecutionTransition {
        from: ExecutionStatus,
        to: ExecutionStatus,
    },

    #[error("execution is not running")]
    ExecutionNotRunning,

    #[error("workflow task {0:?} is not ready")]
    TaskNotReady(WorkflowTaskId),

    #[error("workflow task {0:?} is not retryable")]
    TaskNotRetryable(WorkflowTaskId),

    #[error("workflow task {0:?} does not exist in the execution")]
    UnknownTask(WorkflowTaskId),

    #[error("execution cannot complete while tasks are incomplete")]
    IncompleteTasks,

    // =========================================================================
    // Dynamic Task Invariants
    // =========================================================================
    #[error("invalid task transition from {from:?} to {to:?}")]
    InvalidTaskTransition { from: TaskStatus, to: TaskStatus },

    // =========================================================================
    // Attempt Invariants
    // =========================================================================
    #[error("attempt number is invalid")]
    InvalidAttemptNumber,

    #[error("invalid attempt transition from {from:?} to {to:?}")]
    InvalidAttemptTransition {
        from: AttemptStatus,
        to: AttemptStatus,
    },

    // =========================================================================
    // Identifier Parsing Invariants
    // =========================================================================
    #[error("invalid domain identifier format: {0}")]
    InvalidIdFormat(String),
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_for_simple_variants() {
        assert_eq!(
            ExecutionError::InvalidWorkflowName.to_string(),
            "workflow name is invalid"
        );
        assert_eq!(
            ExecutionError::InvalidWorkflowVersion.to_string(),
            "workflow version is invalid"
        );
        assert_eq!(
            ExecutionError::InvalidTaskType.to_string(),
            "workflow task type is invalid"
        );
        assert_eq!(
            ExecutionError::UnknownDependency.to_string(),
            "workflow task dependency does not exist"
        );
        assert_eq!(
            ExecutionError::CyclicDependency.to_string(),
            "workflow definition contains a cycle"
        );
        assert_eq!(
            ExecutionError::ExecutionNotRunning.to_string(),
            "execution is not running"
        );
        assert_eq!(
            ExecutionError::IncompleteTasks.to_string(),
            "execution cannot complete while tasks are incomplete"
        );
        assert_eq!(
            ExecutionError::InvalidAttemptNumber.to_string(),
            "attempt number is invalid"
        );
    }

    #[test]
    fn display_messages_include_task_id() {
        let task_id = WorkflowTaskId::new();

        assert_eq!(
            ExecutionError::DuplicateTaskId(task_id).to_string(),
            format!("workflow task '{task_id}' id is duplicated in definition")
        );
        assert_eq!(
            ExecutionError::DuplicateDependency(task_id).to_string(),
            format!("workflow task '{task_id}' dependency is duplicated")
        );
        assert_eq!(
            ExecutionError::SelfDependency(task_id).to_string(),
            format!("workflow task '{task_id}' cannot depend on itself")
        );
        assert_eq!(
            ExecutionError::UnknownWorkflowTask(task_id).to_string(),
            format!("workflow task {task_id:?} does not exist in the workflow definition")
        );
        assert_eq!(
            ExecutionError::TaskNotReady(task_id).to_string(),
            format!("workflow task {task_id:?} is not ready")
        );
        assert_eq!(
            ExecutionError::TaskNotRetryable(task_id).to_string(),
            format!("workflow task {task_id:?} is not retryable")
        );
        assert_eq!(
            ExecutionError::UnknownTask(task_id).to_string(),
            format!("workflow task {task_id:?} does not exist in the execution")
        );
    }

    #[test]
    fn display_message_for_invalid_execution_transition() {
        let err = ExecutionError::InvalidExecutionTransition {
            from: ExecutionStatus::Pending,
            to: ExecutionStatus::Completed,
        };
        assert_eq!(
            err.to_string(),
            "invalid execution transition from Pending to Completed"
        );
    }

    #[test]
    fn display_message_for_invalid_task_transition() {
        let err = ExecutionError::InvalidTaskTransition {
            from: TaskStatus::Running,
            to: TaskStatus::Pending,
        };
        assert_eq!(
            err.to_string(),
            "invalid task transition from Running to Pending"
        );
    }

    #[test]
    fn display_message_for_invalid_attempt_transition() {
        let err = ExecutionError::InvalidAttemptTransition {
            from: AttemptStatus::Failed,
            to: AttemptStatus::Running,
        };
        assert_eq!(
            err.to_string(),
            "invalid attempt transition from Failed to Running"
        );
    }

    #[test]
    fn display_message_for_invalid_id_format() {
        let err = ExecutionError::InvalidIdFormat("bad-id".to_string());
        assert_eq!(err.to_string(), "invalid domain identifier format: bad-id");
    }

    #[test]
    fn errors_support_equality_and_clone() {
        let err = ExecutionError::InvalidWorkflowName;
        let cloned = err.clone();
        assert_eq!(err, cloned);
        assert_ne!(err, ExecutionError::InvalidTaskType);
    }
}
