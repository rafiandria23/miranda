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
