use thiserror::Error;

use crate::{TaskError, id::WorkflowTaskId};

use super::ExecutionStatus;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("invalid execution transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: ExecutionStatus,
        to: ExecutionStatus,
    },

    #[error("execution is not running")]
    ExecutionNotRunning,

    #[error("workflow task {0:?} is not ready")]
    TaskNotReady(WorkflowTaskId),

    #[error("workflow task {0:?} does not exist in the workflow destination")]
    UnknownWorkflowTask(WorkflowTaskId),

    #[error("workflow task {0:?} does not exist in the execution")]
    UnknownTask(WorkflowTaskId),

    #[error("execution cannot complete while tasks are incomplete")]
    IncompleteTasks,

    #[error("task transition failed: {0}")]
    TaskTransition(#[from] TaskError),
}
