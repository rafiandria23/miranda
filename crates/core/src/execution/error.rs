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

    #[error("workflow task {0:?} is not ready")]
    TaskNotReady(WorkflowTaskId),

    #[error("workflow task {0:?} does not exist in the execution")]
    UnknownTask(WorkflowTaskId),

    #[error("task transition failed: {0}")]
    TaskTransition(#[from] TaskError),
}
