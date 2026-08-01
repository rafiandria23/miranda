use thiserror::Error;

use super::TaskStatus;

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("invalid task transition from {from:?} to {to:?}")]
    InvalidTransition { from: TaskStatus, to: TaskStatus },
}
