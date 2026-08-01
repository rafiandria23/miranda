use thiserror::Error;

use crate::AttemptError;

use super::TaskStatus;

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("invalid task transition from {from:?} to {to:?}")]
    InvalidTransition { from: TaskStatus, to: TaskStatus },

    #[error("task attempt is invalid: {0}")]
    InvalidAttempt(#[from] AttemptError),
}
