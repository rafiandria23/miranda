use thiserror::Error;

use super::AttemptStatus;

#[derive(Debug, Error)]
pub enum AttemptError {
    #[error("attempt number must be greater than zero")]
    InvalidNumber,

    #[error("invalid attempt transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: AttemptStatus,
        to: AttemptStatus,
    },
}
