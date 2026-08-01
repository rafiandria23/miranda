use thiserror::Error;

use super::ExecutionStatus;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("invalid execution transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: ExecutionStatus,
        to: ExecutionStatus,
    },
}
