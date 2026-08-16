use miranda_core::ExecutionError;
use miranda_storage::StorageError;
use miranda_worker::WorkerError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Domain(#[from] ExecutionError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Worker(#[from] WorkerError),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("no ready tasks and execution not finished")]
    Deadlock,
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_worker::WorkerError;

    use super::*;

    #[test]
    fn display_message_for_execution_failed() {
        let err = EngineError::ExecutionFailed("boom".to_string());
        assert_eq!(err.to_string(), "execution failed: boom");
    }

    #[test]
    fn display_message_for_deadlock() {
        let err = EngineError::Deadlock;
        assert_eq!(err.to_string(), "no ready tasks and execution not finished");
    }

    #[test]
    fn domain_error_wraps_and_displays_transparently() {
        let domain_err = ExecutionError::InvalidWorkflowName;
        let err: EngineError = domain_err.clone().into();

        assert_eq!(err.to_string(), domain_err.to_string());
        assert!(matches!(err, EngineError::Domain(_)));
    }

    #[test]
    fn worker_error_wraps_and_displays_transparently() {
        let worker_err = WorkerError::ExecutionFailed {
            message: "failed".to_string(),
        };
        let err: EngineError = worker_err.clone().into();

        assert_eq!(err.to_string(), worker_err.to_string());
        assert!(matches!(err, EngineError::Worker(_)));
    }
}
