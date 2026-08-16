use miranda_core::{ExecutionError, id::WorkerId};
use miranda_engine::EngineError;
use miranda_storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error(transparent)]
    Domain(#[from] ExecutionError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Engine(#[from] EngineError),

    #[error("worker not found: {0}")]
    WorkerNotFound(WorkerId),

    #[error("worker unavailable: {0}")]
    WorkerUnavailable(WorkerId),

    #[error("task queue error: {0}")]
    Queue(String),

    #[error("routing error: {0}")]
    Routing(String),

    #[error("scheduler error: {0}")]
    Scheduler(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::WorkerId;

    use super::*;

    #[test]
    fn display_message_for_worker_not_found() {
        let worker_id = WorkerId::new();
        let err = ControlPlaneError::WorkerNotFound(worker_id);
        assert_eq!(err.to_string(), format!("worker not found: {worker_id}"));
    }

    #[test]
    fn display_message_for_worker_unavailable() {
        let worker_id = WorkerId::new();
        let err = ControlPlaneError::WorkerUnavailable(worker_id);
        assert_eq!(err.to_string(), format!("worker unavailable: {worker_id}"));
    }

    #[test]
    fn display_message_for_queue_error() {
        let err = ControlPlaneError::Queue("full".to_string());
        assert_eq!(err.to_string(), "task queue error: full");
    }

    #[test]
    fn display_message_for_routing_error() {
        let err = ControlPlaneError::Routing("no worker".to_string());
        assert_eq!(err.to_string(), "routing error: no worker");
    }

    #[test]
    fn display_message_for_scheduler_error() {
        let err = ControlPlaneError::Scheduler("deadlock".to_string());
        assert_eq!(err.to_string(), "scheduler error: deadlock");
    }

    #[test]
    fn display_message_for_invalid_request() {
        let err = ControlPlaneError::InvalidRequest("bad payload".to_string());
        assert_eq!(err.to_string(), "invalid request: bad payload");
    }

    #[test]
    fn domain_error_wraps_and_displays_transparently() {
        let domain_err = ExecutionError::InvalidWorkflowName;
        let err: ControlPlaneError = domain_err.clone().into();

        assert_eq!(err.to_string(), domain_err.to_string());
        assert!(matches!(err, ControlPlaneError::Domain(_)));
    }
}
