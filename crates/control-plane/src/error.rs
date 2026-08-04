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
