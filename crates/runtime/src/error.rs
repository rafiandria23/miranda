use miranda_core::ExecutionError;
use thiserror::Error;

use crate::{ExecutorError, RetryError, SchedulerError, TimerError, WorkerError};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Domain(#[from] ExecutionError),

    #[error(transparent)]
    Executor(#[from] ExecutorError),

    #[error(transparent)]
    Scheduler(#[from] SchedulerError),

    #[error(transparent)]
    Retry(#[from] RetryError),

    #[error(transparent)]
    Timer(#[from] TimerError),

    #[error(transparent)]
    Worker(#[from] WorkerError),
}
