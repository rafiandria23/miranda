pub mod in_process;

pub use in_process::InProcessExecutor;

use miranda_core::workflow::WorkflowTask;
use std::future::Future;

use crate::WorkerError;

pub trait TaskExecutor: Send + Sync {
    fn execute(&self, task: &WorkflowTask) -> impl Future<Output = Result<(), WorkerError>> + Send;
}
