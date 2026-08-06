use miranda_core::workflow::WorkflowTask;

use crate::{TaskExecutor, WorkerError};

pub struct NoopExecutor;

impl TaskExecutor for NoopExecutor {
    async fn execute(&self, _task: &WorkflowTask) -> Result<(), WorkerError> {
        Ok(())
    }
}
