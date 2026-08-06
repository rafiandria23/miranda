use miranda_core::workflow::WorkflowTask;
use std::time::Duration;

use crate::{TaskExecutor, WorkerError};

pub struct NoopExecutor;

impl TaskExecutor for NoopExecutor {
    async fn execute(
        &self,
        _task: &WorkflowTask,
        _timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}
