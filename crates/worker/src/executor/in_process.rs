use miranda_core::workflow::WorkflowTask;
use std::{future::Future, time::Duration};

use crate::{TaskExecutor, WorkerError};

pub struct InProcessExecutor<F> {
    handler: F,
}

impl<F> InProcessExecutor<F> {
    pub fn new(handler: F) -> Self {
        Self { handler }
    }
}

impl<F, Fut> TaskExecutor for InProcessExecutor<F>
where
    F: Fn(&WorkflowTask) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), WorkerError>> + Send,
{
    async fn execute(
        &self,
        task: &WorkflowTask,
        _timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        (self.handler)(task).await
    }
}
