use miranda_core::workflow::WorkflowTask;
use std::future::Future;

use super::TaskResult;

pub trait TaskExecutor {
    fn execute(&self, task: &WorkflowTask) -> impl Future<Output = TaskResult>;
}

pub struct NoopExecutor;

impl TaskExecutor for NoopExecutor {
    async fn execute(&self, task: &WorkflowTask) -> TaskResult {
        TaskResult::Success
    }
}
