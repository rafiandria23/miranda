use std::future::Future;

use miranda_core::WorkflowTask;

use super::ExecutorError;

pub enum TaskResult {
    Success,
    Failure(ExecutorError),
}

pub trait TaskExecutor {
    fn execute(&self, task: &WorkflowTask) -> impl Future<Output = TaskResult>;
}

pub struct NoopExecutor;

impl TaskExecutor for NoopExecutor {
    async fn execute(&self, task: &WorkflowTask) -> TaskResult {
        TaskResult::Success
    }
}
