use miranda_core::workflow::WorkflowTask;
use std::time::Duration;

use crate::{
    TaskExecutor, WorkerError,
    executor::{HttpExecutor, NoopExecutor, ShellExecutor, WaitExecutor},
};

pub struct DispatchExecutor {
    http: HttpExecutor,
}

impl DispatchExecutor {
    pub fn new() -> Self {
        Self {
            http: HttpExecutor::new(),
        }
    }
}

impl Default for DispatchExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskExecutor for DispatchExecutor {
    async fn execute(
        &self,
        task: &WorkflowTask,
        timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        match task.task_type() {
            "shell" => ShellExecutor.execute(task, timeout).await,
            "http" => self.http.execute(task, timeout).await,
            "wait" => WaitExecutor.execute(task, timeout).await,
            "noop" => NoopExecutor.execute(task, timeout).await,
            other => Err(WorkerError::UnsupportedTaskType {
                task_type: other.to_owned(),
            }),
        }
    }
}
