use miranda_core::workflow::WorkflowTask;

use crate::{
    TaskExecutor, WorkerError,
    executor::{HttpExecutor, NoopExecutor, ShellExecutor, WaitExecutor},
};

pub struct DispatchExecutor;

impl TaskExecutor for DispatchExecutor {
    async fn execute(&self, task: &WorkflowTask) -> Result<(), WorkerError> {
        match task.task_type() {
            "shell" => ShellExecutor.execute(task).await,
            "http" => HttpExecutor.execute(task).await,
            "wait" => WaitExecutor.execute(task).await,
            "noop" => NoopExecutor.execute(task).await,
            other => Err(WorkerError::UnsupportedTaskType {
                task_type: other.to_owned(),
            }),
        }
    }
}
