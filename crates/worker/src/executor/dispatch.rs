use miranda_core::{id::ExecutionId, workflow::WorkflowTask};
use miranda_storage::artifact_store::ArtifactStore;
use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::{
    error::WorkerError,
    executor::{HttpExecutor, NoopExecutor, ShellExecutor, TaskExecutor, WaitExecutor},
};

pub struct DispatchExecutor {
    http: HttpExecutor,
    shell: ShellExecutor,
}

impl DispatchExecutor {
    pub fn new(artifact_store: Arc<dyn ArtifactStore>, work_dir_root: PathBuf) -> Self {
        Self {
            http: HttpExecutor::new(),
            shell: ShellExecutor::new(artifact_store, work_dir_root),
        }
    }
}

impl TaskExecutor for DispatchExecutor {
    async fn execute(
        &self,
        execution_id: ExecutionId,
        task: &WorkflowTask,
        timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        match task.task_type() {
            "shell" => self.shell.execute(execution_id, task, timeout).await,
            "http" => self.http.execute(execution_id, task, timeout).await,
            "wait" => WaitExecutor.execute(execution_id, task, timeout).await,
            "noop" => NoopExecutor.execute(execution_id, task, timeout).await,
            other => Err(WorkerError::UnsupportedTaskType {
                task_type: other.to_owned(),
            }),
        }
    }
}
