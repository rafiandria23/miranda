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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::{ExecutionId, WorkflowTaskId};
    use miranda_storage::filesystem::FilesystemStore;

    use super::*;

    fn unique_temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "miranda-dispatch-test-{label}-{}",
            ExecutionId::new()
        ))
    }

    fn dispatcher() -> DispatchExecutor {
        DispatchExecutor::new(
            Arc::new(FilesystemStore::new(unique_temp_dir("store"))),
            unique_temp_dir("work"),
        )
    }

    fn task(task_type: &str) -> WorkflowTask {
        WorkflowTask::new(WorkflowTaskId::new(), task_type.to_owned(), vec![]).unwrap()
    }

    #[tokio::test]
    async fn dispatches_noop_tasks() {
        let dispatcher = dispatcher();
        let result = dispatcher
            .execute(ExecutionId::new(), &task("noop"), None)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatches_wait_tasks() {
        let dispatcher = dispatcher();
        let task = task("wait").with_config(serde_json::json!({ "type": "wait", "duration": 0 }));

        let result = dispatcher.execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn rejects_unsupported_task_types() {
        let dispatcher = dispatcher();

        let result = dispatcher
            .execute(ExecutionId::new(), &task("carrier_pigeon"), None)
            .await;

        assert!(matches!(
            result,
            Err(WorkerError::UnsupportedTaskType { task_type }) if task_type == "carrier_pigeon"
        ));
    }
}
