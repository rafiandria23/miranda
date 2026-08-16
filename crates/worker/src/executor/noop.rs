use miranda_core::{id::ExecutionId, workflow::WorkflowTask};
use std::time::Duration;

use crate::{TaskExecutor, WorkerError};

pub struct NoopExecutor;

impl TaskExecutor for NoopExecutor {
    async fn execute(
        &self,
        _execution_id: ExecutionId,
        _task: &WorkflowTask,
        _timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::WorkflowTaskId;

    use super::*;

    #[tokio::test]
    async fn execute_always_succeeds() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "noop".to_string(), vec![]).unwrap();

        let result = NoopExecutor.execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_ignores_timeout_argument() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "noop".to_string(), vec![]).unwrap();

        let result = NoopExecutor
            .execute(ExecutionId::new(), &task, Some(Duration::from_secs(1)))
            .await;

        assert!(result.is_ok());
    }
}
