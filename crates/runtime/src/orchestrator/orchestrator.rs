use futures_util::future::join_all;
use miranda_core::{Execution, TaskStatus, WorkflowDefinition};
use tracing::{debug, instrument};

use crate::error::RuntimeError;
use crate::executor::{TaskExecutor, TaskResult};
use crate::scheduler;

#[derive(Debug)]
pub struct Orchestrator<E: TaskExecutor> {
    executor: E,
}

impl<E: TaskExecutor> Orchestrator<E> {
    pub fn new(executor: E) -> Self {
        Self { executor }
    }

    // #[instrument(skip(self, definition), fields(execution_id = ?execution.id()))]
    // #[instrument(skip(self, definition), fields(execution_id = ?execution.id()))]
    #[instrument(skip(self, definition), fields(execution_id = ?execution.id()))]
    pub async fn run(
        &self,
        mut execution: Execution,
        definition: &WorkflowDefinition,
    ) -> Result<Execution, RuntimeError> {
        execution.start()?;

        loop {
            let ready = scheduler::next_ready(&execution, definition);

            if ready.is_empty() {
                break;
            }

            debug!(batch_size = ready.len(), "dispatching ready tasks");

            for workflow_task_id in &ready {
                execution.start_task(*workflow_task_id, definition)?;
            }

            let futures = ready.iter().map(|workflow_task_id| {
                let workflow_task = definition
                    .task(*workflow_task_id)
                    .expect("ready task must exist in its own workflow definition");

                async move {
                    let result = self.executor.execute(workflow_task).await;

                    (*workflow_task_id, result)
                }
            });

            let results = join_all(futures).await;

            for (workflow_task_id, result) in results {
                match result {
                    TaskResult::Success => {
                        execution.complete_task(workflow_task_id)?;
                    }

                    TaskResult::Failure(_reason) => {
                        execution.fail_task(workflow_task_id)?;
                    }
                }
            }
        }

        let any_failed = execution
            .tasks()
            .iter()
            .any(|task| task.status() == TaskStatus::Failed);

        if any_failed {
            execution.fail()?;
        } else {
            execution.complete()?;
        }

        Ok(execution)
    }
}
