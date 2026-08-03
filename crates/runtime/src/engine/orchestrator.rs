use futures_util::future::join_all;
use miranda_core::{
    execution::{Execution, TaskStatus},
    workflow::WorkflowDefinition,
};
use miranda_storage::WorkflowStore;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

use crate::{RetryPolicy, RuntimeError, TaskExecutor, TaskResult};

use super::{scheduler, timer};

#[derive(Debug)]
pub struct Orchestrator<E: TaskExecutor, S: WorkflowStore> {
    executor: E,
    store: Arc<S>,
    retry_policy: RetryPolicy,
}

impl<E: TaskExecutor, S: WorkflowStore> Orchestrator<E, S> {
    pub fn new(executor: E, store: Arc<S>) -> Self {
        Self {
            executor,
            store,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;

        self
    }

    #[instrument(skip(self, definition), fields(execution_id = %execution.id()))]
    pub async fn run(
        &self,
        mut execution: Execution,
        definition: &WorkflowDefinition,
    ) -> Result<Execution, RuntimeError> {
        execution.start()?;

        let mut version = 1u64;

        // 1. Initial state save to storage
        self.store.save_execution(&execution).await?;

        loop {
            let ready = scheduler::next_ready(&execution, definition);

            if ready.is_empty() {
                break;
            }

            debug!(batch_size = ready.len(), "dispatching ready tasks");

            for workflow_task_id in &ready {
                execution.start_task(*workflow_task_id, definition)?;
            }

            // Persist state update after starting tasks
            self.store.update_execution(&execution, version).await?;
            version += 1;

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

                    TaskResult::Failure(reason) => {
                        let task_state = execution
                            .task(workflow_task_id)
                            .expect("task must exist in execution state");

                        let current_attempts = task_state.attempts().len() as u32;

                        if self.retry_policy.should_retry(current_attempts) {
                            let backoff_delay =
                                self.retry_policy.delay_for_attempt(current_attempts + 1);

                            warn!(
                                %workflow_task_id,
                                attempt = current_attempts,
                                delay_ms = backoff_delay.as_millis(),
                                error = %reason,
                                "task failed, scheduling retry",
                            );

                            timer::delay(backoff_delay).await;
                            execution.retry_task(workflow_task_id, definition)?;
                        } else {
                            warn!(
                                %workflow_task_id,
                                attempt = current_attempts,
                                error = %reason,
                                "task exhausted retries",
                            );

                            execution.fail_task(workflow_task_id)?;
                        }
                    }
                }
            }

            // Persist state update after handling batch task results
            self.store.update_execution(&execution, version).await?;
            version += 1;
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

        // Final state persist upon completion/failure
        self.store.update_execution(&execution, version).await?;

        Ok(execution)
    }
}
