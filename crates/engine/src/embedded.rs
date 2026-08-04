use miranda_core::{
    event::{Event, EventPayload},
    execution::Execution,
    workflow::WorkflowDefinition,
};
use miranda_scheduler::RetryPolicy;
use miranda_storage::WorkflowStore;
use miranda_worker::TaskExecutor;
use tracing::{debug, error, info, instrument, warn};

use crate::EngineError;

pub struct EmbeddedEngine<E, S> {
    executor: E,
    store: S,
    retry_policy: RetryPolicy,
}

impl<E, S> EmbeddedEngine<E, S> {
    pub fn new(executor: E, store: S) -> Self {
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
}

impl<E, S> EmbeddedEngine<E, S>
where
    E: TaskExecutor,
    S: WorkflowStore,
{
    #[instrument(skip(self, definition), fields(execution_id = %execution.id()))]
    pub async fn run(
        &self,
        mut execution: Execution,
        definition: &WorkflowDefinition,
    ) -> Result<Execution, EngineError> {
        info!("starting execution");

        let mut version = 1u64;

        execution.apply(
            Event::new(execution.id(), EventPayload::ExecutionStarted),
            definition,
        )?;

        self.store.save_execution(&execution).await?;
        version += 1;

        loop {
            let ready = execution.ready_tasks(definition);

            if ready.is_empty() {
                if execution.is_finished() {
                    break;
                }
                error!("deadlock: no ready tasks but execution not finished");
                return Err(EngineError::Deadlock);
            }

            for workflow_task_id in ready {
                debug!(task_id = %workflow_task_id, "starting task");

                execution.apply(
                    Event::new(
                        execution.id(),
                        EventPayload::TaskStarted { workflow_task_id },
                    ),
                    definition,
                )?;

                self.store.update_execution(&execution, version).await?;
                version += 1;

                let workflow_task = definition
                    .task(workflow_task_id)
                    .expect("ready task must exist in definition");

                let result = self.executor.execute(workflow_task).await;

                match result {
                    Ok(()) => {
                        debug!(task_id = %workflow_task_id, "task completed");

                        execution.apply(
                            Event::new(
                                execution.id(),
                                EventPayload::TaskCompleted { workflow_task_id },
                            ),
                            definition,
                        )?;
                    }
                    Err(worker_error) => {
                        warn!(task_id = %workflow_task_id, error = %worker_error, "task failed");

                        execution.apply(
                            Event::new(
                                execution.id(),
                                EventPayload::TaskFailed {
                                    workflow_task_id,
                                    reason: worker_error.to_string(),
                                },
                            ),
                            definition,
                        )?;

                        let task = execution.task(workflow_task_id).expect("task must exist");
                        let attempt_count = task.attempts().len() as u32;

                        if self.retry_policy.should_retry(attempt_count) {
                            let delay = self.retry_policy.delay_for_attempt(attempt_count + 1);
                            info!(
                                task_id = %workflow_task_id,
                                attempt = attempt_count + 1,
                                delay_ms = %delay.as_millis(),
                                "retrying task"
                            );
                            miranda_scheduler::delay(delay).await;

                            execution.apply(
                                Event::new(
                                    execution.id(),
                                    EventPayload::TaskRetried { workflow_task_id },
                                ),
                                definition,
                            )?;
                        } else {
                            error!(task_id = %workflow_task_id, "max retries exceeded");

                            execution.apply(
                                Event::new(
                                    execution.id(),
                                    EventPayload::ExecutionFailed {
                                        reason: worker_error.to_string(),
                                    },
                                ),
                                definition,
                            )?;

                            self.store.update_execution(&execution, version).await?;

                            return Err(EngineError::ExecutionFailed(worker_error.to_string()));
                        }
                    }
                }

                self.store.update_execution(&execution, version).await?;
                version += 1;
            }
        }

        execution.apply(
            Event::new(execution.id(), EventPayload::ExecutionCompleted),
            definition,
        )?;

        self.store.update_execution(&execution, version).await?;

        info!("execution completed");

        Ok(execution)
    }
}
