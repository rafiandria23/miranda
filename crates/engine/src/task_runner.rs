use miranda_core::{
    event::{Event, EventPayload},
    execution::Execution,
    id::WorkflowTaskId,
    workflow::WorkflowDefinition,
};
use miranda_worker::{TaskExecutor, WorkerError};
use tracing::{debug, error, info, warn};

use crate::{EngineError, retry::RetryPolicy, timer::delay};

// =========================================================================
// Task Dispatcher Implementation
// =========================================================================

pub struct TaskDispatcher<'a, E> {
    executor: &'a E,
}

impl<'a, E> TaskDispatcher<'a, E>
where
    E: TaskExecutor,
{
    pub fn new(executor: &'a E) -> Self {
        Self { executor }
    }

    pub async fn dispatch(
        &self,
        execution: &mut Execution,
        definition: &WorkflowDefinition,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<Result<(), WorkerError>, EngineError> {
        debug!(task_id = %workflow_task_id, "starting task");

        execution.apply(
            Event::new(
                execution.id(),
                EventPayload::TaskStarted { workflow_task_id },
            ),
            definition,
        )?;

        let workflow_task = definition
            .task(workflow_task_id)
            .expect("ready task must exist in definition");

        Ok(self.executor.execute(workflow_task).await)
    }
}

// =========================================================================
// Task Outcome Implementation
// =========================================================================

pub struct TaskOutcome<'a> {
    retry_policy: &'a RetryPolicy,
}

impl<'a> TaskOutcome<'a> {
    pub fn new(retry_policy: &'a RetryPolicy) -> Self {
        Self { retry_policy }
    }

    pub async fn apply(
        &self,
        execution: &mut Execution,
        definition: &WorkflowDefinition,
        workflow_task_id: WorkflowTaskId,
        result: Result<(), WorkerError>,
    ) -> Result<(), EngineError> {
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

                Ok(())
            }

            Err(worker_error) => {
                warn!(task_id = %workflow_task_id, error = %worker_error, "task failed");

                let attempt_count = execution
                    .task(workflow_task_id)
                    .map(|t| t.attempts().len() as u32)
                    .unwrap_or(0);
                let will_retry = self.retry_policy.should_retry(attempt_count);

                execution.apply(
                    Event::new(
                        execution.id(),
                        EventPayload::TaskFailed {
                            workflow_task_id,
                            reason: worker_error.to_string(),
                            will_retry,
                        },
                    ),
                    definition,
                )?;

                if will_retry {
                    let delay_duration = self.retry_policy.delay_for_attempt(attempt_count + 1);

                    info!(
                        task_id = %workflow_task_id,
                        attempt = attempt_count + 1,
                        delay_ms = %delay_duration.as_millis(),
                        "retrying task"
                    );

                    delay(delay_duration).await;

                    execution.apply(
                        Event::new(
                            execution.id(),
                            EventPayload::TaskRetried { workflow_task_id },
                        ),
                        definition,
                    )?;

                    Ok(())
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

                    Err(EngineError::ExecutionFailed(worker_error.to_string()))
                }
            }
        }
    }
}
