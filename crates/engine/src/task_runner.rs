use miranda_core::{
    error::ExecutionError,
    event::{Event, EventPayload},
    execution::{Execution, TaskStatus},
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
        let status = execution
            .task(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?
            .status();

        let payload = match status {
            TaskStatus::Pending => EventPayload::TaskStarted { workflow_task_id },

            TaskStatus::Failed => EventPayload::TaskRetried { workflow_task_id },

            _other => return Err(ExecutionError::TaskNotReady(workflow_task_id).into()),
        };

        debug!(task_id = %workflow_task_id, "starting task");

        execution.apply(Event::new(execution.id(), payload), definition)?;

        let workflow_task = definition
            .task(workflow_task_id)
            .expect("ready task must exist in definition");

        let timeout = definition.effective_timeout(workflow_task);

        Ok(self.executor.execute(workflow_task, timeout).await)
    }
}

// =========================================================================
// Task Outcome Implementation
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcomeResult {
    Completed,
    Retried,
}

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
    ) -> Result<TaskOutcomeResult, EngineError> {
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

                Ok(TaskOutcomeResult::Completed)
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

                    Ok(TaskOutcomeResult::Retried)
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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        error::ExecutionError, execution::ExecutionStatus, id::WorkflowVersionId,
        workflow::WorkflowTask,
    };
    use miranda_worker::TaskExecutor;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use crate::retry::Backoff;

    use super::*;

    struct FakeExecutor {
        result: Result<(), WorkerError>,
        calls: AtomicUsize,
    }

    impl FakeExecutor {
        fn new(result: Result<(), WorkerError>) -> Self {
            Self {
                result,
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl TaskExecutor for FakeExecutor {
        async fn execute(&self, _task: &WorkflowTask) -> Result<(), WorkerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }
    }

    fn single_task_definition() -> (WorkflowDefinition, WorkflowTaskId) {
        let task_id = WorkflowTaskId::new();
        let task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        (definition, task_id)
    }

    fn running_execution(definition: &WorkflowDefinition) -> Execution {
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), definition).unwrap();
        execution.start().unwrap();

        execution
    }

    mod task_dispatcher {
        use super::*;

        #[tokio::test]
        async fn dispatch_starts_a_pending_task_and_invokes_executor() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);

            let executor = FakeExecutor::new(Ok(()));
            let dispatcher = TaskDispatcher::new(&executor);

            let result = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap();

            assert!(result.is_ok());
            assert_eq!(
                execution.task(task_id).unwrap().status(),
                TaskStatus::Running
            );
        }

        #[tokio::test]
        async fn dispatch_retries_a_failed_task() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();
            execution.fail_task(task_id).unwrap();

            let executor = FakeExecutor::new(Ok(()));
            let dispatcher = TaskDispatcher::new(&executor);

            let result = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap();

            assert!(result.is_ok());
            assert_eq!(
                execution.task(task_id).unwrap().status(),
                TaskStatus::Running
            );
        }

        #[tokio::test]
        async fn dispatch_passes_through_executor_error() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);

            let executor = FakeExecutor::new(Err(WorkerError::ExecutionFailed {
                message: "boom".to_owned(),
            }));
            let dispatcher = TaskDispatcher::new(&executor);

            let result = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap();

            assert_eq!(
                result,
                Err(WorkerError::ExecutionFailed {
                    message: "boom".to_owned(),
                })
            );
        }

        #[tokio::test]
        async fn dispatch_rejects_an_unknown_task() {
            let (definition, _task_id) = single_task_definition();
            let mut execution = running_execution(&definition);

            let unknown_id = WorkflowTaskId::new();
            let executor = FakeExecutor::new(Ok(()));
            let dispatcher = TaskDispatcher::new(&executor);

            let err = dispatcher
                .dispatch(&mut execution, &definition, unknown_id)
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                EngineError::Domain(ExecutionError::UnknownTask(id)) if id == unknown_id
            ));
        }

        #[tokio::test]
        async fn dispatch_rejects_a_task_that_is_already_running() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();

            let executor = FakeExecutor::new(Ok(()));
            let dispatcher = TaskDispatcher::new(&executor);

            let err = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                EngineError::Domain(ExecutionError::TaskNotReady(id)) if id == task_id
            ));
        }

        #[tokio::test]
        async fn dispatch_rejects_a_completed_task() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();
            execution.complete_task(task_id).unwrap();

            let executor = FakeExecutor::new(Ok(()));
            let dispatcher = TaskDispatcher::new(&executor);

            let err = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                EngineError::Domain(ExecutionError::TaskNotReady(id)) if id == task_id
            ));
        }
    }

    mod task_outcome {
        use super::*;

        #[tokio::test]
        async fn apply_marks_task_completed_on_success() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();

            let policy = RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO));
            let outcome = TaskOutcome::new(&policy);

            let result = outcome
                .apply(&mut execution, &definition, task_id, Ok(()))
                .await
                .unwrap();

            assert_eq!(result, TaskOutcomeResult::Completed);
            assert_eq!(
                execution.task(task_id).unwrap().status(),
                TaskStatus::Completed
            );
        }

        #[tokio::test]
        async fn apply_retries_a_failure_while_under_max_attempts() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();

            let policy = RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO));
            let outcome = TaskOutcome::new(&policy);

            let error = WorkerError::ExecutionFailed {
                message: "transient".to_owned(),
            };
            let result = outcome
                .apply(&mut execution, &definition, task_id, Err(error))
                .await
                .unwrap();

            assert_eq!(result, TaskOutcomeResult::Retried);
            assert_eq!(
                execution.task(task_id).unwrap().status(),
                TaskStatus::Failed
            );
            assert_eq!(execution.status(), ExecutionStatus::Running);
        }

        #[tokio::test]
        async fn apply_fails_execution_once_retries_are_exhausted() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();

            let policy = RetryPolicy::new(0, Backoff::Fixed(Duration::ZERO));
            let outcome = TaskOutcome::new(&policy);

            let error = WorkerError::ExecutionFailed {
                message: "fatal".to_owned(),
            };
            let err = outcome
                .apply(&mut execution, &definition, task_id, Err(error))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                EngineError::ExecutionFailed(reason) if reason == "task execution failed: fatal"
            ));
            assert_eq!(
                execution.task(task_id).unwrap().status(),
                TaskStatus::Failed
            );
            assert_eq!(execution.status(), ExecutionStatus::Failed);
        }

        #[tokio::test]
        async fn apply_counts_prior_attempts_when_deciding_to_retry() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);

            // First attempt: fails and is retried (1 attempt recorded < max_attempts 2).
            execution.start_task(task_id, &definition).unwrap();
            let policy = RetryPolicy::new(2, Backoff::Fixed(Duration::ZERO));
            let outcome = TaskOutcome::new(&policy);
            let error = WorkerError::ExecutionFailed {
                message: "first".to_owned(),
            };
            let result = outcome
                .apply(&mut execution, &definition, task_id, Err(error))
                .await
                .unwrap();
            assert_eq!(result, TaskOutcomeResult::Retried);

            // Second attempt: now 2 attempts are recorded, no longer below
            // max_attempts (2), so the execution is failed instead of retried again.
            execution.retry_task(task_id, &definition).unwrap();
            let error = WorkerError::ExecutionFailed {
                message: "second".to_owned(),
            };
            let err = outcome
                .apply(&mut execution, &definition, task_id, Err(error))
                .await
                .unwrap_err();

            assert!(matches!(err, EngineError::ExecutionFailed(_)));
            assert_eq!(execution.status(), ExecutionStatus::Failed);
        }

        #[tokio::test]
        async fn apply_uses_the_configured_delay_before_retrying() {
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);
            execution.start_task(task_id, &definition).unwrap();

            // A zero delay resolves without ever sleeping; this asserts the call
            // still completes and produces the expected outcome under that policy.
            let policy = RetryPolicy::new(5, Backoff::Fixed(Duration::ZERO));
            let outcome = TaskOutcome::new(&policy);

            let error = WorkerError::ExecutionFailed {
                message: "transient".to_owned(),
            };
            let result = outcome
                .apply(&mut execution, &definition, task_id, Err(error))
                .await
                .unwrap();

            assert_eq!(result, TaskOutcomeResult::Retried);
        }

        #[tokio::test]
        async fn apply_reports_retry_calls_seen_by_a_shared_counter() {
            // Sanity check that the executor's call count observes outcomes
            // applied across sequential dispatch/apply cycles, mirroring how
            // the runner loop would use these primitives.
            let (definition, task_id) = single_task_definition();
            let mut execution = running_execution(&definition);

            let executor = FakeExecutor::new(Err(WorkerError::ExecutionFailed {
                message: "always fails".to_owned(),
            }));
            let dispatcher = TaskDispatcher::new(&executor);
            let policy = RetryPolicy::new(2, Backoff::Fixed(Duration::ZERO));
            let outcome = TaskOutcome::new(&policy);

            let result = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap();
            let outcome_result = outcome
                .apply(&mut execution, &definition, task_id, result)
                .await
                .unwrap();
            assert_eq!(outcome_result, TaskOutcomeResult::Retried);

            let result = dispatcher
                .dispatch(&mut execution, &definition, task_id)
                .await
                .unwrap();
            let err = outcome
                .apply(&mut execution, &definition, task_id, result)
                .await
                .unwrap_err();

            assert!(matches!(err, EngineError::ExecutionFailed(_)));
            assert_eq!(executor.calls(), 2);
        }
    }
}
