use miranda_core::{
    error::ExecutionError,
    event::{Event, EventPayload},
    execution::{Execution, TaskStatus},
    id::WorkflowTaskId,
    workflow::WorkflowDefinition,
};
use miranda_worker::{TaskExecutor, WorkerError};

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

        tracing::debug!(task_id = %workflow_task_id, "starting task");

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
                tracing::debug!(task_id = %workflow_task_id, "task completed");

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
                tracing::warn!(task_id = %workflow_task_id, error = %worker_error, "task failed");

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

                    tracing::info!(
                        task_id = %workflow_task_id,
                        attempt = attempt_count + 1,
                        delay_ms = %delay_duration.as_millis(),
                        "retrying task"
                    );
                    delay(delay_duration).await;

                    Ok(TaskOutcomeResult::Retried)
                } else {
                    tracing::error!(task_id = %workflow_task_id, "max retries exceeded");

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
        execution::ExecutionStatus,
        id::{WorkflowTaskId, WorkflowVersionId},
    };
    use miranda_worker::InProcessExecutor;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;

    type BoxFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

    fn fake_executor(
        result: Result<(), WorkerError>,
    ) -> (
        InProcessExecutor<
            impl Fn(&miranda_core::workflow::WorkflowTask) -> BoxFuture + Send + Sync,
        >,
        Arc<AtomicUsize>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        let executor =
            InProcessExecutor::new(move |_task: &miranda_core::workflow::WorkflowTask| {
                counter.fetch_add(1, Ordering::SeqCst);
                let result = result.clone();
                Box::pin(async move { result }) as BoxFuture
            });

        (executor, calls)
    }

    fn single_task_definition(task_type: &str) -> (WorkflowDefinition, WorkflowTaskId) {
        let task_id = WorkflowTaskId::new();
        let task = miranda_core::workflow::WorkflowTask::new(task_id, task_type.to_owned(), vec![])
            .unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        (definition, task_id)
    }

    fn new_execution(definition: &WorkflowDefinition) -> Execution {
        Execution::from_definition(WorkflowVersionId::new(), definition).unwrap()
    }

    // ---------------------------------------------------------------
    // TaskDispatcher::dispatch
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn dispatch_errors_for_unknown_task() {
        let (definition, _task_id) = single_task_definition("send_email");
        let mut execution = new_execution(&definition);
        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                &definition,
            )
            .unwrap();

        let (executor, _calls) = fake_executor(Ok(()));
        let dispatcher = TaskDispatcher::new(&executor);

        let unknown_id = WorkflowTaskId::new();
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
    async fn dispatch_errors_when_task_is_not_pending_or_failed() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = new_execution(&definition);
        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                &definition,
            )
            .unwrap();
        execution
            .apply(
                Event::new(
                    execution.id(),
                    EventPayload::TaskStarted {
                        workflow_task_id: task_id,
                    },
                ),
                &definition,
            )
            .unwrap();

        let (executor, _calls) = fake_executor(Ok(()));
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
    async fn dispatch_starts_a_pending_task_and_invokes_the_executor() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = new_execution(&definition);
        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                &definition,
            )
            .unwrap();

        let (executor, calls) = fake_executor(Ok(()));
        let dispatcher = TaskDispatcher::new(&executor);

        let result = dispatcher
            .dispatch(&mut execution, &definition, task_id)
            .await
            .unwrap();

        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Running
        );
    }

    #[tokio::test]
    async fn dispatch_restarts_a_failed_task_and_invokes_the_executor() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = new_execution(&definition);
        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                &definition,
            )
            .unwrap();
        execution
            .apply(
                Event::new(
                    execution.id(),
                    EventPayload::TaskStarted {
                        workflow_task_id: task_id,
                    },
                ),
                &definition,
            )
            .unwrap();
        execution
            .apply(
                Event::new(
                    execution.id(),
                    EventPayload::TaskFailed {
                        workflow_task_id: task_id,
                        reason: "boom".to_owned(),
                        will_retry: true,
                    },
                ),
                &definition,
            )
            .unwrap();

        let (executor, calls) = fake_executor(Ok(()));
        let dispatcher = TaskDispatcher::new(&executor);

        let result = dispatcher
            .dispatch(&mut execution, &definition, task_id)
            .await
            .unwrap();

        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Running
        );
        assert_eq!(execution.task(task_id).unwrap().attempts().len(), 2);
    }

    #[tokio::test]
    async fn dispatch_propagates_the_executor_failure() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = new_execution(&definition);
        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                &definition,
            )
            .unwrap();

        let (executor, _calls) = fake_executor(Err(WorkerError::ExecutionFailed {
            message: "boom".to_owned(),
        }));
        let dispatcher = TaskDispatcher::new(&executor);

        let result = dispatcher
            .dispatch(&mut execution, &definition, task_id)
            .await
            .unwrap();

        assert!(matches!(
            result,
            Err(WorkerError::ExecutionFailed { message }) if message == "boom"
        ));
    }

    // ---------------------------------------------------------------
    // TaskOutcome::apply
    // ---------------------------------------------------------------

    fn running_execution_with_started_task(
        definition: &WorkflowDefinition,
        task_id: WorkflowTaskId,
    ) -> Execution {
        let mut execution = new_execution(definition);
        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                definition,
            )
            .unwrap();
        execution
            .apply(
                Event::new(
                    execution.id(),
                    EventPayload::TaskStarted {
                        workflow_task_id: task_id,
                    },
                ),
                definition,
            )
            .unwrap();

        execution
    }

    #[tokio::test]
    async fn apply_marks_task_completed_on_success() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = running_execution_with_started_task(&definition, task_id);

        let retry_policy = RetryPolicy::new(3, crate::retry::Backoff::Fixed(Duration::ZERO));
        let outcome = TaskOutcome::new(&retry_policy);

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
    async fn apply_retries_a_failure_when_attempts_remain() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = running_execution_with_started_task(&definition, task_id);

        let retry_policy = RetryPolicy::new(3, crate::retry::Backoff::Fixed(Duration::ZERO));
        let outcome = TaskOutcome::new(&retry_policy);

        let result = outcome
            .apply(
                &mut execution,
                &definition,
                task_id,
                Err(WorkerError::ExecutionFailed {
                    message: "transient".to_owned(),
                }),
            )
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
    async fn apply_fails_the_execution_once_retries_are_exhausted() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = running_execution_with_started_task(&definition, task_id);

        let retry_policy = RetryPolicy::new(1, crate::retry::Backoff::Fixed(Duration::ZERO));
        let outcome = TaskOutcome::new(&retry_policy);

        let err = outcome
            .apply(
                &mut execution,
                &definition,
                task_id,
                Err(WorkerError::ExecutionFailed {
                    message: "boom".to_owned(),
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            EngineError::ExecutionFailed(reason) if reason == "task execution failed: boom"
        ));
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Failed
        );
        assert_eq!(execution.status(), ExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn apply_never_retries_when_max_attempts_is_zero() {
        let (definition, task_id) = single_task_definition("send_email");
        let mut execution = running_execution_with_started_task(&definition, task_id);

        let retry_policy = RetryPolicy::new(0, crate::retry::Backoff::Fixed(Duration::ZERO));
        let outcome = TaskOutcome::new(&retry_policy);

        let err = outcome
            .apply(
                &mut execution,
                &definition,
                task_id,
                Err(WorkerError::ExecutionFailed {
                    message: "boom".to_owned(),
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, EngineError::ExecutionFailed(_)));
        assert_eq!(execution.status(), ExecutionStatus::Failed);
    }
}
