use miranda_core::{
    event::{Event, EventPayload},
    execution::{Execution, NextAction},
    workflow::WorkflowDefinition,
};
use miranda_storage::WorkflowStore;
use miranda_worker::TaskExecutor;

use crate::{
    EngineError,
    retry::RetryPolicy,
    task_runner::{TaskDispatcher, TaskOutcome, TaskOutcomeResult},
};

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
    #[tracing::instrument(skip(self, definition), fields(execution_id = %execution.id()))]
    pub async fn run(
        &self,
        mut execution: Execution,
        definition: &WorkflowDefinition,
    ) -> Result<Execution, EngineError> {
        tracing::info!("starting execution");

        execution.apply(
            Event::new(execution.id(), EventPayload::ExecutionStarted),
            definition,
        )?;
        self.store.save_execution(&execution).await?;

        let dispatcher = TaskDispatcher::new(&self.executor);
        let outcome = TaskOutcome::new(&self.retry_policy);

        let (_, mut version) = self.store.get_execution(execution.id()).await?;

        loop {
            match execution.next_action(definition) {
                NextAction::Finished(_) => break,

                NextAction::Deadlocked => {
                    tracing::error!("deadlock: no ready tasks but execution not finished");

                    return Err(EngineError::Deadlock);
                }

                NextAction::RunTasks(ready) => {
                    for workflow_task_id in ready {
                        let workflow_task = definition
                            .task(workflow_task_id)
                            .expect("ready task must exist in definition");

                        if workflow_task.task_type() == "noop" {
                            tracing::debug!(task_id = %workflow_task_id, "noop task, completing without dispatch");

                            execution.apply(
                                Event::new(
                                    execution.id(),
                                    EventPayload::TaskStarted { workflow_task_id },
                                ),
                                definition,
                            )?;
                            execution.apply(
                                Event::new(
                                    execution.id(),
                                    EventPayload::TaskCompleted { workflow_task_id },
                                ),
                                definition,
                            )?;

                            self.store.update_execution(&execution, version).await?;
                            version += 1;

                            continue;
                        }

                        loop {
                            let result = dispatcher
                                .dispatch(&mut execution, definition, workflow_task_id)
                                .await?;

                            let outcome_result = outcome
                                .apply(&mut execution, definition, workflow_task_id, result)
                                .await;

                            self.store.update_execution(&execution, version).await?;
                            version += 1;

                            match outcome_result? {
                                TaskOutcomeResult::Completed => break,
                                TaskOutcomeResult::Retried => continue,
                            }
                        }
                    }
                }
            }
        }

        execution.apply(
            Event::new(execution.id(), EventPayload::ExecutionCompleted),
            definition,
        )?;
        self.store.update_execution(&execution, version).await?;

        tracing::info!("execution completed");

        Ok(execution)
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        execution::{ExecutionStatus, TaskStatus},
        id::{WorkflowTaskId, WorkflowVersionId},
        workflow::WorkflowTask,
    };
    use miranda_storage::InMemoryStore;
    use miranda_worker::{InProcessExecutor, WorkerError};
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
        InProcessExecutor<impl Fn(&WorkflowTask) -> BoxFuture + Send + Sync>,
        Arc<AtomicUsize>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        let executor = InProcessExecutor::new(move |_task: &WorkflowTask| {
            counter.fetch_add(1, Ordering::SeqCst);
            let result = result.clone();
            Box::pin(async move { result }) as BoxFuture
        });

        (executor, calls)
    }

    fn single_task_definition(task_type: &str) -> (WorkflowDefinition, WorkflowTaskId) {
        let task_id = WorkflowTaskId::new();
        let task = WorkflowTask::new(task_id, task_type.to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        (definition, task_id)
    }

    fn new_execution(definition: &WorkflowDefinition) -> Execution {
        Execution::from_definition(WorkflowVersionId::new(), definition).unwrap()
    }

    #[tokio::test]
    async fn run_completes_execution_when_task_succeeds() {
        let (definition, task_id) = single_task_definition("send_email");
        let execution = new_execution(&definition);

        let (executor, calls) = fake_executor(Ok(()));
        let store = InMemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(
            result.task(task_id).unwrap().status(),
            TaskStatus::Completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_completes_noop_tasks_without_dispatching_to_executor() {
        let (definition, task_id) = single_task_definition("noop");
        let execution = new_execution(&definition);

        let (executor, calls) = fake_executor(Ok(()));
        let store = InMemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(
            result.task(task_id).unwrap().status(),
            TaskStatus::Completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn run_persists_execution_state_to_the_store() {
        let (definition, _task_id) = single_task_definition("send_email");
        let execution = new_execution(&definition);
        let execution_id = execution.id();

        let (executor, _calls) = fake_executor(Ok(()));
        let store = InMemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        engine.run(execution, &definition).await.unwrap();

        let (stored, _version) = engine.store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.status(), ExecutionStatus::Completed);
    }

    #[tokio::test]
    async fn run_retries_a_failing_task_until_it_succeeds() {
        let (definition, task_id) = single_task_definition("send_email");
        let execution = new_execution(&definition);

        let attempt = Arc::new(AtomicUsize::new(0));
        let attempt_counter = attempt.clone();
        let executor = InProcessExecutor::new(move |_task: &WorkflowTask| {
            let current = attempt_counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if current == 0 {
                    Err(WorkerError::ExecutionFailed {
                        message: "transient".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }) as BoxFuture
        });

        let store = InMemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store).with_retry_policy(RetryPolicy::new(
            3,
            crate::retry::Backoff::Fixed(Duration::ZERO),
        ));

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(
            result.task(task_id).unwrap().status(),
            TaskStatus::Completed
        );
        assert_eq!(attempt.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn run_returns_execution_failed_once_retries_are_exhausted() {
        let (definition, _task_id) = single_task_definition("send_email");
        let execution = new_execution(&definition);

        let (executor, calls) = fake_executor(Err(WorkerError::ExecutionFailed {
            message: "boom".to_owned(),
        }));
        let store = InMemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store).with_retry_policy(RetryPolicy::new(
            2,
            crate::retry::Backoff::Fixed(Duration::ZERO),
        ));

        let err = engine.run(execution, &definition).await.unwrap_err();

        assert!(matches!(
            err,
            EngineError::ExecutionFailed(reason) if reason == "task execution failed: boom"
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn run_completes_a_chain_of_dependent_tasks_in_order() {
        let task_a = WorkflowTaskId::new();
        let task_b = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![
            WorkflowTask::new(task_a, "send_email".to_owned(), vec![]).unwrap(),
            WorkflowTask::new(task_b, "send_email".to_owned(), vec![task_a]).unwrap(),
        ])
        .unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, calls) = fake_executor(Ok(()));
        let store = InMemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(result.task(task_a).unwrap().status(), TaskStatus::Completed);
        assert_eq!(result.task(task_b).unwrap().status(), TaskStatus::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
