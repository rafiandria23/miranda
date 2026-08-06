use miranda_core::{
    event::{Event, EventPayload},
    execution::{Execution, NextAction},
    workflow::WorkflowDefinition,
};
use miranda_storage::WorkflowStore;
use miranda_worker::TaskExecutor;
use tracing::{debug, error, info, instrument};

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
    #[instrument(skip(self, definition), fields(execution_id = %execution.id()))]
    pub async fn run(
        &self,
        mut execution: Execution,
        definition: &WorkflowDefinition,
    ) -> Result<Execution, EngineError> {
        info!("starting execution");

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
                    error!("deadlock: no ready tasks but execution not finished");

                    return Err(EngineError::Deadlock);
                }

                NextAction::RunTasks(ready) => {
                    for workflow_task_id in ready {
                        let workflow_task = definition
                            .task(workflow_task_id)
                            .expect("ready task must exist in definition");

                        if workflow_task.task_type() == "noop" {
                            debug!(task_id = %workflow_task_id, "noop task, completing without dispatch");

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

        info!("execution completed");

        Ok(execution)
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc, sync::Mutex, time::Duration};

    use miranda_core::{
        execution::ExecutionStatus,
        id::{WorkflowTaskId, WorkflowVersionId},
        workflow::WorkflowTask,
    };
    use miranda_storage::MemoryStore;
    use miranda_worker::{InProcessExecutor, WorkerError};

    use crate::retry::Backoff;

    use super::*;

    struct CallLog(Mutex<Vec<WorkflowTaskId>>);

    impl CallLog {
        fn call_order(&self) -> Vec<WorkflowTaskId> {
            self.0.lock().unwrap().clone()
        }
    }

    type BoxFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkerError>> + Send>>;

    fn scripted_executor(
        results: Vec<Result<(), WorkerError>>,
    ) -> (
        InProcessExecutor<impl Fn(&WorkflowTask) -> BoxFuture + Send + Sync>,
        Arc<CallLog>,
    ) {
        let calls = Arc::new(CallLog(Mutex::new(Vec::new())));
        let log = calls.clone();
        let results = Arc::new(Mutex::new(VecDeque::from(results)));

        let executor = InProcessExecutor::new(move |task: &WorkflowTask| {
            log.0.lock().unwrap().push(task.id());

            let result = results
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted executor ran out of results");

            Box::pin(async move { result }) as BoxFuture
        });

        (executor, calls)
    }

    fn task(id: WorkflowTaskId, dependencies: Vec<WorkflowTaskId>) -> WorkflowTask {
        WorkflowTask::new(id, "send_email".to_owned(), dependencies).unwrap()
    }

    fn noop_task(id: WorkflowTaskId, dependencies: Vec<WorkflowTaskId>) -> WorkflowTask {
        WorkflowTask::new(id, "noop".to_owned(), dependencies).unwrap()
    }

    fn new_execution() -> Execution {
        Execution::new(WorkflowVersionId::new())
    }

    #[tokio::test]
    async fn run_completes_a_single_task_workflow() {
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![task(task_id, vec![])]).unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, _calls) = scripted_executor(vec![Ok(())]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(
            result.task(task_id).unwrap().status(),
            miranda_core::execution::TaskStatus::Completed
        );
    }

    #[tokio::test]
    async fn run_executes_dependent_tasks_after_their_dependencies() {
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![
            task(dependency_id, vec![]),
            task(task_id, vec![dependency_id]),
        ])
        .unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, calls) = scripted_executor(vec![Ok(()), Ok(())]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(calls.call_order(), vec![dependency_id, task_id]);
    }

    #[tokio::test]
    async fn run_retries_a_failing_task_until_it_succeeds() {
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![task(task_id, vec![])]).unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, calls) = scripted_executor(vec![
            Err(WorkerError::ExecutionFailed {
                message: "transient".to_owned(),
            }),
            Ok(()),
        ]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store)
            .with_retry_policy(RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO)));

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(calls.call_order().len(), 2);
    }

    #[tokio::test]
    async fn run_fails_the_execution_once_retries_are_exhausted() {
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![task(task_id, vec![])]).unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, _calls) = scripted_executor(vec![Err(WorkerError::ExecutionFailed {
            message: "fatal".to_owned(),
        })]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store)
            .with_retry_policy(RetryPolicy::new(0, Backoff::Fixed(Duration::ZERO)));

        let execution_id = execution.id();
        let err = engine.run(execution, &definition).await.unwrap_err();

        assert!(
            matches!(err, EngineError::ExecutionFailed(reason) if reason == "task execution failed: fatal")
        );

        let (stored_execution, _version) = engine.store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored_execution.status(), ExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn run_persists_the_completed_execution_in_the_store() {
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![task(task_id, vec![])]).unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        let execution_id = execution.id();

        let (executor, _calls) = scripted_executor(vec![Ok(())]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        engine.run(execution, &definition).await.unwrap();

        let (stored_execution, _version) = engine.store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored_execution.status(), ExecutionStatus::Completed);
    }

    #[tokio::test]
    async fn run_completes_immediately_for_a_definition_with_no_tasks() {
        let definition = WorkflowDefinition::new(vec![]).unwrap();
        let execution = new_execution();

        let (executor, calls) = scripted_executor(vec![]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert!(calls.call_order().is_empty());
    }

    #[tokio::test]
    async fn run_completes_a_noop_task_without_dispatching_it() {
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![noop_task(task_id, vec![])]).unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, calls) = scripted_executor(vec![]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(
            result.task(task_id).unwrap().status(),
            miranda_core::execution::TaskStatus::Completed
        );
        assert!(calls.call_order().is_empty());
    }

    #[tokio::test]
    async fn run_executes_dispatched_tasks_after_a_noop_dependency() {
        let noop_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![
            noop_task(noop_id, vec![]),
            task(task_id, vec![noop_id]),
        ])
        .unwrap();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let (executor, calls) = scripted_executor(vec![Ok(())]);
        let store = MemoryStore::new();
        let engine = EmbeddedEngine::new(executor, store);

        let result = engine.run(execution, &definition).await.unwrap();

        assert_eq!(result.status(), ExecutionStatus::Completed);
        assert_eq!(
            result.task(noop_id).unwrap().status(),
            miranda_core::execution::TaskStatus::Completed
        );
        assert_eq!(calls.call_order(), vec![task_id]);
    }
}
