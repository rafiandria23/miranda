use miranda_core::{
    event::{Event, EventPayload},
    execution::{Execution, NextAction},
    workflow::WorkflowDefinition,
};
use miranda_storage::WorkflowStore;
use miranda_worker::TaskExecutor;
use tracing::{error, info, instrument};

use crate::{
    EngineError,
    retry::RetryPolicy,
    task_runner::{TaskDispatcher, TaskOutcome},
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
                        let result = dispatcher
                            .dispatch(&mut execution, definition, workflow_task_id)
                            .await?;

                        outcome
                            .apply(&mut execution, definition, workflow_task_id, result)
                            .await?;

                        self.store.update_execution(&execution, version).await?;
                        version += 1;
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
