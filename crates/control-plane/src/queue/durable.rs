use miranda_core::{queue::QueuedTask, workflow::WorkflowDefinition};
use miranda_storage::{task_queue_store::TaskQueueStore, workflow_store::WorkflowStore};
use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    error::ControlPlaneError,
    queue::{QueueItem, TaskQueue},
};

pub struct DurableTaskQueue<S> {
    store: Arc<S>,
}

impl<S> DurableTaskQueue<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

impl<S> TaskQueue for DurableTaskQueue<S>
where
    S: TaskQueueStore + WorkflowStore + Send + Sync + 'static,
{
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        _definition: Arc<WorkflowDefinition>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .enqueue(task)
                .await
                .map_err(ControlPlaneError::from)
        })
    }

    fn dequeue<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<QueueItem>, ControlPlaneError>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(task) = self
                .store
                .dequeue()
                .await
                .map_err(ControlPlaneError::from)?
            else {
                return Ok(None);
            };

            let (execution, _version) = self
                .store
                .get_execution(task.execution_id())
                .await
                .map_err(ControlPlaneError::from)?;

            let definition = Arc::new(
                self.store
                    .get_definition(execution.workflow_version_id())
                    .await
                    .map_err(ControlPlaneError::from)?,
            );

            Ok(Some((task, definition)))
        })
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        execution::Execution,
        id::{WorkflowTaskId, WorkflowVersionId},
        workflow::WorkflowTask,
    };
    use miranda_storage::InMemoryStore;
    use std::sync::Arc;

    use super::*;

    fn definition_with_task(task_type: &str) -> (WorkflowDefinition, WorkflowTaskId) {
        let task = WorkflowTask::new(WorkflowTaskId::new(), task_type.to_string(), Vec::new())
            .expect("valid task");
        let task_id = task.id();
        let definition = WorkflowDefinition::new(vec![task]).expect("valid definition");

        (definition, task_id)
    }

    async fn seed_execution(
        store: &InMemoryStore,
        task_type: &str,
    ) -> (QueuedTask, Arc<WorkflowDefinition>) {
        let (definition, task_id) = definition_with_task(task_type);
        let version_id = WorkflowVersionId::new();

        store
            .save_definition(
                miranda_core::id::WorkflowId::new(),
                "wf",
                version_id,
                1,
                &definition,
            )
            .await
            .expect("save definition succeeds");

        let execution = Execution::new(version_id);

        store
            .save_execution(&execution)
            .await
            .expect("save execution succeeds");

        let queued_task = QueuedTask::new(execution.id(), task_id);

        (queued_task, Arc::new(definition))
    }

    #[tokio::test]
    async fn dequeue_returns_none_when_queue_is_empty() {
        let store = Arc::new(InMemoryStore::new());
        let queue = DurableTaskQueue::new(store);

        let result = queue.dequeue().await.expect("dequeue succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn enqueue_then_dequeue_returns_task_with_resolved_definition() {
        let store = Arc::new(InMemoryStore::new());
        let queue = DurableTaskQueue::new(store.clone());

        let (queued_task, definition) = seed_execution(&store, "send_email").await;
        let task_id = queued_task.workflow_task_id();
        let execution_id = queued_task.execution_id();

        queue
            .enqueue(queued_task, definition.clone())
            .await
            .expect("enqueue succeeds");

        let (dequeued_task, dequeued_definition) = queue
            .dequeue()
            .await
            .expect("dequeue succeeds")
            .expect("task is present");

        assert_eq!(dequeued_task.workflow_task_id(), task_id);
        assert_eq!(dequeued_task.execution_id(), execution_id);
        assert_eq!(*dequeued_definition, *definition);
    }

    #[tokio::test]
    async fn dequeue_removes_task_from_queue() {
        let store = Arc::new(InMemoryStore::new());
        let queue = DurableTaskQueue::new(store.clone());

        let (queued_task, definition) = seed_execution(&store, "send_email").await;

        queue
            .enqueue(queued_task, definition)
            .await
            .expect("enqueue succeeds");

        queue.dequeue().await.expect("dequeue succeeds");

        let result = queue.dequeue().await.expect("second dequeue succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn dequeue_errors_when_execution_is_missing() {
        let store = Arc::new(InMemoryStore::new());
        let queue = DurableTaskQueue::new(store.clone());

        let queued_task =
            QueuedTask::new(miranda_core::id::ExecutionId::new(), WorkflowTaskId::new());

        store
            .enqueue(queued_task)
            .await
            .expect("raw enqueue succeeds");

        let result = queue.dequeue().await;

        assert!(matches!(
            result,
            Err(ControlPlaneError::Storage(
                miranda_storage::StorageError::ExecutionNotFound(_)
            ))
        ));
    }

    #[tokio::test]
    async fn dequeue_errors_when_definition_is_missing() {
        let store = Arc::new(InMemoryStore::new());
        let queue = DurableTaskQueue::new(store.clone());

        let execution = Execution::new(WorkflowVersionId::new());

        store
            .save_execution(&execution)
            .await
            .expect("save execution succeeds");

        let queued_task = QueuedTask::new(execution.id(), WorkflowTaskId::new());

        store
            .enqueue(queued_task)
            .await
            .expect("raw enqueue succeeds");

        let result = queue.dequeue().await;

        assert!(matches!(
            result,
            Err(ControlPlaneError::Storage(
                miranda_storage::StorageError::WorkflowVersionNotFound(_)
            ))
        ));
    }
}
