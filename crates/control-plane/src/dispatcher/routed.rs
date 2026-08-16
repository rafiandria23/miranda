use miranda_core::id::WorkerId;

use crate::{
    dispatcher::DispatchStrategy, error::ControlPlaneError, queue::TaskQueue, router::Router,
};

pub struct RoutedDispatcher<Q, R> {
    queue: Q,
    router: R,
}

impl<Q, R> RoutedDispatcher<Q, R>
where
    Q: TaskQueue,
    R: Router,
{
    pub fn new(queue: Q, router: R) -> Self {
        Self { queue, router }
    }
}

impl<Q, R> DispatchStrategy for RoutedDispatcher<Q, R>
where
    Q: TaskQueue,
    R: Router,
{
    async fn next(
        &self,
        worker_id: WorkerId,
    ) -> Result<
        Option<(
            miranda_core::queue::QueuedTask,
            std::sync::Arc<miranda_core::workflow::WorkflowDefinition>,
        )>,
        ControlPlaneError,
    > {
        let (task, definition) = match self.queue.dequeue().await? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let task_type = definition
            .task(task.workflow_task_id())
            .map(|t| t.task_type())
            .ok_or_else(|| {
                ControlPlaneError::InvalidRequest(
                    "task not found in its own definition".to_string(),
                )
            })?;

        if self.router.worker_satisfies(worker_id, task_type).await {
            Ok(Some((task, definition)))
        } else {
            self.queue.enqueue(task, definition).await?;

            Ok(None)
        }
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::{ExecutionId, WorkerId, WorkflowTaskId},
        queue::QueuedTask,
        router::WorkerRegistration,
        workflow::{WorkflowDefinition, WorkflowTask},
    };
    use std::sync::Arc;
    use time::OffsetDateTime;

    use crate::{queue::InMemoryTaskQueue, router::InMemoryRouter};

    use super::*;

    fn definition_with_task(task_type: &str) -> (WorkflowDefinition, WorkflowTaskId) {
        let task = WorkflowTask::new(WorkflowTaskId::new(), task_type.to_string(), Vec::new())
            .expect("valid task");
        let task_id = task.id();
        let definition = WorkflowDefinition::new(vec![task]).expect("valid definition");

        (definition, task_id)
    }

    async fn register_worker(router: &InMemoryRouter, capabilities: Vec<String>) -> WorkerId {
        let worker_id = WorkerId::new();
        router
            .register(WorkerRegistration::new(
                worker_id,
                capabilities,
                OffsetDateTime::now_utc(),
            ))
            .await
            .expect("register succeeds");

        worker_id
    }

    #[tokio::test]
    async fn next_returns_none_when_queue_is_empty() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let dispatcher = RoutedDispatcher::new(queue, router);

        let result = dispatcher
            .next(WorkerId::new())
            .await
            .expect("dispatch succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_returns_task_when_worker_satisfies_task_type() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        let (definition, task_id) = definition_with_task("send_email");
        let definition = Arc::new(definition);
        let queued_task = QueuedTask::new(ExecutionId::new(), task_id);

        queue
            .enqueue(queued_task, definition.clone())
            .await
            .expect("enqueue succeeds");

        let worker_id = register_worker(&router, vec!["send_email".to_string()]).await;

        let dispatcher = RoutedDispatcher::new(queue, router);

        let (task, returned_definition) = dispatcher
            .next(worker_id)
            .await
            .expect("dispatch succeeds")
            .expect("task is dispatched");

        assert_eq!(task.workflow_task_id(), task_id);
        assert_eq!(*returned_definition, *definition);
    }

    #[tokio::test]
    async fn next_requeues_task_and_returns_none_when_worker_does_not_satisfy_task_type() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        let (definition, task_id) = definition_with_task("send_email");
        let definition = Arc::new(definition);
        let queued_task = QueuedTask::new(ExecutionId::new(), task_id);

        queue
            .enqueue(queued_task, definition.clone())
            .await
            .expect("enqueue succeeds");

        let worker_id = register_worker(&router, vec!["send_sms".to_string()]).await;

        let dispatcher = RoutedDispatcher::new(queue, router);

        let result = dispatcher.next(worker_id).await.expect("dispatch succeeds");

        assert!(result.is_none());

        let (requeued_task, requeued_definition) = dispatcher
            .queue
            .dequeue()
            .await
            .expect("dequeue succeeds")
            .expect("task was requeued");

        assert_eq!(requeued_task.workflow_task_id(), task_id);
        assert_eq!(*requeued_definition, *definition);
    }

    #[tokio::test]
    async fn next_errors_when_task_is_missing_from_its_own_definition() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        let (definition, _) = definition_with_task("send_email");
        let definition = Arc::new(definition);
        let queued_task = QueuedTask::new(ExecutionId::new(), WorkflowTaskId::new());

        queue
            .enqueue(queued_task, definition)
            .await
            .expect("enqueue succeeds");

        let dispatcher = RoutedDispatcher::new(queue, router);

        let result = dispatcher.next(WorkerId::new()).await;

        assert!(matches!(result, Err(ControlPlaneError::InvalidRequest(_))));
    }
}
