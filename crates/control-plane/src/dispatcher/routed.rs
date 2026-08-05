use miranda_core::id::WorkerId;

use crate::{
    ControlPlaneError,
    dispatcher::DispatchStrategy,
    queue::{QueueItem, TaskQueue},
    router::Router,
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
    async fn next(&self, worker_id: WorkerId) -> Result<Option<QueueItem>, ControlPlaneError> {
        let item = match self.queue.dequeue().await? {
            Some(item) => item,
            None => return Ok(None),
        };

        let task_type = item
            .definition
            .task(item.workflow_task_id)
            .map(|t| t.task_type())
            .ok_or_else(|| {
                ControlPlaneError::InvalidRequest(
                    "task not found in its own definition".to_string(),
                )
            })?;

        if self.router.worker_satisfies(worker_id, task_type).await {
            Ok(Some(item))
        } else {
            self.queue.enqueue(item).await?;

            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::{ExecutionId, WorkflowTaskId},
        workflow::{WorkflowDefinition, WorkflowTask},
    };
    use std::{sync::Arc, time::Instant};

    use crate::{
        queue::InMemoryTaskQueue,
        router::{InMemoryRouter, WorkerInfo},
    };

    use super::*;

    fn queue_item_with_type(task_type: &str) -> QueueItem {
        let workflow_task_id = WorkflowTaskId::new();
        let task = WorkflowTask::new(workflow_task_id, task_type.to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        QueueItem {
            execution_id: ExecutionId::new(),
            workflow_task_id,
            definition: Arc::new(definition),
        }
    }

    fn worker_with_capabilities(capabilities: &[&str]) -> WorkerInfo {
        WorkerInfo {
            id: WorkerId::new(),
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            last_heartbeat: Instant::now(),
        }
    }

    #[tokio::test]
    async fn next_returns_none_when_queue_is_empty() {
        let dispatcher = RoutedDispatcher::new(InMemoryTaskQueue::new(), InMemoryRouter::new());

        assert!(dispatcher.next(WorkerId::new()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_returns_item_when_worker_satisfies_task_type() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let item = queue_item_with_type("send_email");
        let execution_id = item.execution_id;

        queue.enqueue(item).await.unwrap();

        let worker = worker_with_capabilities(&["send_email"]);
        let worker_id = worker.id;
        router.register(worker).await.unwrap();

        let dispatcher = RoutedDispatcher::new(queue, router);
        let dispatched = dispatcher.next(worker_id).await.unwrap().unwrap();

        assert_eq!(dispatched.execution_id, execution_id);
    }

    #[tokio::test]
    async fn next_requeues_and_returns_none_when_worker_lacks_capability() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let item = queue_item_with_type("send_email");
        let execution_id = item.execution_id;

        queue.enqueue(item).await.unwrap();

        let worker = worker_with_capabilities(&["other"]);
        let worker_id = worker.id;
        router.register(worker).await.unwrap();

        let dispatcher = RoutedDispatcher::new(queue.clone(), router);

        assert!(dispatcher.next(worker_id).await.unwrap().is_none());

        let requeued = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(requeued.execution_id, execution_id);
    }

    #[tokio::test]
    async fn next_returns_none_when_unregistered_worker_dequeues_item() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        queue
            .enqueue(queue_item_with_type("send_email"))
            .await
            .unwrap();

        let dispatcher = RoutedDispatcher::new(queue.clone(), router);

        assert!(dispatcher.next(WorkerId::new()).await.unwrap().is_none());
        assert!(queue.dequeue().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn next_errors_when_task_not_found_in_its_own_definition() {
        let other_task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![other_task]).unwrap();
        let item = QueueItem {
            execution_id: ExecutionId::new(),
            workflow_task_id: WorkflowTaskId::new(),
            definition: Arc::new(definition),
        };

        let queue = InMemoryTaskQueue::new();
        queue.enqueue(item).await.unwrap();

        let dispatcher = RoutedDispatcher::new(queue, InMemoryRouter::new());
        let err = dispatcher.next(WorkerId::new()).await.unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn next_only_matches_worker_with_exact_task_type_capability() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let item = queue_item_with_type("send_email");
        let execution_id = item.execution_id;

        queue.enqueue(item).await.unwrap();

        let worker = worker_with_capabilities(&["send_email", "send_sms"]);
        let worker_id = worker.id;
        router.register(worker).await.unwrap();

        let dispatcher = RoutedDispatcher::new(queue, router);
        let dispatched = dispatcher.next(worker_id).await.unwrap().unwrap();

        assert_eq!(dispatched.execution_id, execution_id);
    }
}
