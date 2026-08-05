pub mod routed;

pub use routed::RoutedDispatcher;

use miranda_core::id::WorkerId;
use std::future::Future;

use crate::{
    ControlPlaneError,
    queue::{QueueItem, TaskQueue},
};

pub trait DispatchStrategy: Send + Sync {
    fn next(
        &self,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<Option<QueueItem>, ControlPlaneError>> + Send;
}

pub struct Dispatcher<Q> {
    queue: Q,
}

impl<Q> Dispatcher<Q>
where
    Q: TaskQueue,
{
    pub fn new(queue: Q) -> Self {
        Self { queue }
    }
}

impl<Q> DispatchStrategy for Dispatcher<Q>
where
    Q: TaskQueue,
{
    async fn next(&self, _worker_id: WorkerId) -> Result<Option<QueueItem>, ControlPlaneError> {
        self.queue.dequeue().await
    }
}

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::{ExecutionId, WorkflowTaskId},
        workflow::{WorkflowDefinition, WorkflowTask},
    };
    use std::sync::Arc;

    use crate::queue::InMemoryTaskQueue;

    use super::*;

    fn queue_item() -> QueueItem {
        let workflow_task_id = WorkflowTaskId::new();
        let task = WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        QueueItem {
            execution_id: ExecutionId::new(),
            workflow_task_id,
            definition: Arc::new(definition),
        }
    }

    #[tokio::test]
    async fn next_returns_none_when_queue_is_empty() {
        let dispatcher = Dispatcher::new(InMemoryTaskQueue::new());

        assert!(dispatcher.next(WorkerId::new()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_returns_dequeued_item_ignoring_worker_id() {
        let queue = InMemoryTaskQueue::new();
        let item = queue_item();
        let execution_id = item.execution_id;

        queue.enqueue(item).await.unwrap();

        let dispatcher = Dispatcher::new(queue);
        let dispatched = dispatcher.next(WorkerId::new()).await.unwrap().unwrap();

        assert_eq!(dispatched.execution_id, execution_id);
    }

    #[tokio::test]
    async fn next_follows_fifo_order() {
        let queue = InMemoryTaskQueue::new();
        let first = queue_item();
        let second = queue_item();
        let first_execution_id = first.execution_id;
        let second_execution_id = second.execution_id;

        queue.enqueue(first).await.unwrap();
        queue.enqueue(second).await.unwrap();

        let dispatcher = Dispatcher::new(queue);

        assert_eq!(
            dispatcher
                .next(WorkerId::new())
                .await
                .unwrap()
                .unwrap()
                .execution_id,
            first_execution_id
        );
        assert_eq!(
            dispatcher
                .next(WorkerId::new())
                .await
                .unwrap()
                .unwrap()
                .execution_id,
            second_execution_id
        );
    }

    #[tokio::test]
    async fn next_drains_queue_and_stays_empty() {
        let queue = InMemoryTaskQueue::new();
        queue.enqueue(queue_item()).await.unwrap();

        let dispatcher = Dispatcher::new(queue);

        assert!(dispatcher.next(WorkerId::new()).await.unwrap().is_some());
        assert!(dispatcher.next(WorkerId::new()).await.unwrap().is_none());
    }
}
