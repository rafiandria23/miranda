pub mod routed;

pub use routed::RoutedDispatcher;

use miranda_core::{id::WorkerId, queue::QueuedTask, workflow::WorkflowDefinition};
use std::{future::Future, sync::Arc};

use crate::{error::ControlPlaneError, queue::TaskQueue};

pub trait DispatchStrategy: Send + Sync {
    fn next(
        &self,
        worker_id: WorkerId,
    ) -> impl Future<
        Output = Result<Option<(QueuedTask, Arc<WorkflowDefinition>)>, ControlPlaneError>,
    > + Send;
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
    async fn next(
        &self,
        _worker_id: WorkerId,
    ) -> Result<Option<(QueuedTask, Arc<WorkflowDefinition>)>, ControlPlaneError> {
        self.queue.dequeue().await
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::{ExecutionId, WorkflowTaskId};

    use crate::queue::InMemoryTaskQueue;

    use super::*;

    #[tokio::test]
    async fn next_returns_none_when_queue_is_empty() {
        let dispatcher = Dispatcher::new(InMemoryTaskQueue::new());

        let result = dispatcher.next(WorkerId::new()).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_returns_the_dequeued_task() {
        let queue = InMemoryTaskQueue::new();
        let task = QueuedTask::new(ExecutionId::new(), WorkflowTaskId::new());
        let task_id = task.id();
        let definition = Arc::new(WorkflowDefinition::new(Vec::new()).unwrap());

        queue.enqueue(task, definition).await.unwrap();

        let dispatcher = Dispatcher::new(queue);

        let (dequeued_task, _) = dispatcher.next(WorkerId::new()).await.unwrap().unwrap();

        assert_eq!(dequeued_task.id(), task_id);
    }
}
