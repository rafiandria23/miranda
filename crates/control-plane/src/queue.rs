use miranda_core::{
    id::{ExecutionId, WorkflowTaskId},
    workflow::WorkflowDefinition,
};
use std::{collections::VecDeque, future::Future, sync::Arc};
use tokio::sync::Mutex;

use crate::ControlPlaneError;

#[derive(Debug)]
pub struct QueueItem {
    pub execution_id: ExecutionId,
    pub workflow_task_id: WorkflowTaskId,
    pub definition: Arc<WorkflowDefinition>,
}

pub trait TaskQueue: Send + Sync {
    fn enqueue(
        &self,
        item: QueueItem,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;

    fn dequeue(&self) -> impl Future<Output = Result<Option<QueueItem>, ControlPlaneError>> + Send;
}

// =========================================================================
// In-Memory Implementation
// =========================================================================

#[derive(Debug, Default, Clone)]
pub struct InMemoryTaskQueue {
    inner: Arc<Mutex<VecDeque<QueueItem>>>,
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskQueue for InMemoryTaskQueue {
    async fn enqueue(&self, item: QueueItem) -> Result<(), ControlPlaneError> {
        let mut queue = self.inner.lock().await;

        queue.push_back(item);

        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<QueueItem>, ControlPlaneError> {
        let mut queue = self.inner.lock().await;

        Ok(queue.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miranda_core::workflow::WorkflowTask;

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
    async fn dequeue_returns_none_when_empty() {
        let queue = InMemoryTaskQueue::new();

        assert!(queue.dequeue().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dequeue_returns_enqueued_item() {
        let queue = InMemoryTaskQueue::new();
        let item = queue_item();
        let execution_id = item.execution_id;
        let workflow_task_id = item.workflow_task_id;

        queue.enqueue(item).await.unwrap();

        let dequeued = queue.dequeue().await.unwrap().unwrap();

        assert_eq!(dequeued.execution_id, execution_id);
        assert_eq!(dequeued.workflow_task_id, workflow_task_id);
    }

    #[tokio::test]
    async fn dequeue_follows_fifo_order() {
        let queue = InMemoryTaskQueue::new();
        let first = queue_item();
        let second = queue_item();
        let third = queue_item();
        let first_execution_id = first.execution_id;
        let second_execution_id = second.execution_id;
        let third_execution_id = third.execution_id;

        queue.enqueue(first).await.unwrap();
        queue.enqueue(second).await.unwrap();
        queue.enqueue(third).await.unwrap();

        assert_eq!(
            queue.dequeue().await.unwrap().unwrap().execution_id,
            first_execution_id
        );
        assert_eq!(
            queue.dequeue().await.unwrap().unwrap().execution_id,
            second_execution_id
        );
        assert_eq!(
            queue.dequeue().await.unwrap().unwrap().execution_id,
            third_execution_id
        );
    }

    #[tokio::test]
    async fn dequeue_drains_queue_and_stays_empty() {
        let queue = InMemoryTaskQueue::new();
        queue.enqueue(queue_item()).await.unwrap();

        assert!(queue.dequeue().await.unwrap().is_some());
        assert!(queue.dequeue().await.unwrap().is_none());
        assert!(queue.dequeue().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn cloned_queue_shares_underlying_state() {
        let queue = InMemoryTaskQueue::new();
        let cloned = queue.clone();
        let item = queue_item();
        let execution_id = item.execution_id;

        queue.enqueue(item).await.unwrap();

        let dequeued = cloned.dequeue().await.unwrap().unwrap();

        assert_eq!(dequeued.execution_id, execution_id);
    }
}
