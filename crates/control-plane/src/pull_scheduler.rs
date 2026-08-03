use miranda_core::id::WorkerId;

use crate::{
    ControlPlaneError,
    queue::{QueueItem, TaskQueue},
};

#[derive(Debug)]
pub struct PullScheduler<Q> {
    queue: Q,
}

impl<Q> PullScheduler<Q>
where
    Q: TaskQueue,
{
    pub fn new(queue: Q) -> Self {
        Self { queue }
    }

    pub async fn poll(&self, _worker_id: WorkerId) -> Result<Option<QueueItem>, ControlPlaneError> {
        // Worker polls for next available task
        // No worker-specific routing in basic pull model
        self.queue.dequeue().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::InMemoryTaskQueue;
    use miranda_core::{
        id::{ExecutionId, WorkflowTaskId},
        workflow::{WorkflowDefinition, WorkflowTask},
    };
    use std::sync::Arc;

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
    async fn poll_returns_none_when_queue_is_empty() {
        let scheduler = PullScheduler::new(InMemoryTaskQueue::new());

        let item = scheduler.poll(WorkerId::new()).await.unwrap();

        assert!(item.is_none());
    }

    #[tokio::test]
    async fn poll_returns_enqueued_item() {
        let queue = InMemoryTaskQueue::new();
        let item = queue_item();
        let execution_id = item.execution_id;
        let workflow_task_id = item.workflow_task_id;

        queue.enqueue(item).await.unwrap();

        let scheduler = PullScheduler::new(queue);
        let polled = scheduler.poll(WorkerId::new()).await.unwrap().unwrap();

        assert_eq!(polled.execution_id, execution_id);
        assert_eq!(polled.workflow_task_id, workflow_task_id);
    }

    #[tokio::test]
    async fn poll_dequeues_in_fifo_order() {
        let queue = InMemoryTaskQueue::new();
        let first = queue_item();
        let second = queue_item();
        let first_execution_id = first.execution_id;
        let second_execution_id = second.execution_id;

        queue.enqueue(first).await.unwrap();
        queue.enqueue(second).await.unwrap();

        let scheduler = PullScheduler::new(queue);

        let polled_first = scheduler.poll(WorkerId::new()).await.unwrap().unwrap();
        let polled_second = scheduler.poll(WorkerId::new()).await.unwrap().unwrap();

        assert_eq!(polled_first.execution_id, first_execution_id);
        assert_eq!(polled_second.execution_id, second_execution_id);
    }

    #[tokio::test]
    async fn poll_removes_item_from_queue() {
        let queue = InMemoryTaskQueue::new();
        queue.enqueue(queue_item()).await.unwrap();

        let scheduler = PullScheduler::new(queue);

        assert!(scheduler.poll(WorkerId::new()).await.unwrap().is_some());
        assert!(scheduler.poll(WorkerId::new()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn poll_is_indifferent_to_worker_id() {
        let queue = InMemoryTaskQueue::new();
        queue.enqueue(queue_item()).await.unwrap();

        let scheduler = PullScheduler::new(queue);

        let polled = scheduler.poll(WorkerId::new()).await.unwrap();

        assert!(polled.is_some());
    }
}
