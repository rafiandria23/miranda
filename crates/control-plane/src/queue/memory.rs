use miranda_core::{queue::QueuedTask, workflow::WorkflowDefinition};
use std::{collections::VecDeque, future::Future, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

use crate::{error::ControlPlaneError, queue::TaskQueue};

#[derive(Debug, Default, Clone)]
pub struct InMemoryTaskQueue {
    inner: Arc<Mutex<VecDeque<(QueuedTask, Arc<WorkflowDefinition>)>>>,
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskQueue for InMemoryTaskQueue {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        definition: Arc<WorkflowDefinition>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.inner.lock().await.push_back((task, definition));

            Ok(())
        })
    }

    fn dequeue<'a>(
        &'a self,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<(QueuedTask, Arc<WorkflowDefinition>)>,
                        ControlPlaneError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move { Ok(self.inner.lock().await.pop_front()) })
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::{ExecutionId, WorkflowTaskId};

    use super::*;

    fn queued_task() -> QueuedTask {
        QueuedTask::new(ExecutionId::new(), WorkflowTaskId::new())
    }

    fn definition() -> Arc<WorkflowDefinition> {
        Arc::new(WorkflowDefinition::new(Vec::new()).expect("valid definition"))
    }

    #[tokio::test]
    async fn dequeue_returns_none_when_queue_is_empty() {
        let queue = InMemoryTaskQueue::new();

        let result = queue.dequeue().await.expect("dequeue succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn enqueue_then_dequeue_returns_the_same_task_and_definition() {
        let queue = InMemoryTaskQueue::new();

        let task = queued_task();
        let task_id = task.id();
        let definition = definition();

        queue
            .enqueue(task, definition.clone())
            .await
            .expect("enqueue succeeds");

        let (dequeued_task, dequeued_definition) = queue
            .dequeue()
            .await
            .expect("dequeue succeeds")
            .expect("task is present");

        assert_eq!(dequeued_task.id(), task_id);
        assert_eq!(*dequeued_definition, *definition);
    }

    #[tokio::test]
    async fn dequeue_removes_task_from_queue() {
        let queue = InMemoryTaskQueue::new();

        queue
            .enqueue(queued_task(), definition())
            .await
            .expect("enqueue succeeds");

        queue.dequeue().await.expect("dequeue succeeds");

        let result = queue.dequeue().await.expect("second dequeue succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn dequeue_returns_tasks_in_fifo_order() {
        let queue = InMemoryTaskQueue::new();

        let first = queued_task();
        let first_id = first.id();
        let second = queued_task();
        let second_id = second.id();

        queue
            .enqueue(first, definition())
            .await
            .expect("enqueue succeeds");
        queue
            .enqueue(second, definition())
            .await
            .expect("enqueue succeeds");

        let (dequeued_first, _) = queue
            .dequeue()
            .await
            .expect("dequeue succeeds")
            .expect("first task is present");
        let (dequeued_second, _) = queue
            .dequeue()
            .await
            .expect("dequeue succeeds")
            .expect("second task is present");

        assert_eq!(dequeued_first.id(), first_id);
        assert_eq!(dequeued_second.id(), second_id);
    }
}
