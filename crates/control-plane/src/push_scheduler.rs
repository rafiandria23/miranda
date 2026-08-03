use miranda_core::id::WorkerId;

use crate::{
    ControlPlaneError,
    queue::{QueueItem, TaskQueue},
    router::Router,
};

#[derive(Debug)]
pub struct PushScheduler<Q, R> {
    queue: Q,
    router: R,
}

impl<Q, R> PushScheduler<Q, R>
where
    Q: TaskQueue,
    R: Router,
{
    pub fn new(queue: Q, router: R) -> Self {
        Self { queue, router }
    }

    pub async fn dispatch(&self) -> Result<Option<(WorkerId, QueueItem)>, ControlPlaneError> {
        let item = match self.queue.dequeue().await? {
            Some(a) => a,
            None => return Ok(None),
        };

        // Find capable worker for this task type
        let task_type = "default"; // TODO: get from item.definition
        let worker_id = match self.router.select_worker(task_type).await {
            Some(w_id) => w_id,
            None => {
                // No worker available, re-queue
                self.queue.enqueue(item).await?;

                return Ok(None);
            }
        };

        Ok(Some((worker_id, item)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue::InMemoryTaskQueue,
        router::{InMemoryRouter, WorkerInfo},
    };
    use miranda_core::{
        id::{ExecutionId, WorkflowTaskId},
        workflow::{WorkflowDefinition, WorkflowTask},
    };
    use std::{sync::Arc, time::Instant};

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

    fn worker_with_capabilities(capabilities: &[&str]) -> WorkerInfo {
        WorkerInfo {
            id: WorkerId::new(),
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            last_heartbeat: Instant::now(),
        }
    }

    #[tokio::test]
    async fn dispatch_returns_none_when_queue_is_empty() {
        let scheduler = PushScheduler::new(InMemoryTaskQueue::new(), InMemoryRouter::new());

        let result = scheduler.dispatch().await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn dispatch_returns_worker_and_item_when_capable_worker_exists() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        let item = queue_item();
        let execution_id = item.execution_id;
        queue.enqueue(item).await.unwrap();

        let worker = worker_with_capabilities(&["default"]);
        let worker_id = worker.id;
        router.register(worker).await.unwrap();

        let scheduler = PushScheduler::new(queue, router);
        let (dispatched_worker_id, dispatched_item) = scheduler.dispatch().await.unwrap().unwrap();

        assert_eq!(dispatched_worker_id, worker_id);
        assert_eq!(dispatched_item.execution_id, execution_id);
    }

    #[tokio::test]
    async fn dispatch_requeues_item_when_no_capable_worker() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        let item = queue_item();
        let execution_id = item.execution_id;
        queue.enqueue(item).await.unwrap();

        let scheduler = PushScheduler::new(queue, router);

        let result = scheduler.dispatch().await.unwrap();
        assert!(result.is_none());

        // The item should have been re-queued, not dropped.
        let requeued = scheduler.queue.dequeue().await.unwrap().unwrap();
        assert_eq!(requeued.execution_id, execution_id);
    }

    #[tokio::test]
    async fn dispatch_ignores_workers_lacking_required_capability() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        queue.enqueue(queue_item()).await.unwrap();
        router
            .register(worker_with_capabilities(&["other"]))
            .await
            .unwrap();

        let scheduler = PushScheduler::new(queue, router);

        let result = scheduler.dispatch().await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn dispatch_removes_item_from_queue_on_success() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        queue.enqueue(queue_item()).await.unwrap();
        router
            .register(worker_with_capabilities(&["default"]))
            .await
            .unwrap();

        let scheduler = PushScheduler::new(queue, router);

        assert!(scheduler.dispatch().await.unwrap().is_some());
        assert!(scheduler.dispatch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dispatch_deregistered_worker_is_no_longer_selected() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();

        queue.enqueue(queue_item()).await.unwrap();
        let worker = worker_with_capabilities(&["default"]);
        let worker_id = worker.id;
        router.register(worker).await.unwrap();
        router.deregister(worker_id).await.unwrap();

        let scheduler = PushScheduler::new(queue, router);

        let result = scheduler.dispatch().await.unwrap();

        assert!(result.is_none());
    }
}
