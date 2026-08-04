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
