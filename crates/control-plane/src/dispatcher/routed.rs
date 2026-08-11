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
