use miranda_core::id::WorkerId;

use crate::{
    ControlPlaneError,
    queue::{TaskAssignment, TaskQueue},
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

    pub async fn poll(
        &self,
        _worker_id: WorkerId,
    ) -> Result<Option<TaskAssignment>, ControlPlaneError> {
        // Worker polls for next available task
        // No worker-specific routing in basic pull model
        self.queue.dequeue().await
    }
}
