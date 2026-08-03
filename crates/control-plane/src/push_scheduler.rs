use miranda_core::id::WorkerId;

use crate::{
    ControlPlaneError,
    queue::{TaskAssignment, TaskQueue},
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

    pub async fn dispatch(&self) -> Result<Option<(WorkerId, TaskAssignment)>, ControlPlaneError> {
        let assignment = match self.queue.dequeue().await? {
            Some(a) => a,
            None => return Ok(None),
        };

        // Find capable worker for this task type
        let task_type = "default"; // TODO: get from assignment.definition
        let worker_id = match self.router.select_worker(task_type).await {
            Some(w_id) => w_id,
            None => {
                // No worker available, re-queue
                self.queue.enqueue(assignment).await?;

                return Ok(None);
            }
        };

        Ok(Some((worker_id, assignment)))
    }
}
