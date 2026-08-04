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
