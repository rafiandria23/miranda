mod durable;
mod memory;

pub use durable::DurableTaskQueue;
pub use memory::InMemoryTaskQueue;

use miranda_core::{queue::QueuedTask, workflow::WorkflowDefinition};
use std::{future::Future, pin::Pin, sync::Arc};

use crate::ControlPlaneError;

pub type QueueItem = (QueuedTask, Arc<WorkflowDefinition>);
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ControlPlaneError>> + Send + 'a>>;

pub trait TaskQueue: Send + Sync {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        definition: Arc<WorkflowDefinition>,
    ) -> BoxFuture<'a, ()>;

    fn dequeue<'a>(&'a self) -> BoxFuture<'a, Option<QueueItem>>;
}

impl TaskQueue for Arc<dyn TaskQueue + '_> {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        definition: Arc<WorkflowDefinition>,
    ) -> BoxFuture<'a, ()> {
        (**self).enqueue(task, definition)
    }

    fn dequeue<'a>(&'a self) -> BoxFuture<'a, Option<QueueItem>> {
        (**self).dequeue()
    }
}
