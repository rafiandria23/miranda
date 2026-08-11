mod durable;
mod memory;

pub use durable::DurableTaskQueue;
pub use memory::InMemoryTaskQueue;

use miranda_core::{queue::QueuedTask, workflow::WorkflowDefinition};
use std::{future::Future, pin::Pin, sync::Arc};

use crate::ControlPlaneError;

pub trait TaskQueue: Send + Sync {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        definition: Arc<WorkflowDefinition>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>>;

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
    >;
}

impl TaskQueue for Arc<dyn TaskQueue + '_> {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        definition: Arc<WorkflowDefinition>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        (**self).enqueue(task, definition)
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
        (**self).dequeue()
    }
}
