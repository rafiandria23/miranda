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
