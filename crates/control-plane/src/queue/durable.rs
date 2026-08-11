use miranda_core::{queue::QueuedTask, workflow::WorkflowDefinition};
use miranda_storage::{task_queue_store::TaskQueueStore, workflow_store::WorkflowStore};
use std::{future::Future, pin::Pin, sync::Arc};

use crate::{error::ControlPlaneError, queue::TaskQueue};

pub struct DurableTaskQueue<S> {
    store: Arc<S>,
}

impl<S> DurableTaskQueue<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

impl<S> TaskQueue for DurableTaskQueue<S>
where
    S: TaskQueueStore + WorkflowStore + Send + Sync + 'static,
{
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
        _definition: Arc<WorkflowDefinition>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .enqueue(task)
                .await
                .map_err(ControlPlaneError::from)
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
        Box::pin(async move {
            let Some(task) = self
                .store
                .dequeue()
                .await
                .map_err(ControlPlaneError::from)?
            else {
                return Ok(None);
            };

            let (execution, _version) = self
                .store
                .get_execution(task.execution_id())
                .await
                .map_err(ControlPlaneError::from)?;

            let definition = Arc::new(
                self.store
                    .get_definition(execution.workflow_version_id())
                    .await
                    .map_err(ControlPlaneError::from)?,
            );

            Ok(Some((task, definition)))
        })
    }
}
