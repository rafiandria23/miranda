use miranda_core::{
    id::{ExecutionId, WorkflowTaskId},
    workflow::WorkflowDefinition,
};
use std::{collections::VecDeque, future::Future, sync::Arc};
use tokio::sync::Mutex;

use crate::ControlPlaneError;

#[derive(Debug)]
pub struct QueueItem {
    pub execution_id: ExecutionId,
    pub workflow_task_id: WorkflowTaskId,
    pub definition: Arc<WorkflowDefinition>,
}

pub trait TaskQueue: Send + Sync {
    fn enqueue(
        &self,
        item: QueueItem,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;

    fn dequeue(&self) -> impl Future<Output = Result<Option<QueueItem>, ControlPlaneError>> + Send;
}

// =========================================================================
// In-Memory Implementation
// =========================================================================

#[derive(Debug, Default, Clone)]
pub struct InMemoryTaskQueue {
    inner: Arc<Mutex<VecDeque<QueueItem>>>,
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskQueue for InMemoryTaskQueue {
    async fn enqueue(&self, item: QueueItem) -> Result<(), ControlPlaneError> {
        let mut queue = self.inner.lock().await;

        queue.push_back(item);

        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<QueueItem>, ControlPlaneError> {
        let mut queue = self.inner.lock().await;

        Ok(queue.pop_front())
    }
}
