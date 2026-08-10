use miranda_core::queue::QueuedTask;
use std::{future::Future, pin::Pin, sync::Arc};

use crate::error::StorageError;

pub trait TaskQueueStore: Send + Sync {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn dequeue<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<QueuedTask>, StorageError>> + Send + 'a>>;
}

impl TaskQueueStore for Arc<dyn TaskQueueStore> {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).enqueue(task)
    }

    fn dequeue<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<QueuedTask>, StorageError>> + Send + 'a>> {
        (**self).dequeue()
    }
}
