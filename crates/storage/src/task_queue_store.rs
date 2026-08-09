use miranda_core::queue::QueuedTask;
use std::future::Future;

use crate::error::StorageError;

pub trait TaskQueueStore: Send + Sync {
    fn enqueue(&self, task: QueuedTask) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn dequeue(&self) -> impl Future<Output = Result<Option<QueuedTask>, StorageError>> + Send;
}
