use miranda_core::{id::WorkerId, router::WorkerRegistration};
use std::future::Future;
use time::Duration;

use crate::error::StorageError;

pub trait RouterStore: Send + Sync {
    fn register_worker(
        &self,
        registration: WorkerRegistration,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn deregister_worker(
        &self,
        id: WorkerId,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn touch_worker(&self, id: WorkerId) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn select_worker(
        &self,
        capability: &str,
    ) -> impl Future<Output = Result<Option<WorkerId>, StorageError>> + Send;

    fn worker_has_capability(
        &self,
        id: WorkerId,
        capability: &str,
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;

    fn reap_stale_workers(
        &self,
        threshold: Duration,
    ) -> impl Future<Output = Result<Vec<WorkerId>, StorageError>> + Send;
}
