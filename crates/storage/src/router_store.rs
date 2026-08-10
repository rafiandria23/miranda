use miranda_core::{id::WorkerId, router::WorkerRegistration};
use std::{future::Future, pin::Pin, sync::Arc};
use time::Duration;

use crate::error::StorageError;

pub trait RouterStore: Send + Sync {
    fn register_worker<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn deregister_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn touch_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<WorkerId>, StorageError>> + Send + 'a>>;

    fn worker_has_capability<'a>(
        &'a self,
        id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>>;

    fn reap_stale_workers<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkerId>, StorageError>> + Send + 'a>>;
}

impl RouterStore for Arc<dyn RouterStore> {
    fn register_worker<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).register_worker(registration)
    }

    fn deregister_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).deregister_worker(id)
    }

    fn touch_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).touch_worker(id)
    }

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<WorkerId>, StorageError>> + Send + 'a>> {
        (**self).select_worker(capability)
    }

    fn worker_has_capability<'a>(
        &'a self,
        id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        (**self).worker_has_capability(id, capability)
    }

    fn reap_stale_workers<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkerId>, StorageError>> + Send + 'a>> {
        (**self).reap_stale_workers(threshold)
    }
}
