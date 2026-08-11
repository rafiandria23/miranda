use miranda_core::{id::WorkerId, router::WorkerRegistration};
use miranda_storage::router_store::RouterStore;
use std::{future::Future, pin::Pin, sync::Arc};
use time::Duration;

use crate::{error::ControlPlaneError, router::Router};

pub struct DurableRouter<S> {
    store: Arc<S>,
}

impl<S> DurableRouter<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

impl<S> Router for DurableRouter<S>
where
    S: RouterStore + Send + Sync + 'static,
{
    fn register<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .register_worker(registration)
                .await
                .map_err(ControlPlaneError::from)
        })
    }

    fn deregister<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .deregister_worker(worker_id)
                .await
                .map_err(ControlPlaneError::from)
        })
    }

    fn touch<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .touch_worker(worker_id)
                .await
                .map_err(ControlPlaneError::from)
        })
    }

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<WorkerId>> + Send + 'a>> {
        Box::pin(async move { self.store.select_worker(capability).await.ok().flatten() })
    }

    fn worker_satisfies<'a>(
        &'a self,
        worker_id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .worker_has_capability(worker_id, capability)
                .await
                .unwrap_or(false)
        })
    }

    fn reap_stale<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Vec<WorkerId>> + Send + 'a>> {
        Box::pin(async move {
            self.store
                .reap_stale_workers(threshold)
                .await
                .unwrap_or_default()
        })
    }
}
