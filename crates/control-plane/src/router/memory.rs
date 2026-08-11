use miranda_core::{id::WorkerId, router::WorkerRegistration};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use time::Duration;
use tokio::sync::RwLock;

use crate::{error::ControlPlaneError, router::Router};

#[derive(Debug, Default, Clone)]
pub struct InMemoryRouter {
    workers: Arc<RwLock<HashMap<WorkerId, WorkerRegistration>>>,
}

impl InMemoryRouter {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Router for InMemoryRouter {
    fn register<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.workers
                .write()
                .await
                .insert(registration.id(), registration);

            Ok(())
        })
    }

    fn deregister<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            self.workers.write().await.remove(&worker_id);

            Ok(())
        })
    }

    fn touch<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(reg) = self.workers.write().await.get_mut(&worker_id) {
                *reg = reg.clone().with_heartbeat(time::OffsetDateTime::now_utc());
            }

            Ok(())
        })
    }

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<WorkerId>> + Send + 'a>> {
        Box::pin(async move {
            self.workers
                .read()
                .await
                .values()
                .find(|reg| reg.has_capability(capability))
                .map(|reg| reg.id())
        })
    }

    fn worker_satisfies<'a>(
        &'a self,
        worker_id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            self.workers
                .read()
                .await
                .get(&worker_id)
                .is_some_and(|reg| reg.has_capability(capability))
        })
    }

    fn reap_stale<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Vec<WorkerId>> + Send + 'a>> {
        Box::pin(async move {
            let mut workers = self.workers.write().await;

            let stale: Vec<WorkerId> = workers
                .values()
                .filter(|reg| reg.is_stale(threshold))
                .map(|reg| reg.id())
                .collect();

            for id in &stale {
                workers.remove(id);
            }

            stale
        })
    }
}
