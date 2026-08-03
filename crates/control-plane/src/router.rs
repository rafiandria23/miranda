use miranda_core::id::WorkerId;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Instant,
};
use tokio::sync::RwLock;

use crate::ControlPlaneError;

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub capabilities: HashSet<String>,
    pub last_heartbeat: Instant,
}

pub trait Router: Send + Sync {
    fn register(
        &self,
        worker: WorkerInfo,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;

    fn deregister(
        &self,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;

    fn select_worker(&self, task_type: &str) -> impl Future<Output = Option<WorkerId>> + Send;
}

// =========================================================================
// In-Memory Implementation
// =========================================================================

#[derive(Debug, Default, Clone)]
pub struct InMemoryRouter {
    workers: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>,
}

impl InMemoryRouter {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Router for InMemoryRouter {
    async fn register(&self, worker: WorkerInfo) -> Result<(), ControlPlaneError> {
        let mut workers = self.workers.write().await;

        workers.insert(worker.id, worker);

        Ok(())
    }

    async fn deregister(&self, worker_id: WorkerId) -> Result<(), ControlPlaneError> {
        let mut workers = self.workers.write().await;

        workers.remove(&worker_id);

        Ok(())
    }

    async fn select_worker(&self, task_type: &str) -> Option<WorkerId> {
        let workers = self.workers.read().await;

        workers
            .values()
            .find(|w| w.capabilities.contains(task_type))
            .map(|w| w.id)
    }
}
