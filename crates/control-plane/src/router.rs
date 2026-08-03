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

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_with_capabilities(capabilities: &[&str]) -> WorkerInfo {
        WorkerInfo {
            id: WorkerId::new(),
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            last_heartbeat: Instant::now(),
        }
    }

    #[tokio::test]
    async fn select_worker_returns_none_when_no_workers_registered() {
        let router = InMemoryRouter::new();

        assert!(router.select_worker("default").await.is_none());
    }

    #[tokio::test]
    async fn select_worker_returns_worker_with_matching_capability() {
        let router = InMemoryRouter::new();
        let worker = worker_with_capabilities(&["default"]);
        let worker_id = worker.id;

        router.register(worker).await.unwrap();

        assert_eq!(router.select_worker("default").await, Some(worker_id));
    }

    #[tokio::test]
    async fn select_worker_returns_none_when_no_worker_has_capability() {
        let router = InMemoryRouter::new();

        router
            .register(worker_with_capabilities(&["other"]))
            .await
            .unwrap();

        assert!(router.select_worker("default").await.is_none());
    }

    #[tokio::test]
    async fn register_overwrites_existing_worker_with_same_id() {
        let router = InMemoryRouter::new();
        let worker_id = WorkerId::new();

        router
            .register(WorkerInfo {
                id: worker_id,
                capabilities: ["default"].iter().map(|c| c.to_string()).collect(),
                last_heartbeat: Instant::now(),
            })
            .await
            .unwrap();

        router
            .register(WorkerInfo {
                id: worker_id,
                capabilities: ["other"].iter().map(|c| c.to_string()).collect(),
                last_heartbeat: Instant::now(),
            })
            .await
            .unwrap();

        assert!(router.select_worker("default").await.is_none());
        assert_eq!(router.select_worker("other").await, Some(worker_id));
    }

    #[tokio::test]
    async fn deregister_removes_worker() {
        let router = InMemoryRouter::new();
        let worker = worker_with_capabilities(&["default"]);
        let worker_id = worker.id;

        router.register(worker).await.unwrap();
        router.deregister(worker_id).await.unwrap();

        assert!(router.select_worker("default").await.is_none());
    }

    #[tokio::test]
    async fn deregister_unknown_worker_is_a_no_op() {
        let router = InMemoryRouter::new();

        assert!(router.deregister(WorkerId::new()).await.is_ok());
    }

    #[tokio::test]
    async fn select_worker_picks_a_worker_when_multiple_qualify() {
        let router = InMemoryRouter::new();
        let worker_a = worker_with_capabilities(&["default"]);
        let worker_b = worker_with_capabilities(&["default"]);
        let ids = [worker_a.id, worker_b.id];

        router.register(worker_a).await.unwrap();
        router.register(worker_b).await.unwrap();

        let selected = router.select_worker("default").await.unwrap();

        assert!(ids.contains(&selected));
    }
}
