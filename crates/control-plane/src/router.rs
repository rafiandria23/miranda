use miranda_core::id::WorkerId;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

use crate::ControlPlaneError;

pub const DEFAULT_WORKER_STALENESS_THRESHOLD: Duration = Duration::from_secs(120);

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

    fn touch(
        &self,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;

    fn select_worker(&self, task_type: &str) -> impl Future<Output = Option<WorkerId>> + Send;

    fn worker_satisfies(
        &self,
        worker_id: WorkerId,
        task_type: &str,
    ) -> impl Future<Output = bool> + Send;

    fn reap_stale(
        &self,
        staleness_threshold: Duration,
    ) -> impl Future<Output = Vec<WorkerInfo>> + Send;
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

    async fn touch(&self, worker_id: WorkerId) -> Result<(), ControlPlaneError> {
        let mut workers = self.workers.write().await;

        if let Some(info) = workers.get_mut(&worker_id) {
            info.last_heartbeat = Instant::now();
        }

        Ok(())
    }

    async fn select_worker(&self, task_type: &str) -> Option<WorkerId> {
        let workers = self.workers.read().await;

        workers
            .values()
            .find(|w| w.capabilities.contains(task_type))
            .map(|w| w.id)
    }

    async fn worker_satisfies(&self, worker_id: WorkerId, task_type: &str) -> bool {
        let workers = self.workers.read().await;

        workers
            .get(&worker_id)
            .is_some_and(|info| info.capabilities.contains(task_type))
    }

    async fn reap_stale(&self, staleness_threshold: Duration) -> Vec<WorkerInfo> {
        let mut workers = self.workers.write().await;
        let now = Instant::now();

        let stale_ids: Vec<WorkerId> = workers
            .iter()
            .filter(|(_, info)| now.duration_since(info.last_heartbeat) > staleness_threshold)
            .map(|(id, _)| *id)
            .collect();

        stale_ids
            .into_iter()
            .filter_map(|id| workers.remove(&id))
            .collect()
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
    async fn touch_updates_last_heartbeat_and_prevents_reaping() {
        let router = InMemoryRouter::new();
        let worker = WorkerInfo {
            id: WorkerId::new(),
            capabilities: ["default"].iter().map(|c| c.to_string()).collect(),
            last_heartbeat: Instant::now() - Duration::from_secs(60),
        };
        let worker_id = worker.id;

        router.register(worker).await.unwrap();
        router.touch(worker_id).await.unwrap();

        let reaped = router.reap_stale(Duration::from_secs(30)).await;

        assert!(reaped.is_empty());
        assert_eq!(router.select_worker("default").await, Some(worker_id));
    }

    #[tokio::test]
    async fn touch_unknown_worker_is_a_no_op() {
        let router = InMemoryRouter::new();

        assert!(router.touch(WorkerId::new()).await.is_ok());
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

    #[tokio::test]
    async fn reap_stale_removes_workers_past_the_staleness_threshold() {
        let router = InMemoryRouter::new();
        let worker = WorkerInfo {
            id: WorkerId::new(),
            capabilities: ["default"].iter().map(|c| c.to_string()).collect(),
            last_heartbeat: Instant::now() - Duration::from_secs(60),
        };
        let worker_id = worker.id;

        router.register(worker).await.unwrap();

        let reaped = router.reap_stale(Duration::from_secs(30)).await;

        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].id, worker_id);
        assert!(router.select_worker("default").await.is_none());
    }

    #[tokio::test]
    async fn reap_stale_leaves_workers_within_the_staleness_threshold() {
        let router = InMemoryRouter::new();
        let worker = worker_with_capabilities(&["default"]);
        let worker_id = worker.id;

        router.register(worker).await.unwrap();

        let reaped = router.reap_stale(Duration::from_secs(30)).await;

        assert!(reaped.is_empty());
        assert_eq!(router.select_worker("default").await, Some(worker_id));
    }

    #[tokio::test]
    async fn reap_stale_returns_empty_when_no_workers_registered() {
        let router = InMemoryRouter::new();

        let reaped = router.reap_stale(Duration::from_secs(30)).await;

        assert!(reaped.is_empty());
    }
}
