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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_storage::InMemoryStore;
    use time::OffsetDateTime;

    use super::*;

    fn registration(id: WorkerId, capabilities: &[&str]) -> WorkerRegistration {
        WorkerRegistration::new(
            id,
            capabilities.iter().map(|c| c.to_string()).collect(),
            OffsetDateTime::now_utc(),
        )
    }

    #[tokio::test]
    async fn register_then_worker_satisfies_reports_capability() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);
        let worker_id = WorkerId::new();

        router
            .register(registration(worker_id, &["send_email"]))
            .await
            .expect("register succeeds");

        assert!(router.worker_satisfies(worker_id, "send_email").await);
        assert!(!router.worker_satisfies(worker_id, "other").await);
    }

    #[tokio::test]
    async fn worker_satisfies_is_false_for_unknown_worker() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);

        assert!(!router.worker_satisfies(WorkerId::new(), "send_email").await);
    }

    #[tokio::test]
    async fn select_worker_returns_none_when_no_worker_has_capability() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);

        assert!(router.select_worker("send_email").await.is_none());
    }

    #[tokio::test]
    async fn select_worker_returns_registered_worker_with_capability() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);
        let worker_id = WorkerId::new();

        router
            .register(registration(worker_id, &["send_email"]))
            .await
            .expect("register succeeds");

        assert_eq!(router.select_worker("send_email").await, Some(worker_id));
    }

    #[tokio::test]
    async fn deregister_removes_worker() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);
        let worker_id = WorkerId::new();

        router
            .register(registration(worker_id, &["send_email"]))
            .await
            .expect("register succeeds");

        router
            .deregister(worker_id)
            .await
            .expect("deregister succeeds");

        assert!(!router.worker_satisfies(worker_id, "send_email").await);
        assert!(router.select_worker("send_email").await.is_none());
    }

    #[tokio::test]
    async fn touch_updates_heartbeat_so_worker_is_not_reaped() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);
        let worker_id = WorkerId::new();

        let stale_registration = WorkerRegistration::new(
            worker_id,
            vec!["send_email".to_string()],
            OffsetDateTime::now_utc() - Duration::minutes(10),
        );

        router
            .register(stale_registration)
            .await
            .expect("register succeeds");

        router.touch(worker_id).await.expect("touch succeeds");

        let stale = router.reap_stale(Duration::minutes(5)).await;

        assert!(stale.is_empty());
        assert!(router.worker_satisfies(worker_id, "send_email").await);
    }

    #[tokio::test]
    async fn reap_stale_removes_workers_past_threshold() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);
        let worker_id = WorkerId::new();

        let stale_registration = WorkerRegistration::new(
            worker_id,
            vec!["send_email".to_string()],
            OffsetDateTime::now_utc() - Duration::minutes(10),
        );

        router
            .register(stale_registration)
            .await
            .expect("register succeeds");

        let reaped = router.reap_stale(Duration::minutes(5)).await;

        assert_eq!(reaped, vec![worker_id]);
        assert!(!router.worker_satisfies(worker_id, "send_email").await);
    }

    #[tokio::test]
    async fn reap_stale_returns_empty_when_no_workers_registered() {
        let store = Arc::new(InMemoryStore::new());
        let router = DurableRouter::new(store);

        let reaped = router.reap_stale(Duration::minutes(5)).await;

        assert!(reaped.is_empty());
    }
}
