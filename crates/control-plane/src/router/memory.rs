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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::WorkerId;
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
        let router = InMemoryRouter::new();
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
        let router = InMemoryRouter::new();

        assert!(!router.worker_satisfies(WorkerId::new(), "send_email").await);
    }

    #[tokio::test]
    async fn select_worker_returns_none_when_no_worker_has_capability() {
        let router = InMemoryRouter::new();

        assert!(router.select_worker("send_email").await.is_none());
    }

    #[tokio::test]
    async fn select_worker_returns_registered_worker_with_capability() {
        let router = InMemoryRouter::new();
        let worker_id = WorkerId::new();

        router
            .register(registration(worker_id, &["send_email"]))
            .await
            .expect("register succeeds");

        assert_eq!(router.select_worker("send_email").await, Some(worker_id));
    }

    #[tokio::test]
    async fn register_overwrites_existing_worker_with_same_id() {
        let router = InMemoryRouter::new();
        let worker_id = WorkerId::new();

        router
            .register(registration(worker_id, &["send_email"]))
            .await
            .expect("register succeeds");

        router
            .register(registration(worker_id, &["send_sms"]))
            .await
            .expect("register succeeds");

        assert!(!router.worker_satisfies(worker_id, "send_email").await);
        assert!(router.worker_satisfies(worker_id, "send_sms").await);
    }

    #[tokio::test]
    async fn deregister_removes_worker() {
        let router = InMemoryRouter::new();
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
    async fn deregister_unknown_worker_is_a_no_op() {
        let router = InMemoryRouter::new();

        router
            .deregister(WorkerId::new())
            .await
            .expect("deregister succeeds");
    }

    #[tokio::test]
    async fn touch_updates_heartbeat_so_worker_is_not_reaped() {
        let router = InMemoryRouter::new();
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
    async fn touch_unknown_worker_is_a_no_op() {
        let router = InMemoryRouter::new();

        router.touch(WorkerId::new()).await.expect("touch succeeds");
    }

    #[tokio::test]
    async fn reap_stale_removes_workers_past_threshold() {
        let router = InMemoryRouter::new();
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
        let router = InMemoryRouter::new();

        let reaped = router.reap_stale(Duration::minutes(5)).await;

        assert!(reaped.is_empty());
    }
}
