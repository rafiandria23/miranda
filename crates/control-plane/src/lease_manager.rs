use miranda_core::{
    id::{ExecutionId, WorkerId, WorkflowTaskId},
    lease::Lease,
};
use miranda_storage::lease_store::LeaseStore;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::error::ControlPlaneError;

pub const DEFAULT_LEASE_TTL: Duration = Duration::seconds(60);

pub struct LeaseManager<S> {
    store: S,
}

impl<S: LeaseStore> LeaseManager<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn create(
        &self,
        execution_id: ExecutionId,
        workflow_task_id: WorkflowTaskId,
        worker_id: WorkerId,
        ttl: Duration,
    ) -> Result<String, ControlPlaneError> {
        let token = Uuid::new_v4().to_string();
        let expires_at = OffsetDateTime::now_utc() + ttl;

        self.store
            .create(Lease {
                token: token.clone(),
                execution_id,
                workflow_task_id,
                worker_id,
                expires_at,
            })
            .await
            .map_err(ControlPlaneError::from)?;

        Ok(token)
    }

    pub async fn validate(
        &self,
        token: &str,
        worker_id: WorkerId,
    ) -> Result<Lease, ControlPlaneError> {
        let lease = self
            .store
            .get(token)
            .await
            .map_err(ControlPlaneError::from)?
            .ok_or_else(|| ControlPlaneError::InvalidRequest("invalid lease token".to_string()))?;

        if lease.is_expired() {
            return Err(ControlPlaneError::InvalidRequest(
                "lease expired".to_string(),
            ));
        }

        if lease.worker_id != worker_id {
            return Err(ControlPlaneError::InvalidRequest(
                "lease worker mismatch".to_string(),
            ));
        }

        Ok(lease)
    }

    pub async fn release(&self, token: &str) -> Result<(), ControlPlaneError> {
        self.store
            .release(token)
            .await
            .map_err(ControlPlaneError::from)
    }

    pub async fn renew(&self, token: &str, ttl: Duration) -> Result<(), ControlPlaneError> {
        let new_expires_at = OffsetDateTime::now_utc() + ttl;

        self.store
            .renew(token, new_expires_at)
            .await
            .map_err(ControlPlaneError::from)
    }

    pub async fn get_active_leases(&self, worker_id: WorkerId) -> Vec<String> {
        self.store
            .active_for_worker(worker_id)
            .await
            .unwrap_or_default()
    }

    pub async fn reap_expired(&self) -> Vec<Lease> {
        self.store.reap_expired().await.unwrap_or_default()
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_storage::InMemoryStore;

    use super::*;

    fn manager() -> LeaseManager<InMemoryStore> {
        LeaseManager::new(InMemoryStore::new())
    }

    #[tokio::test]
    async fn create_returns_a_token_that_validates_for_the_same_worker() {
        let manager = manager();
        let worker_id = WorkerId::new();

        let token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                DEFAULT_LEASE_TTL,
            )
            .await
            .expect("create succeeds");

        let lease = manager
            .validate(&token, worker_id)
            .await
            .expect("validate succeeds");

        assert_eq!(lease.token, token);
        assert_eq!(lease.worker_id, worker_id);
    }

    #[tokio::test]
    async fn validate_fails_for_an_unknown_token() {
        let manager = manager();

        let result = manager.validate("unknown-token", WorkerId::new()).await;

        assert!(matches!(
            result,
            Err(ControlPlaneError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn validate_fails_when_the_lease_has_expired() {
        let manager = manager();
        let worker_id = WorkerId::new();

        let token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                Duration::seconds(-1),
            )
            .await
            .expect("create succeeds");

        let result = manager.validate(&token, worker_id).await;

        assert!(matches!(
            result,
            Err(ControlPlaneError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn validate_fails_when_the_worker_does_not_match() {
        let manager = manager();

        let token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                WorkerId::new(),
                DEFAULT_LEASE_TTL,
            )
            .await
            .expect("create succeeds");

        let result = manager.validate(&token, WorkerId::new()).await;

        assert!(matches!(
            result,
            Err(ControlPlaneError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn release_removes_the_lease() {
        let manager = manager();
        let worker_id = WorkerId::new();

        let token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                DEFAULT_LEASE_TTL,
            )
            .await
            .expect("create succeeds");

        manager.release(&token).await.expect("release succeeds");

        let result = manager.validate(&token, worker_id).await;

        assert!(matches!(
            result,
            Err(ControlPlaneError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn renew_extends_an_expired_lease_so_it_validates_again() {
        let manager = manager();
        let worker_id = WorkerId::new();

        let token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                Duration::seconds(-1),
            )
            .await
            .expect("create succeeds");

        manager
            .renew(&token, DEFAULT_LEASE_TTL)
            .await
            .expect("renew succeeds");

        let lease = manager
            .validate(&token, worker_id)
            .await
            .expect("validate succeeds");

        assert_eq!(lease.token, token);
    }

    #[tokio::test]
    async fn get_active_leases_returns_only_unexpired_leases_for_the_worker() {
        let manager = manager();
        let worker_id = WorkerId::new();
        let other_worker_id = WorkerId::new();

        let active_token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                DEFAULT_LEASE_TTL,
            )
            .await
            .expect("create succeeds");

        manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                Duration::seconds(-1),
            )
            .await
            .expect("create succeeds");

        manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                other_worker_id,
                DEFAULT_LEASE_TTL,
            )
            .await
            .expect("create succeeds");

        let active = manager.get_active_leases(worker_id).await;

        assert_eq!(active, vec![active_token]);
    }

    #[tokio::test]
    async fn reap_expired_removes_and_returns_only_expired_leases() {
        let manager = manager();
        let worker_id = WorkerId::new();

        let expired_token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                Duration::seconds(-1),
            )
            .await
            .expect("create succeeds");

        let active_token = manager
            .create(
                ExecutionId::new(),
                WorkflowTaskId::new(),
                worker_id,
                DEFAULT_LEASE_TTL,
            )
            .await
            .expect("create succeeds");

        let reaped = manager.reap_expired().await;

        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].token, expired_token);

        let lease = manager
            .validate(&active_token, worker_id)
            .await
            .expect("validate succeeds");

        assert_eq!(lease.token, active_token);
    }
}
