use miranda_core::id::{ExecutionId, WorkerId, WorkflowTaskId};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::ControlPlaneError;

pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseToken(pub String);

#[derive(Debug, Clone)]
pub struct Lease {
    pub token: LeaseToken,
    pub execution_id: ExecutionId,
    pub workflow_task_id: WorkflowTaskId,
    pub worker_id: WorkerId,
    pub expires_at: Instant,
    pub ttl: Duration,
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    pub fn renew(&mut self) {
        self.expires_at = Instant::now() + self.ttl;
    }
}

#[derive(Debug, Default)]
pub struct LeaseManager {
    leases: Arc<RwLock<HashMap<LeaseToken, Lease>>>,
}

impl LeaseManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(
        &self,
        execution_id: ExecutionId,
        workflow_task_id: WorkflowTaskId,
        worker_id: WorkerId,
        ttl: Duration,
    ) -> LeaseToken {
        let token = LeaseToken(Uuid::new_v4().to_string());

        let lease = Lease {
            token: token.clone(),
            execution_id,
            workflow_task_id,
            worker_id,
            expires_at: Instant::now() + ttl,
            ttl,
        };

        let mut leases = self.leases.write().await;
        leases.insert(token.clone(), lease);

        token
    }

    pub async fn validate(
        &self,
        token: &LeaseToken,
        worker_id: WorkerId,
    ) -> Result<Lease, ControlPlaneError> {
        let leases = self.leases.read().await;
        let lease = leases
            .get(token)
            .ok_or(ControlPlaneError::InvalidRequest(
                "invalid lease token".to_string(),
            ))?
            .clone();

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

    pub async fn release(&self, token: &LeaseToken) -> Result<(), ControlPlaneError> {
        let mut leases = self.leases.write().await;

        leases
            .remove(token)
            .ok_or(ControlPlaneError::InvalidRequest(
                "lease not found".to_string(),
            ))?;

        Ok(())
    }

    pub async fn renew(&self, token: &LeaseToken) -> Result<(), ControlPlaneError> {
        let mut leases = self.leases.write().await;
        let lease = leases
            .get_mut(token)
            .ok_or(ControlPlaneError::InvalidRequest(
                "lease not found".to_string(),
            ))?;

        lease.renew();

        Ok(())
    }

    pub async fn get_active_leases(&self, worker_id: WorkerId) -> Vec<LeaseToken> {
        let leases = self.leases.read().await;

        leases
            .values()
            .filter(|l| l.worker_id == worker_id && !l.is_expired())
            .map(|l| l.token.clone())
            .collect()
    }

    pub async fn reap_expired(&self) -> Vec<Lease> {
        let mut leases = self.leases.write().await;
        let expired: Vec<LeaseToken> = leases
            .iter()
            .filter(|(_, l)| l.is_expired())
            .map(|(t, _)| t.clone())
            .collect();

        expired
            .into_iter()
            .filter_map(|t| leases.remove(&t))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (ExecutionId, WorkflowTaskId, WorkerId) {
        (ExecutionId::new(), WorkflowTaskId::new(), WorkerId::new())
    }

    #[tokio::test]
    async fn create_returns_unique_tokens() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token_a = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;
        let token_b = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;

        assert_ne!(token_a, token_b);
    }

    #[tokio::test]
    async fn validate_succeeds_for_active_lease_with_matching_worker() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;

        let lease = manager.validate(&token, worker_id).await.unwrap();

        assert_eq!(lease.token, token);
        assert_eq!(lease.execution_id, execution_id);
        assert_eq!(lease.workflow_task_id, task_id);
        assert_eq!(lease.worker_id, worker_id);
    }

    #[tokio::test]
    async fn validate_fails_for_unknown_token() {
        let manager = LeaseManager::new();
        let (_, _, worker_id) = ids();

        let err = manager
            .validate(&LeaseToken("nonexistent".to_string()), worker_id)
            .await
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn validate_fails_for_expired_lease() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token = manager
            .create(execution_id, task_id, worker_id, Duration::ZERO)
            .await;

        tokio::time::sleep(Duration::from_millis(5)).await;

        let err = manager.validate(&token, worker_id).await.unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn validate_fails_for_worker_mismatch() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();
        let other_worker_id = WorkerId::new();

        let token = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;

        let err = manager.validate(&token, other_worker_id).await.unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn release_removes_lease() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;

        manager.release(&token).await.unwrap();

        let err = manager.validate(&token, worker_id).await.unwrap_err();
        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn release_fails_for_unknown_token() {
        let manager = LeaseManager::new();

        let err = manager
            .release(&LeaseToken("nonexistent".to_string()))
            .await
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn release_fails_when_called_twice() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;

        manager.release(&token).await.unwrap();
        let err = manager.release(&token).await.unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn renew_extends_expiration() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token = manager
            .create(execution_id, task_id, worker_id, Duration::from_millis(20))
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        manager.renew(&token).await.unwrap();

        let lease = manager.validate(&token, worker_id).await.unwrap();
        assert!(!lease.is_expired());
    }

    #[tokio::test]
    async fn renew_fails_for_unknown_token() {
        let manager = LeaseManager::new();

        let err = manager
            .renew(&LeaseToken("nonexistent".to_string()))
            .await
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn renew_can_resurrect_an_expired_lease() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let token = manager
            .create(execution_id, task_id, worker_id, Duration::from_millis(5))
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        manager.renew(&token).await.unwrap();

        let lease = manager.validate(&token, worker_id).await.unwrap();
        assert!(!lease.is_expired());
    }

    #[tokio::test]
    async fn get_active_leases_returns_only_matching_non_expired_leases() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();
        let other_worker_id = WorkerId::new();

        let active_token = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;
        let expired_token = manager
            .create(execution_id, task_id, worker_id, Duration::ZERO)
            .await;
        let _other_worker_token = manager
            .create(execution_id, task_id, other_worker_id, DEFAULT_LEASE_TTL)
            .await;

        tokio::time::sleep(Duration::from_millis(5)).await;

        let active = manager.get_active_leases(worker_id).await;

        assert_eq!(active, vec![active_token]);
        assert!(!active.contains(&expired_token));
    }

    #[tokio::test]
    async fn get_active_leases_returns_empty_for_unknown_worker() {
        let manager = LeaseManager::new();

        let active = manager.get_active_leases(WorkerId::new()).await;

        assert!(active.is_empty());
    }

    #[tokio::test]
    async fn reap_expired_removes_only_expired_leases() {
        let manager = LeaseManager::new();
        let (execution_id, task_id, worker_id) = ids();

        let active_token = manager
            .create(execution_id, task_id, worker_id, DEFAULT_LEASE_TTL)
            .await;
        let expired_token = manager
            .create(execution_id, task_id, worker_id, Duration::ZERO)
            .await;

        tokio::time::sleep(Duration::from_millis(5)).await;
        let reaped = manager.reap_expired().await;

        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].token, expired_token);

        assert!(manager.validate(&active_token, worker_id).await.is_ok());
        assert!(matches!(
            manager.release(&expired_token).await.unwrap_err(),
            ControlPlaneError::InvalidRequest(_)
        ));
    }

    #[test]
    fn lease_renew_updates_expires_at() {
        let mut lease = Lease {
            token: LeaseToken("token".to_string()),
            execution_id: ExecutionId::new(),
            workflow_task_id: WorkflowTaskId::new(),
            worker_id: WorkerId::new(),
            expires_at: Instant::now() - Duration::from_secs(1),
            ttl: Duration::from_secs(60),
        };

        assert!(lease.is_expired());

        lease.renew();

        assert!(!lease.is_expired());
    }
}
