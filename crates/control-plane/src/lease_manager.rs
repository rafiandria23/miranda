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
