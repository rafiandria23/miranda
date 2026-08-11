use miranda_core::id::{ExecutionId, WorkerId, WorkflowTaskId};
use std::{collections::HashMap, sync::Arc, time::Instant};
use time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::ControlPlaneError;

pub const DEFAULT_LEASE_TTL: Duration = Duration::seconds(60);

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
        let std_ttl: std::time::Duration =
            self.ttl.try_into().expect("lease TTL must be non-negative");

        self.expires_at = Instant::now() + std_ttl;
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

        let std_ttl: std::time::Duration = ttl.try_into().expect("lease TTL must be non-negative");

        let lease = Lease {
            token: token.clone(),
            execution_id,
            workflow_task_id,
            worker_id,
            expires_at: Instant::now() + std_ttl,
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
