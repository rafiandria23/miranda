use std::time::{Duration, Instant};

use miranda_core::id::{WorkerId, WorkflowTaskId};

#[derive(Debug, Clone)]
pub struct TaskLease {
    task_id: WorkflowTaskId,
    worker_id: WorkerId,
    lease_token: String,
    expires_at: Instant,
    ttl: Duration,
}

impl TaskLease {
    pub fn new(
        task_id: WorkflowTaskId,
        worker_id: WorkerId,
        lease_token: String,
        ttl: Duration,
    ) -> Self {
        Self {
            task_id,
            worker_id,
            lease_token,
            expires_at: Instant::now() + ttl,
            ttl,
        }
    }

    pub fn task_id(&self) -> WorkflowTaskId {
        self.task_id
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub fn renew(&mut self) {
        self.expires_at = Instant::now() + self.ttl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_lease_expiration_and_renewal() {
        let task_id = WorkflowTaskId::new();
        let worker_id = WorkerId::new();
        let mut lease = TaskLease::new(
            task_id,
            worker_id,
            "token-123".to_string(),
            Duration::from_millis(50),
        );

        assert!(!lease.is_expired());
        std::thread::sleep(Duration::from_millis(60));
        assert!(lease.is_expired());

        lease.renew();
        assert!(!lease.is_expired());
    }
}
