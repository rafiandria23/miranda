use miranda_core::id::{WorkerId, WorkflowTaskId};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TaskLease {
    pub task_id: WorkflowTaskId,
    pub worker_id: WorkerId,
    pub token: String,
    pub expires_at: Instant,
    pub ttl: Duration,
}

impl TaskLease {
    pub fn new(task_id: WorkflowTaskId, worker_id: WorkerId, token: String, ttl: Duration) -> Self {
        Self {
            task_id,
            worker_id,
            token,
            expires_at: Instant::now() + ttl,
            ttl,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    pub fn time_remaining(&self) -> Duration {
        let now = Instant::now();

        if now >= self.expires_at {
            Duration::ZERO
        } else {
            self.expires_at - now
        }
    }

    pub fn renew(&mut self) {
        self.expires_at = Instant::now() + self.ttl;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_creation_and_expiration() {
        let task_id = WorkflowTaskId::new();
        let worker_id = WorkerId::new();
        let ttl = Duration::from_millis(50);

        let lease = TaskLease::new(task_id, worker_id, "token-123".to_string(), ttl);

        assert!(!lease.is_expired());
        assert_eq!(lease.token, "token-123");

        std::thread::sleep(Duration::from_millis(60));
        assert!(lease.is_expired());
        assert_eq!(lease.time_remaining(), Duration::ZERO);
    }

    #[test]
    fn lease_renewal() {
        let task_id = WorkflowTaskId::new();
        let worker_id = WorkerId::new();
        let mut lease = TaskLease::new(
            task_id,
            worker_id,
            "token".to_string(),
            Duration::from_millis(50),
        );

        std::thread::sleep(Duration::from_millis(30));
        assert!(!lease.is_expired());

        lease.renew();
        assert!(!lease.is_expired());
    }
}
