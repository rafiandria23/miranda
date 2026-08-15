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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_lease(ttl: Duration) -> TaskLease {
        TaskLease::new(
            WorkflowTaskId::new(),
            WorkerId::new(),
            "token-123".to_owned(),
            ttl,
        )
    }

    #[test]
    fn new_populates_all_fields() {
        let task_id = WorkflowTaskId::new();
        let worker_id = WorkerId::new();
        let lease = TaskLease::new(task_id, worker_id, "token-abc".to_owned(), Duration::from_secs(30));

        assert_eq!(lease.task_id, task_id);
        assert_eq!(lease.worker_id, worker_id);
        assert_eq!(lease.token, "token-abc");
        assert_eq!(lease.ttl, Duration::from_secs(30));
    }

    #[test]
    fn is_expired_is_false_immediately_after_creation() {
        let lease = make_lease(Duration::from_secs(30));

        assert!(!lease.is_expired());
    }

    #[test]
    fn is_expired_becomes_true_after_the_ttl_elapses() {
        let lease = make_lease(Duration::from_millis(20));

        std::thread::sleep(Duration::from_millis(40));

        assert!(lease.is_expired());
    }

    #[test]
    fn time_remaining_is_zero_once_expired() {
        let lease = make_lease(Duration::from_millis(20));

        std::thread::sleep(Duration::from_millis(40));

        assert_eq!(lease.time_remaining(), Duration::ZERO);
    }

    #[test]
    fn time_remaining_is_at_most_the_ttl_before_expiration() {
        let lease = make_lease(Duration::from_secs(30));

        let remaining = lease.time_remaining();

        assert!(remaining > Duration::ZERO);
        assert!(remaining <= Duration::from_secs(30));
    }

    #[test]
    fn renew_extends_expiration_past_the_original_deadline() {
        let mut lease = make_lease(Duration::from_millis(30));
        let original_expires_at = lease.expires_at;

        std::thread::sleep(Duration::from_millis(15));
        lease.renew();

        assert!(lease.expires_at > original_expires_at);
        assert!(!lease.is_expired());
    }

    #[test]
    fn renew_can_recover_an_already_expired_lease() {
        let mut lease = make_lease(Duration::from_millis(10));

        std::thread::sleep(Duration::from_millis(20));
        assert!(lease.is_expired());

        lease.renew();

        assert!(!lease.is_expired());
        assert!(lease.time_remaining() > Duration::ZERO);
    }
}
