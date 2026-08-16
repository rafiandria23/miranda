use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::id::{ExecutionId, WorkerId, WorkflowTaskId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub worker_id: WorkerId,
    pub workflow_task_id: WorkflowTaskId,
    pub execution_id: ExecutionId,
    pub token: String,
    pub expires_at: OffsetDateTime,
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        OffsetDateTime::now_utc() > self.expires_at
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use time::Duration;

    use crate::id::{ExecutionId, WorkerId, WorkflowTaskId};

    use super::*;

    fn make_lease(expires_at: OffsetDateTime) -> Lease {
        Lease {
            worker_id: WorkerId::new(),
            workflow_task_id: WorkflowTaskId::new(),
            execution_id: ExecutionId::new(),
            token: "token".to_string(),
            expires_at,
        }
    }

    #[test]
    fn is_expired_returns_true_when_past() {
        let lease = make_lease(OffsetDateTime::now_utc() - Duration::seconds(1));
        assert!(lease.is_expired());
    }

    #[test]
    fn is_expired_returns_false_when_future() {
        let lease = make_lease(OffsetDateTime::now_utc() + Duration::seconds(60));
        assert!(!lease.is_expired());
    }

    #[test]
    fn serde_round_trip() {
        let lease = make_lease(OffsetDateTime::now_utc() + Duration::seconds(60));
        let json = serde_json::to_string(&lease).unwrap();
        let deserialized: Lease = serde_json::from_str(&json).unwrap();
        assert_eq!(lease, deserialized);
    }
}
