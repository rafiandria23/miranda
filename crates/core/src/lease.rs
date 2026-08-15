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
