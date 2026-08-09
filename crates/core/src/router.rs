use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

use crate::id::WorkerId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRegistration {
    id: WorkerId,
    capabilities: Vec<String>,
    last_heartbeat: OffsetDateTime,
}

impl WorkerRegistration {
    pub fn new(id: WorkerId, capabilities: Vec<String>, last_heartbeat: OffsetDateTime) -> Self {
        Self {
            id,
            capabilities,
            last_heartbeat,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    pub fn last_heartbeat(&self) -> OffsetDateTime {
        self.last_heartbeat
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }

    pub fn with_heartbeat(mut self, last_heartbeat: OffsetDateTime) -> Self {
        self.last_heartbeat = last_heartbeat;
        self
    }

    pub fn is_stale(&self, threshold: Duration) -> bool {
        OffsetDateTime::now_utc() - self.last_heartbeat > threshold
    }
}
