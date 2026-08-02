use miranda_core::id::WorkerId;
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerStatus {
    Active,
    Draining,
    Offline,
}

#[derive(Debug, Clone)]
pub struct WorkerNode {
    id: WorkerId,
    capabilities: HashSet<String>,
    status: WorkerStatus,
    last_heartbeat: Instant,
    heartbeat_timeout: Duration,
}

impl WorkerNode {
    pub fn new(id: WorkerId, capabilities: Vec<String>, heartbeat_timeout: Duration) -> Self {
        Self {
            id,
            capabilities: capabilities.into_iter().collect(),
            status: WorkerStatus::Active,
            last_heartbeat: Instant::now(),
            heartbeat_timeout,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn status(&self) -> &WorkerStatus {
        &self.status
    }

    pub fn supports_capability(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }

    pub fn record_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();

        if self.status == WorkerStatus::Offline {
            self.status = WorkerStatus::Active;
        }
    }

    pub fn set_status(&mut self, status: WorkerStatus) {
        self.status = status;
    }

    pub fn is_healthy(&self) -> bool {
        self.status == WorkerStatus::Active
            && Instant::now().duration_since(self.last_heartbeat) < self.heartbeat_timeout
    }
}
