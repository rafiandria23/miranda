use miranda_core::id::ExecutionId;
use miranda_storage::snapshot_store::SnapshotStore;

use crate::error::ControlPlaneError;

pub const DEFAULT_SNAPSHOT_INTERVAL_EVENTS: u64 = 60;

pub struct SnapshotManager<S> {
    store: S,
    interval_events: u64,
}

impl<S: SnapshotStore> SnapshotManager<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            interval_events: DEFAULT_SNAPSHOT_INTERVAL_EVENTS,
        }
    }

    pub fn with_interval_events(mut self, interval_events: u64) -> Self {
        self.interval_events = interval_events;
        self
    }

    pub fn is_due(&self, event_count: u64, last_snapshot_version: Option<u64>) -> bool {
        match last_snapshot_version {
            None => event_count >= self.interval_events,
            Some(last) => event_count.saturating_sub(last) >= self.interval_events,
        }
    }

    pub async fn save(
        &self,
        execution_id: ExecutionId,
        version: u64,
        data: &[u8],
    ) -> Result<(), ControlPlaneError> {
        self.store
            .save_snapshot(execution_id, version, data)
            .await
            .map_err(ControlPlaneError::from)
    }

    pub async fn load_latest(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<(u64, Vec<u8>)>, ControlPlaneError> {
        self.store
            .load_latest_snapshot(execution_id)
            .await
            .map_err(ControlPlaneError::from)
    }
}
