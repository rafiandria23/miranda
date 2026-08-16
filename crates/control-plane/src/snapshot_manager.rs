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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_storage::filesystem::FilesystemStore;

    use super::*;

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "miranda-snapshot-manager-test-{label}-{}",
            ExecutionId::new()
        ))
    }

    fn manager() -> SnapshotManager<FilesystemStore> {
        SnapshotManager::new(FilesystemStore::new(unique_temp_dir("store")))
    }

    #[test]
    fn is_due_without_prior_snapshot_compares_against_interval() {
        let manager = SnapshotManager::new(FilesystemStore::new(unique_temp_dir("store")))
            .with_interval_events(60);

        assert!(!manager.is_due(59, None));
        assert!(manager.is_due(60, None));
        assert!(manager.is_due(61, None));
    }

    #[test]
    fn is_due_with_prior_snapshot_compares_against_the_delta() {
        let manager = SnapshotManager::new(FilesystemStore::new(unique_temp_dir("store")))
            .with_interval_events(60);

        assert!(!manager.is_due(100, Some(50)));
        assert!(manager.is_due(110, Some(50)));
    }

    #[test]
    fn is_due_never_underflows_when_event_count_is_less_than_last_snapshot() {
        let manager = SnapshotManager::new(FilesystemStore::new(unique_temp_dir("store")))
            .with_interval_events(60);

        assert!(!manager.is_due(10, Some(50)));
    }

    #[test]
    fn with_interval_events_overrides_the_default() {
        let manager = SnapshotManager::new(FilesystemStore::new(unique_temp_dir("store")))
            .with_interval_events(5);

        assert!(manager.is_due(5, None));
    }

    #[tokio::test]
    async fn save_then_load_latest_returns_the_saved_snapshot() {
        let manager = manager();
        let execution_id = ExecutionId::new();

        manager
            .save(execution_id, 1, b"snapshot-data")
            .await
            .unwrap();

        let (version, data) = manager
            .load_latest(execution_id)
            .await
            .unwrap()
            .expect("snapshot is present");

        assert_eq!(version, 1);
        assert_eq!(data, b"snapshot-data");
    }

    #[tokio::test]
    async fn load_latest_returns_none_when_no_snapshot_exists() {
        let manager = manager();

        let result = manager.load_latest(ExecutionId::new()).await.unwrap();

        assert!(result.is_none());
    }
}
