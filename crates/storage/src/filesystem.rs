use miranda_core::id::ExecutionId;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;

use crate::{SnapshotStore, StorageError};

#[derive(Debug, Clone)]
pub struct FilesystemStore {
    base_dir: PathBuf,
}

impl FilesystemStore {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    fn execution_dir(&self, execution_id: ExecutionId) -> PathBuf {
        self.base_dir.join(execution_id.to_string())
    }

    fn snapshot_path(&self, execution_id: ExecutionId, version: u64) -> PathBuf {
        self.execution_dir(execution_id)
            .join(format!("{version}.snapshot"))
    }
}

impl SnapshotStore for FilesystemStore {
    async fn save(
        &self,
        execution_id: ExecutionId,
        version: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let dir = self.execution_dir(execution_id);

        fs::create_dir_all(&dir)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let path = self.snapshot_path(execution_id, version);
        let tmp_path = path.with_extension("snapshot.tmp");

        fs::write(&tmp_path, data)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        Ok(())
    }

    async fn load(&self, execution_id: ExecutionId, version: u64) -> Result<Vec<u8>, StorageError> {
        let path = self.snapshot_path(execution_id, version);

        fs::read(&path).await.map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                StorageError::SnapshotNotFound {
                    execution_id,
                    version,
                }
            } else {
                StorageError::Backend(e.to_string())
            }
        })
    }

    async fn load_latest(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<(u64, Vec<u8>)>, StorageError> {
        let dir = self.execution_dir(execution_id);

        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(StorageError::Backend(e.to_string())),
        };

        let mut latest_version: Option<u64> = None;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?
        {
            if let Some(version) = parse_version(&entry.path()) {
                latest_version = Some(latest_version.map_or(version, |v| v.max(version)));
            }
        }

        let Some(version) = latest_version else {
            return Ok(None);
        };

        let data = self.load(execution_id, version).await?;

        Ok(Some((version, data)))
    }

    async fn delete(&self, execution_id: ExecutionId) -> Result<(), StorageError> {
        let dir = self.execution_dir(execution_id);

        match fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Backend(e.to_string())),
        }
    }
}

fn parse_version(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.parse().ok()
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (FilesystemStore, PathBuf) {
        let base_dir = std::env::temp_dir().join(format!("miranda-fs-test-{}", ExecutionId::new()));
        (FilesystemStore::new(&base_dir), base_dir)
    }

    #[tokio::test]
    async fn save_then_load_returns_same_data() {
        let (store, base_dir) = store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"hello").await.unwrap();
        let data = store.load(execution_id, 1).await.unwrap();

        assert_eq!(data, b"hello");

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn load_missing_snapshot_returns_not_found() {
        let (store, base_dir) = store();
        let execution_id = ExecutionId::new();

        let err = store.load(execution_id, 1).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::SnapshotNotFound {
                execution_id: id,
                version: 1,
            } if id == execution_id
        ));

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn load_missing_execution_dir_returns_not_found() {
        let (store, _base_dir) = store();
        let execution_id = ExecutionId::new();

        let err = store.load(execution_id, 1).await.unwrap_err();

        assert!(matches!(err, StorageError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn load_latest_with_no_snapshots_returns_none() {
        let (store, base_dir) = store();
        let execution_id = ExecutionId::new();

        let result = store.load_latest(execution_id).await.unwrap();

        assert!(result.is_none());

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn load_latest_with_missing_execution_dir_returns_none() {
        let (store, _base_dir) = store();
        let execution_id = ExecutionId::new();

        let result = store.load_latest(execution_id).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn load_latest_returns_highest_version() {
        let (store, base_dir) = store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"v1").await.unwrap();
        store.save(execution_id, 3, b"v3").await.unwrap();
        store.save(execution_id, 2, b"v2").await.unwrap();

        let (version, data) = store.load_latest(execution_id).await.unwrap().unwrap();

        assert_eq!(version, 3);
        assert_eq!(data, b"v3");

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn save_overwrites_existing_version() {
        let (store, base_dir) = store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"first").await.unwrap();
        store.save(execution_id, 1, b"second").await.unwrap();

        let data = store.load(execution_id, 1).await.unwrap();

        assert_eq!(data, b"second");

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn snapshots_are_isolated_per_execution_id() {
        let (store, base_dir) = store();
        let execution_a = ExecutionId::new();
        let execution_b = ExecutionId::new();

        store.save(execution_a, 1, b"a").await.unwrap();

        let result = store.load(execution_b, 1).await;

        assert!(matches!(
            result,
            Err(StorageError::SnapshotNotFound { .. })
        ));

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn delete_removes_all_snapshots() {
        let (store, base_dir) = store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"v1").await.unwrap();
        store.save(execution_id, 2, b"v2").await.unwrap();

        store.delete(execution_id).await.unwrap();

        let result = store.load_latest(execution_id).await.unwrap();
        assert!(result.is_none());

        let err = store.load(execution_id, 1).await.unwrap_err();
        assert!(matches!(err, StorageError::SnapshotNotFound { .. }));

        let _ = fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn delete_on_missing_execution_dir_is_ok() {
        let (store, _base_dir) = store();
        let execution_id = ExecutionId::new();

        let result = store.delete(execution_id).await;

        assert!(result.is_ok());
    }

    #[test]
    fn parse_version_parses_numeric_file_stem() {
        let path = Path::new("42.snapshot");

        assert_eq!(parse_version(path), Some(42));
    }

    #[test]
    fn parse_version_rejects_non_numeric_file_stem() {
        let path = Path::new("latest.snapshot");

        assert_eq!(parse_version(path), None);
    }

    #[test]
    fn parse_version_rejects_temp_files() {
        let path = Path::new("42.snapshot.tmp");

        assert_eq!(parse_version(path), None);
    }
}
