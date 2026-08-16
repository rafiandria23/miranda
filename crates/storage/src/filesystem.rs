use miranda_core::id::ExecutionId;
use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
};
use tokio::fs;

use crate::{error::StorageError, snapshot_store::SnapshotStore};

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

fn parse_version(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.parse().ok()
}

// =========================================================================
// Snapshot Store Implementation
// =========================================================================

impl SnapshotStore for FilesystemStore {
    fn save<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
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
        })
    }

    fn load<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
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
        })
    }

    fn load_latest<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, StorageError>> + Send + 'a>>
    {
        Box::pin(async move {
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

            let data = SnapshotStore::load(self, execution_id, version).await?;

            Ok(Some((version, data)))
        })
    }

    fn delete<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let dir = self.execution_dir(execution_id);

            match fs::remove_dir_all(&dir).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(StorageError::Backend(e.to_string())),
            }
        })
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("miranda-fs-store-{}", ExecutionId::new()));
            Self(path)
        }

        fn store(&self) -> FilesystemStore {
            FilesystemStore::new(self.0.clone())
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    // ---------------------------------------------------------------
    // parse_version
    // ---------------------------------------------------------------

    #[test]
    fn parse_version_extracts_numeric_stem() {
        assert_eq!(parse_version(Path::new("42.snapshot")), Some(42));
    }

    #[test]
    fn parse_version_rejects_non_numeric_stem() {
        assert_eq!(parse_version(Path::new("latest.snapshot")), None);
    }

    #[test]
    fn parse_version_rejects_path_without_stem() {
        assert_eq!(parse_version(Path::new("")), None);
    }

    // ---------------------------------------------------------------
    // SnapshotStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn save_then_load_returns_same_data() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"hello").await.unwrap();

        let data = store.load(execution_id, 1).await.unwrap();

        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn save_overwrites_existing_version() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"first").await.unwrap();
        store.save(execution_id, 1, b"second").await.unwrap();

        let data = store.load(execution_id, 1).await.unwrap();

        assert_eq!(data, b"second");
    }

    #[tokio::test]
    async fn save_keeps_distinct_versions_separate() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"v1").await.unwrap();
        store.save(execution_id, 2, b"v2").await.unwrap();

        assert_eq!(store.load(execution_id, 1).await.unwrap(), b"v1");
        assert_eq!(store.load(execution_id, 2).await.unwrap(), b"v2");
    }

    #[tokio::test]
    async fn load_missing_version_returns_snapshot_not_found() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"data").await.unwrap();

        let err = store.load(execution_id, 2).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::SnapshotNotFound {
                execution_id: eid,
                version: 2,
            } if eid == execution_id
        ));
    }

    #[tokio::test]
    async fn load_missing_execution_returns_snapshot_not_found() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        let err = store.load(execution_id, 1).await.unwrap_err();

        assert!(matches!(err, StorageError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn load_latest_on_missing_execution_returns_none() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        let result = store.load_latest(execution_id).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn load_latest_returns_highest_version() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"v1").await.unwrap();
        store.save(execution_id, 3, b"v3").await.unwrap();
        store.save(execution_id, 2, b"v2").await.unwrap();

        let (version, data) = store.load_latest(execution_id).await.unwrap().unwrap();

        assert_eq!(version, 3);
        assert_eq!(data, b"v3");
    }

    #[tokio::test]
    async fn load_latest_ignores_unrelated_files_in_dir() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"v1").await.unwrap();

        let stray = store.execution_dir(execution_id).join("notes.txt");
        fs::write(&stray, b"not a snapshot").await.unwrap();

        let (version, data) = store.load_latest(execution_id).await.unwrap().unwrap();

        assert_eq!(version, 1);
        assert_eq!(data, b"v1");
    }

    #[tokio::test]
    async fn delete_removes_all_versions() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save(execution_id, 1, b"v1").await.unwrap();
        store.save(execution_id, 2, b"v2").await.unwrap();

        store.delete(execution_id).await.unwrap();

        let result = store.load_latest(execution_id).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn delete_on_missing_execution_is_ok() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.delete(execution_id).await.unwrap();
    }

    #[tokio::test]
    async fn delete_does_not_affect_other_executions() {
        let dir = TempDir::new();
        let store = dir.store();
        let a = ExecutionId::new();
        let b = ExecutionId::new();

        store.save(a, 1, b"a").await.unwrap();
        store.save(b, 1, b"b").await.unwrap();

        store.delete(a).await.unwrap();

        assert!(store.load_latest(a).await.unwrap().is_none());
        assert_eq!(store.load(b, 1).await.unwrap(), b"b");
    }
}
