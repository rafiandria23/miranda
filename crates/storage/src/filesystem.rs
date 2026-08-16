use miranda_core::id::{ExecutionId, WorkflowTaskId};
use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
};
use tokio::fs;

use crate::{artifact_store::ArtifactStore, error::StorageError, snapshot_store::SnapshotStore};

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

    // ---------------------------------------------------------------
    // Snapshot path helpers
    // ---------------------------------------------------------------

    fn execution_dir(&self, execution_id: ExecutionId) -> PathBuf {
        self.base_dir.join(execution_id.to_string())
    }

    fn snapshot_path(&self, execution_id: ExecutionId, version: u64) -> PathBuf {
        self.execution_dir(execution_id)
            .join(format!("{version}.snapshot"))
    }

    // ---------------------------------------------------------------
    // Artifact path helpers
    // ---------------------------------------------------------------

    fn task_dir(&self, execution_id: ExecutionId, task_id: WorkflowTaskId) -> PathBuf {
        self.execution_dir(execution_id)
            .join("tasks")
            .join(task_id.to_string())
    }

    fn artifact_path(
        &self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &str,
    ) -> PathBuf {
        self.task_dir(execution_id, task_id).join(path)
    }
}

fn parse_version(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.parse().ok()
}

fn walk_dir<'a>(
    root: &'a Path,
    dir: &'a Path,
    paths: &'a mut Vec<String>,
) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
    Box::pin(async move {
        let mut entries = match fs::read_dir(dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StorageError::Backend(e.to_string())),
        };

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            if file_type.is_dir() {
                walk_dir(root, &path, paths).await?;
            } else if path.extension().and_then(|e| e.to_str()) != Some("tmp")
                && let Ok(relative) = path.strip_prefix(root)
                && let Some(s) = relative.to_str()
            {
                paths.push(s.to_owned());
            }
        }

        Ok(())
    })
}

// =========================================================================
// Snapshot Store Implementation
// =========================================================================

impl SnapshotStore for FilesystemStore {
    fn save_snapshot<'a>(
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

    fn load_snapshot<'a>(
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

    fn load_latest_snapshot<'a>(
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

            let data = self.load_snapshot(execution_id, version).await?;

            Ok(Some((version, data)))
        })
    }

    fn delete_snapshots<'a>(
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
// Snapshot Store Implementation
// =========================================================================

impl ArtifactStore for FilesystemStore {
    fn save_artifact<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &'a str,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let full_path = self.artifact_path(execution_id, task_id, path);

            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
            }

            let tmp_path = {
                let mut tmp = full_path.clone().into_os_string();
                tmp.push(".tmp");
                PathBuf::from(tmp)
            };

            fs::write(&tmp_path, data)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            fs::rename(&tmp_path, &full_path)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn load_artifact<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let full_path = self.artifact_path(execution_id, task_id, path);

            match fs::metadata(&full_path).await {
                Ok(meta) if meta.is_dir() => {
                    return Err(StorageError::ArtifactIsDirectory {
                        execution_id,
                        task_id,
                        path: path.to_owned(),
                    });
                }
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(StorageError::ArtifactNotFound {
                        execution_id,
                        task_id,
                        path: path.to_owned(),
                    });
                }
                Err(e) => return Err(StorageError::Backend(e.to_string())),
            }

            fs::read(&full_path)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))
        })
    }

    fn list_artifacts<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let dir = self.task_dir(execution_id, task_id);
            let mut paths = Vec::new();

            walk_dir(&dir, &dir, &mut paths).await?;

            Ok(paths)
        })
    }

    fn delete_artifacts<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let dir = self.task_dir(execution_id, task_id);

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

        store
            .save_snapshot(execution_id, 1, b"hello")
            .await
            .unwrap();

        let data = store.load_snapshot(execution_id, 1).await.unwrap();

        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn save_overwrites_existing_version() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store
            .save_snapshot(execution_id, 1, b"first")
            .await
            .unwrap();
        store
            .save_snapshot(execution_id, 1, b"second")
            .await
            .unwrap();

        let data = store.load_snapshot(execution_id, 1).await.unwrap();

        assert_eq!(data, b"second");
    }

    #[tokio::test]
    async fn save_keeps_distinct_versions_separate() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save_snapshot(execution_id, 1, b"v1").await.unwrap();
        store.save_snapshot(execution_id, 2, b"v2").await.unwrap();

        assert_eq!(store.load_snapshot(execution_id, 1).await.unwrap(), b"v1");
        assert_eq!(store.load_snapshot(execution_id, 2).await.unwrap(), b"v2");
    }

    #[tokio::test]
    async fn load_missing_version_returns_snapshot_not_found() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save_snapshot(execution_id, 1, b"data").await.unwrap();

        let err = store.load_snapshot(execution_id, 2).await.unwrap_err();

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

        let err = store.load_snapshot(execution_id, 1).await.unwrap_err();

        assert!(matches!(err, StorageError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn load_latest_on_missing_execution_returns_none() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        let result = store.load_latest_snapshot(execution_id).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn load_latest_returns_highest_version() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save_snapshot(execution_id, 1, b"v1").await.unwrap();
        store.save_snapshot(execution_id, 3, b"v3").await.unwrap();
        store.save_snapshot(execution_id, 2, b"v2").await.unwrap();

        let (version, data) = store
            .load_latest_snapshot(execution_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(version, 3);
        assert_eq!(data, b"v3");
    }

    #[tokio::test]
    async fn load_latest_ignores_unrelated_files_in_dir() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save_snapshot(execution_id, 1, b"v1").await.unwrap();

        let stray = store.execution_dir(execution_id).join("notes.txt");
        fs::write(&stray, b"not a snapshot").await.unwrap();

        let (version, data) = store
            .load_latest_snapshot(execution_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(version, 1);
        assert_eq!(data, b"v1");
    }

    #[tokio::test]
    async fn delete_removes_all_versions() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.save_snapshot(execution_id, 1, b"v1").await.unwrap();
        store.save_snapshot(execution_id, 2, b"v2").await.unwrap();

        store.delete_snapshots(execution_id).await.unwrap();

        let result = store.load_latest_snapshot(execution_id).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn delete_on_missing_execution_is_ok() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();

        store.delete_snapshots(execution_id).await.unwrap();
    }

    #[tokio::test]
    async fn delete_does_not_affect_other_executions() {
        let dir = TempDir::new();
        let store = dir.store();
        let a = ExecutionId::new();
        let b = ExecutionId::new();

        store.save_snapshot(a, 1, b"a").await.unwrap();
        store.save_snapshot(b, 1, b"b").await.unwrap();

        store.delete_snapshots(a).await.unwrap();

        assert!(store.load_latest_snapshot(a).await.unwrap().is_none());
        assert_eq!(store.load_snapshot(b, 1).await.unwrap(), b"b");
    }

    // ---------------------------------------------------------------
    // ArtifactStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn save_artifact_then_load_returns_same_data() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_id, "out.txt", b"hello")
            .await
            .unwrap();

        let data = store
            .load_artifact(execution_id, task_id, "out.txt")
            .await
            .unwrap();

        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn save_artifact_overwrites_existing_path() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_id, "out.txt", b"first")
            .await
            .unwrap();
        store
            .save_artifact(execution_id, task_id, "out.txt", b"second")
            .await
            .unwrap();

        let data = store
            .load_artifact(execution_id, task_id, "out.txt")
            .await
            .unwrap();

        assert_eq!(data, b"second");
    }

    #[tokio::test]
    async fn save_artifact_supports_nested_paths() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_id, "nested/dir/out.txt", b"nested")
            .await
            .unwrap();

        let data = store
            .load_artifact(execution_id, task_id, "nested/dir/out.txt")
            .await
            .unwrap();

        assert_eq!(data, b"nested");
    }

    #[tokio::test]
    async fn load_artifact_missing_path_returns_artifact_not_found() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        let err = store
            .load_artifact(execution_id, task_id, "missing.txt")
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            StorageError::ArtifactNotFound {
                execution_id: eid,
                task_id: tid,
                ref path,
            } if eid == execution_id && tid == task_id && path == "missing.txt"
        ));
    }

    #[tokio::test]
    async fn list_artifacts_on_missing_task_returns_empty() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        let paths = store.list_artifacts(execution_id, task_id).await.unwrap();

        assert!(paths.is_empty());
    }

    #[tokio::test]
    async fn list_artifacts_returns_all_saved_paths_including_nested() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_id, "a.txt", b"a")
            .await
            .unwrap();
        store
            .save_artifact(execution_id, task_id, "nested/b.txt", b"b")
            .await
            .unwrap();

        let mut paths = store.list_artifacts(execution_id, task_id).await.unwrap();
        paths.sort();

        assert_eq!(paths, vec!["a.txt".to_string(), "nested/b.txt".to_string()]);
    }

    #[tokio::test]
    async fn list_artifacts_excludes_tmp_files() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_id, "a.txt", b"a")
            .await
            .unwrap();

        let stray = store.task_dir(execution_id, task_id).join("stray.tmp");
        fs::write(&stray, b"tmp data").await.unwrap();

        let paths = store.list_artifacts(execution_id, task_id).await.unwrap();

        assert_eq!(paths, vec!["a.txt".to_string()]);
    }

    #[tokio::test]
    async fn delete_artifacts_removes_all_paths() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_id, "a.txt", b"a")
            .await
            .unwrap();
        store
            .save_artifact(execution_id, task_id, "b.txt", b"b")
            .await
            .unwrap();

        store.delete_artifacts(execution_id, task_id).await.unwrap();

        let paths = store.list_artifacts(execution_id, task_id).await.unwrap();

        assert!(paths.is_empty());
    }

    #[tokio::test]
    async fn delete_artifacts_on_missing_task_is_ok() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();

        store.delete_artifacts(execution_id, task_id).await.unwrap();
    }

    #[tokio::test]
    async fn delete_artifacts_does_not_affect_other_tasks() {
        let dir = TempDir::new();
        let store = dir.store();
        let execution_id = ExecutionId::new();
        let task_a = WorkflowTaskId::new();
        let task_b = WorkflowTaskId::new();

        store
            .save_artifact(execution_id, task_a, "a.txt", b"a")
            .await
            .unwrap();
        store
            .save_artifact(execution_id, task_b, "b.txt", b"b")
            .await
            .unwrap();

        store.delete_artifacts(execution_id, task_a).await.unwrap();

        assert!(
            store
                .list_artifacts(execution_id, task_a)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .load_artifact(execution_id, task_b, "b.txt")
                .await
                .unwrap(),
            b"b"
        );
    }
}
