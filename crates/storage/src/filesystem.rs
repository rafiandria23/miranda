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
