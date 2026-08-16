use miranda_core::id::ExecutionId;
use std::{future::Future, pin::Pin, sync::Arc};

use crate::error::StorageError;

type LoadLatestFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, StorageError>> + Send + 'a>>;

pub trait SnapshotStore: Send + Sync {
    fn save_snapshot<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn load_snapshot<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>>;

    fn load_latest_snapshot<'a>(&'a self, execution_id: ExecutionId) -> LoadLatestFuture<'a>;

    fn delete_snapshots<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;
}

impl SnapshotStore for Arc<dyn SnapshotStore + '_> {
    fn save_snapshot<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).save_snapshot(execution_id, version, data)
    }

    fn load_snapshot<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>> {
        (**self).load_snapshot(execution_id, version)
    }

    fn load_latest_snapshot<'a>(&'a self, execution_id: ExecutionId) -> LoadLatestFuture<'a> {
        (**self).load_latest_snapshot(execution_id)
    }

    fn delete_snapshots<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).delete_snapshots(execution_id)
    }
}
