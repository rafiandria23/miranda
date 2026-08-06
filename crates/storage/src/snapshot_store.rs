use miranda_core::id::ExecutionId;
use std::future::Future;

use crate::StorageError;

pub trait SnapshotStore: Send + Sync {
    fn save(
        &self,
        execution_id: ExecutionId,
        version: u64,
        data: &[u8],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn load(
        &self,
        execution_id: ExecutionId,
        version: u64,
    ) -> impl Future<Output = Result<Vec<u8>, StorageError>> + Send;

    fn load_latest(
        &self,
        execution_id: ExecutionId,
    ) -> impl Future<Output = Result<Option<(u64, Vec<u8>)>, StorageError>> + Send;

    fn delete(
        &self,
        execution_id: ExecutionId,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}
