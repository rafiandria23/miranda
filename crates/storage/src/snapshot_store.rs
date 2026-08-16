use miranda_core::id::ExecutionId;
use std::{future::Future, pin::Pin, sync::Arc};

use crate::error::StorageError;

pub trait SnapshotStore: Send + Sync {
    fn save<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn load<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>>;

    fn load_latest<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, StorageError>> + Send + 'a>>;

    fn delete<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;
}

impl SnapshotStore for Arc<dyn SnapshotStore + '_> {
    fn save<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).save(execution_id, version, data)
    }

    fn load<'a>(
        &'a self,
        execution_id: ExecutionId,
        version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>> {
        (**self).load(execution_id, version)
    }

    fn load_latest<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, StorageError>> + Send + 'a>>
    {
        (**self).load_latest(execution_id)
    }

    fn delete<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).delete(execution_id)
    }
}
