use miranda_core::id::{ExecutionId, WorkflowTaskId};
use std::{future::Future, pin::Pin, sync::Arc};

use crate::error::StorageError;

pub trait ArtifactStore: Send + Sync {
    fn save_artifact<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &'a str,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn load_artifact<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>>;

    fn list_artifacts<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>>;

    fn delete_artifacts<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;
}

impl ArtifactStore for Arc<dyn ArtifactStore + '_> {
    fn save_artifact<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &'a str,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).save_artifact(execution_id, task_id, path, data)
    }

    fn load_artifact<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, StorageError>> + Send + 'a>> {
        (**self).load_artifact(execution_id, task_id, path)
    }

    fn list_artifacts<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>> {
        (**self).list_artifacts(execution_id, task_id)
    }

    fn delete_artifacts<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        (**self).delete_artifacts(execution_id, task_id)
    }
}
