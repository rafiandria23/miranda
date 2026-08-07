use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;

use crate::{StorageError, WorkflowStore};

#[derive(Default, Clone)]
pub struct MemoryStore {
    definitions: Arc<RwLock<HashMap<WorkflowVersionId, (WorkflowId, u64, WorkflowDefinition)>>>,
    executions: Arc<RwLock<HashMap<ExecutionId, (Execution, u64)>>>, // (Execution, Version)
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl WorkflowStore for MemoryStore {
    fn save_definition<'a>(
        &'a self,
        workflow_id: WorkflowId,
        name: &'a str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &'a WorkflowDefinition,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let _ = name;

            let mut definitions = self.definitions.write().await;

            definitions.insert(version_id, (workflow_id, version, definition.clone()));

            Ok(())
        })
    }

    fn get_versions<'a>(
        &'a self,
        workflow_id: WorkflowId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkflowVersionId>, StorageError>> + Send + 'a>>
    {
        Box::pin(async move {
            let definitions = self.definitions.read().await;

            let versions: Vec<_> = definitions
                .iter()
                .filter(|(_, (wf_id, _, _))| *wf_id == workflow_id)
                .map(|(version_id, _)| *version_id)
                .collect();

            Ok(versions)
        })
    }

    fn get_definition<'a>(
        &'a self,
        version_id: WorkflowVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<WorkflowDefinition, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let definitions = self.definitions.read().await;

            definitions
                .get(&version_id)
                .map(|(_, _, definition)| definition.clone())
                .ok_or(StorageError::WorkflowVersionNotFound(version_id))
        })
    }

    fn save_execution<'a>(
        &'a self,
        execution: &'a Execution,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut executions = self.executions.write().await;

            executions.insert(execution.id(), (execution.clone(), 1));

            Ok(())
        })
    }

    fn update_execution<'a>(
        &'a self,
        execution: &'a Execution,
        expected_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut executions = self.executions.write().await;

            let current_version = match executions.get(&execution.id()) {
                Some((_, version)) => *version,
                None => return Err(StorageError::ExecutionNotFound(execution.id())),
            };

            if current_version != expected_version {
                return Err(StorageError::OptimisticLockFailed {
                    id: execution.id(),
                    expected: expected_version,
                    actual: current_version,
                });
            }

            executions.insert(execution.id(), (execution.clone(), current_version + 1));

            Ok(())
        })
    }

    fn get_execution<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(Execution, u64), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let executions = self.executions.read().await;

            executions
                .get(&execution_id)
                .map(|(execution, version)| (execution.clone(), *version))
                .ok_or(StorageError::ExecutionNotFound(execution_id))
        })
    }

    fn get_active_executions<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Execution>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let executions = self.executions.read().await;

            let active_executions = executions
                .values()
                .map(|(execution, _)| execution.clone())
                .filter(|execution| !execution.is_finished())
                .collect();

            Ok(active_executions)
        })
    }
}
