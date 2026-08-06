use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use std::{collections::HashMap, sync::Arc};
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
    async fn save_definition(
        &self,
        workflow_id: WorkflowId,
        name: &str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &WorkflowDefinition,
    ) -> Result<(), StorageError> {
        let _ = name;

        let mut definitions = self.definitions.write().await;

        definitions.insert(version_id, (workflow_id, version, definition.clone()));

        Ok(())
    }

    async fn get_versions(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<WorkflowVersionId>, StorageError> {
        let definitions = self.definitions.read().await;

        let versions: Vec<_> = definitions
            .iter()
            .filter(|(_, (wf_id, _, _))| *wf_id == workflow_id)
            .map(|(version_id, _)| *version_id)
            .collect();

        Ok(versions)
    }

    async fn get_definition(
        &self,
        version_id: WorkflowVersionId,
    ) -> Result<WorkflowDefinition, StorageError> {
        let definitions = self.definitions.read().await;

        definitions
            .get(&version_id)
            .map(|(_, _, definition)| definition.clone())
            .ok_or(StorageError::WorkflowVersionNotFound(version_id))
    }

    async fn save_execution(&self, execution: &Execution) -> Result<(), StorageError> {
        let mut executions = self.executions.write().await;

        executions.insert(execution.id(), (execution.clone(), 1));

        Ok(())
    }

    async fn update_execution(
        &self,
        execution: &Execution,
        expected_version: u64,
    ) -> Result<(), StorageError> {
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
    }

    async fn get_execution(
        &self,
        execution_id: ExecutionId,
    ) -> Result<(Execution, u64), StorageError> {
        let executions = self.executions.read().await;

        executions
            .get(&execution_id)
            .map(|(execution, version)| (execution.clone(), *version))
            .ok_or(StorageError::ExecutionNotFound(execution_id))
    }

    async fn get_active_executions(&self) -> Result<Vec<Execution>, StorageError> {
        let executions = self.executions.read().await;

        let active_executions = executions
            .values()
            .map(|(execution, _)| execution.clone())
            .filter(|execution| !execution.is_finished())
            .collect();

        Ok(active_executions)
    }
}
