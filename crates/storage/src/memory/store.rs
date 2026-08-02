use miranda_core::{
    definition::WorkflowDefinition,
    id::{ExecutionId, WorkflowId},
    instance::Execution,
};
use std::{collections::HashMap, sync::RwLock};

use crate::{error::StorageError, traits::WorkflowStore};

#[derive(Default)]
pub struct MemoryStore {
    definitions: RwLock<HashMap<WorkflowId, WorkflowDefinition>>,
    executions: RwLock<HashMap<ExecutionId, (Execution, u64)>>, // (Execution, Version)
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl WorkflowStore for MemoryStore {
    async fn save_definition(
        &self,
        id: WorkflowId,
        definition: &WorkflowDefinition,
    ) -> Result<(), StorageError> {
        let mut definitions = self
            .definitions
            .write()
            .map_err(|err| StorageError::Database(err.to_string()))?;

        definitions.insert(id, definition.clone());

        Ok(())
    }

    async fn get_definition(&self, id: WorkflowId) -> Result<WorkflowDefinition, StorageError> {
        let definitions = self
            .definitions
            .read()
            .map_err(|err| StorageError::Database(err.to_string()))?;

        definitions
            .get(&id)
            .cloned()
            .ok_or(StorageError::WorkflowNotFound(id))
    }

    async fn save_execution(&self, execution: &Execution) -> Result<(), StorageError> {
        let mut executions = self
            .executions
            .write()
            .map_err(|err| StorageError::Database(err.to_string()))?;

        executions.insert(execution.id(), (execution.clone(), 1));

        Ok(())
    }

    async fn update_execution(
        &self,
        execution: &Execution,
        expected_version: u64,
    ) -> Result<(), StorageError> {
        let mut executions = self
            .executions
            .write()
            .map_err(|err| StorageError::Database(err.to_string()))?;

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

    async fn get_execution(&self, id: ExecutionId) -> Result<Execution, StorageError> {
        let executions = self
            .executions
            .read()
            .map_err(|err| StorageError::Database(err.to_string()))?;

        executions
            .get(&id)
            .map(|(execution, _)| execution.clone())
            .ok_or(StorageError::ExecutionNotFound(id))
    }
}
