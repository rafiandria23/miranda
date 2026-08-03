use miranda_core::{
    definition::WorkflowDefinition,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    instance::Execution,
};
use std::future::Future;

use crate::error::StorageError;

pub trait WorkflowStore: Send + Sync {
    // Workflow Definition (versioned)

    fn save_definition(
        &self,
        workflow_id: WorkflowId,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &WorkflowDefinition,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn get_versions(
        &self,
        workflow_id: WorkflowId,
    ) -> impl Future<Output = Result<Vec<WorkflowVersionId>, StorageError>> + Send;

    fn get_definition(
        &self,
        version_id: WorkflowVersionId,
    ) -> impl Future<Output = Result<WorkflowDefinition, StorageError>> + Send;

    // Execution State Management

    fn save_execution(
        &self,
        execution: &Execution,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn update_execution(
        &self,
        execution: &Execution,
        expected_version: u64,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn get_execution(
        &self,
        execution_id: ExecutionId,
    ) -> impl Future<Output = Result<Execution, StorageError>> + Send;

    fn get_active_executions(
        &self,
    ) -> impl Future<Output = Result<Vec<Execution>, StorageError>> + Send;
}
