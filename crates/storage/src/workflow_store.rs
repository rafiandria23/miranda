use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use std::future::Future;

use crate::StorageError;

pub trait WorkflowStore: Send + Sync {
    fn save_definition(
        &self,
        workflow_id: WorkflowId,
        name: &str,
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
    ) -> impl Future<Output = Result<(Execution, u64), StorageError>> + Send;

    fn get_active_executions(
        &self,
    ) -> impl Future<Output = Result<Vec<Execution>, StorageError>> + Send;
}
