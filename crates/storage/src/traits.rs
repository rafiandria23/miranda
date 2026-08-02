use miranda_core::{
    definition::WorkflowDefinition,
    id::{ExecutionId, WorkflowId},
    instance::Execution,
};
use std::{future::Future, process::Output};

use crate::error::StorageError;

pub trait WorkflowStore: Send + Sync {
    // Workflow Definition
    fn save_definition(
        &self,
        id: WorkflowId,
        definition: &WorkflowDefinition,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn get_definition(
        &self,
        id: WorkflowId,
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
        id: ExecutionId,
    ) -> impl Future<Output = Result<Execution, StorageError>> + Send;
}
