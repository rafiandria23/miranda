use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use std::{future::Future, pin::Pin, sync::Arc};

use crate::StorageError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

pub trait WorkflowStore: Send + Sync {
    fn save_definition<'a>(
        &'a self,
        workflow_id: WorkflowId,
        name: &'a str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &'a WorkflowDefinition,
    ) -> BoxFuture<'a, ()>;

    fn get_versions<'a>(&'a self, workflow_id: WorkflowId)
    -> BoxFuture<'a, Vec<WorkflowVersionId>>;

    fn get_definition<'a>(
        &'a self,
        version_id: WorkflowVersionId,
    ) -> BoxFuture<'a, WorkflowDefinition>;

    fn save_execution<'a>(&'a self, execution: &'a Execution) -> BoxFuture<'a, ()>;

    fn update_execution<'a>(
        &'a self,
        execution: &'a Execution,
        expected_version: u64,
    ) -> BoxFuture<'a, ()>;

    fn get_execution<'a>(&'a self, execution_id: ExecutionId) -> BoxFuture<'a, (Execution, u64)>;

    fn get_active_executions<'a>(&'a self) -> BoxFuture<'a, Vec<Execution>>;
}

impl WorkflowStore for Arc<dyn WorkflowStore> {
    fn save_definition<'a>(
        &'a self,
        workflow_id: WorkflowId,
        name: &'a str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &'a WorkflowDefinition,
    ) -> BoxFuture<'a, ()> {
        (**self).save_definition(workflow_id, name, version_id, version, definition)
    }

    fn get_versions<'a>(
        &'a self,
        workflow_id: WorkflowId,
    ) -> BoxFuture<'a, Vec<WorkflowVersionId>> {
        (**self).get_versions(workflow_id)
    }

    fn get_definition<'a>(
        &'a self,
        version_id: WorkflowVersionId,
    ) -> BoxFuture<'a, WorkflowDefinition> {
        (**self).get_definition(version_id)
    }

    fn save_execution<'a>(&'a self, execution: &'a Execution) -> BoxFuture<'a, ()> {
        (**self).save_execution(execution)
    }

    fn update_execution<'a>(
        &'a self,
        execution: &'a Execution,
        expected_version: u64,
    ) -> BoxFuture<'a, ()> {
        (**self).update_execution(execution, expected_version)
    }

    fn get_execution<'a>(&'a self, execution_id: ExecutionId) -> BoxFuture<'a, (Execution, u64)> {
        (**self).get_execution(execution_id)
    }

    fn get_active_executions<'a>(&'a self) -> BoxFuture<'a, Vec<Execution>> {
        (**self).get_active_executions()
    }
}
