use serde::{Deserialize, Serialize};

use crate::{
    error::ExecutionError,
    id::{WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersion {
    id: WorkflowVersionId,
    workflow_id: WorkflowId,
    version: u64,
    definition: WorkflowDefinition,
}

impl WorkflowVersion {
    pub fn new(
        workflow_id: WorkflowId,
        version: u64,
        definition: WorkflowDefinition,
    ) -> Result<Self, ExecutionError> {
        let workflow_version = Self {
            id: WorkflowVersionId::new(),
            workflow_id,
            version,
            definition,
        };

        workflow_version.validate()?;
        Ok(workflow_version)
    }

    pub fn id(&self) -> WorkflowVersionId {
        self.id
    }

    pub fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn definition(&self) -> &WorkflowDefinition {
        &self.definition
    }

    pub fn validate(&self) -> Result<(), ExecutionError> {
        self.validate_version()?;
        Ok(())
    }

    fn validate_version(&self) -> Result<(), ExecutionError> {
        if self.version == 0 {
            return Err(ExecutionError::InvalidWorkflowVersion);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{id::WorkflowTaskId, workflow::WorkflowTask};

    use super::*;

    fn single_task_definition() -> WorkflowDefinition {
        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        WorkflowDefinition::new(vec![task]).unwrap()
    }

    #[test]
    fn new_assigns_a_fresh_id_and_stores_workflow_id_version_and_definition() {
        let workflow_id = WorkflowId::new();
        let definition = single_task_definition();

        let version = WorkflowVersion::new(workflow_id, 3, definition.clone()).unwrap();

        assert_eq!(version.workflow_id(), workflow_id);
        assert_eq!(version.version(), 3);
        assert_eq!(version.definition(), &definition);
    }

    #[test]
    fn new_assigns_unique_ids_across_calls() {
        let workflow_id = WorkflowId::new();

        let first = WorkflowVersion::new(workflow_id, 1, single_task_definition()).unwrap();
        let second = WorkflowVersion::new(workflow_id, 1, single_task_definition()).unwrap();

        assert_ne!(first.id(), second.id());
    }

    #[test]
    fn new_rejects_zero_version() {
        let workflow_id = WorkflowId::new();

        let version = WorkflowVersion::new(workflow_id, 0, single_task_definition());

        assert_eq!(version, Err(ExecutionError::InvalidWorkflowVersion));
    }

    #[test]
    fn validate_succeeds_for_nonzero_version() {
        let workflow_id = WorkflowId::new();
        let version = WorkflowVersion::new(workflow_id, 1, single_task_definition()).unwrap();

        assert!(version.validate().is_ok());
    }

    #[test]
    fn workflow_version_round_trips_through_json() {
        let workflow_id = WorkflowId::new();
        let version = WorkflowVersion::new(workflow_id, 5, single_task_definition()).unwrap();

        let json = serde_json::to_string(&version).unwrap();
        let deserialized: WorkflowVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(version, deserialized);
    }
}
