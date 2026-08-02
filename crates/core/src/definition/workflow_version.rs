use serde::{Deserialize, Serialize};

use crate::error::ExecutionError;
use crate::id::{WorkflowId, WorkflowVersionId};

use super::WorkflowDefinition;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersion {
    id: WorkflowVersionId,
    workflow_id: WorkflowId,
    version: u32,
    definition: WorkflowDefinition,
}

impl WorkflowVersion {
    pub fn new(
        workflow_id: WorkflowId,
        version: u32,
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

    pub fn version(&self) -> u32 {
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
    use super::*;
    use crate::definition::WorkflowTask;
    use crate::id::WorkflowTaskId;

    #[test]
    fn creates_workflow_version() {
        let workflow_id = WorkflowId::new();
        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![task]).unwrap();
        let version = WorkflowVersion::new(workflow_id, 3, definition).unwrap();

        assert_eq!(version.workflow_id(), workflow_id);
        assert_eq!(version.version(), 3);
        assert_eq!(version.definition().tasks().len(), 1);
        assert_ne!(version.id(), WorkflowVersionId::new());
    }

    #[test]
    fn rejects_zero_version() {
        let workflow_id = WorkflowId::new();
        let definition = WorkflowDefinition::new(vec![]).unwrap();
        let version = WorkflowVersion::new(workflow_id, 0, definition);

        assert!(matches!(
            version,
            Err(ExecutionError::InvalidWorkflowVersion)
        ));
    }
}
