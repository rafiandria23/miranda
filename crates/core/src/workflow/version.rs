use crate::id::{WorkflowId, WorkflowVersionId};

use super::{WorkflowDefinition, WorkflowVersionError};

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
    ) -> Result<Self, WorkflowVersionError> {
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

    pub fn validate(&self) -> Result<(), WorkflowVersionError> {
        self.validate_version()?;

        Ok(())
    }

    fn validate_version(&self) -> Result<(), WorkflowVersionError> {
        if self.version == 0 {
            return Err(WorkflowVersionError::InvalidVersion);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{WorkflowTask, id::WorkflowTaskId};

    use super::*;

    #[test]
    fn creates_workflow_version() {
        let workflow_id = WorkflowId::new();

        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        let version = WorkflowVersion::new(workflow_id, 3, definition).unwrap();

        assert_eq!(version.workflow_id(), workflow_id);
        assert_eq!(version.version(), 3);
    }

    #[test]
    fn rejects_zero_version() {
        let workflow_id = WorkflowId::new();

        let definition = WorkflowDefinition::new(vec![]).unwrap();

        let version = WorkflowVersion::new(workflow_id, 0, definition);

        assert!(version.is_err());
    }
}
