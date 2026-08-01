use crate::error::WorkflowVersionError;
use crate::id::{WorkflowId, WorkflowVersionId};

pub struct WorkflowVersion {
    id: WorkflowVersionId,
    workflow_id: WorkflowId,
    version: u32,
}

impl WorkflowVersion {
    pub fn new(workflow_id: WorkflowId, version: u32) -> Result<Self, WorkflowVersionError> {
        if version == 0 {
            return Err(WorkflowVersionError::InvalidVersion);
        }

        Ok(Self {
            id: WorkflowVersionId::new(),
            workflow_id,
            version,
        })
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow_version() {
        let workflow_id = WorkflowId::new();
        let workflow_version = WorkflowVersion::new(workflow_id, 3).unwrap();

        assert_eq!(workflow_version.version(), 3);
    }

    #[test]
    fn rejects_zero_version() {
        let workflow_id = WorkflowId::new();
        let workflow_version = WorkflowVersion::new(workflow_id, 0);

        assert!(workflow_version.is_err());
    }
}
