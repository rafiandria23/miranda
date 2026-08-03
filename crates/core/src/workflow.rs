pub mod definition;
pub mod task;
pub mod version;

pub use definition::WorkflowDefinition;
pub use task::WorkflowTask;
pub use version::WorkflowVersion;

use serde::{Deserialize, Serialize};

use crate::{error::ExecutionError, id::WorkflowId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workflow {
    id: WorkflowId,
    name: String,
    versions: Vec<WorkflowVersion>,
}

impl Workflow {
    pub fn new(name: String) -> Result<Self, ExecutionError> {
        let workflow = Self {
            id: WorkflowId::new(),
            name,
            versions: Vec::new(),
        };

        workflow.validate()?;
        Ok(workflow)
    }

    pub fn id(&self) -> WorkflowId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn versions(&self) -> &[WorkflowVersion] {
        &self.versions
    }

    pub fn validate(&self) -> Result<(), ExecutionError> {
        self.validate_name()?;
        self.validate_versions()?;
        Ok(())
    }

    fn validate_name(&self) -> Result<(), ExecutionError> {
        if self.name.trim().is_empty() {
            return Err(ExecutionError::InvalidWorkflowName);
        }
        Ok(())
    }

    fn validate_versions(&self) -> Result<(), ExecutionError> {
        for version in &self.versions {
            version.validate()?;
        }
        Ok(())
    }

    pub fn add_version(
        &mut self,
        definition: WorkflowDefinition,
    ) -> Result<&WorkflowVersion, ExecutionError> {
        let version = self.versions.len() as u64 + 1;
        let workflow_version = WorkflowVersion::new(self.id, version, definition)?;

        self.versions.push(workflow_version);
        Ok(self
            .versions
            .last()
            .expect("workflow version was just added"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::WorkflowTaskId;

    fn single_task_definition() -> WorkflowDefinition {
        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        WorkflowDefinition::new(vec![task]).unwrap()
    }

    #[test]
    fn creates_workflow() {
        let workflow = Workflow::new("onboarding".to_owned()).unwrap();

        assert_eq!(workflow.name(), "onboarding");
        assert!(workflow.versions().is_empty());
    }

    #[test]
    fn rejects_empty_name() {
        let workflow = Workflow::new("".to_owned());

        assert_eq!(workflow, Err(ExecutionError::InvalidWorkflowName));
    }

    #[test]
    fn rejects_whitespace_name() {
        let workflow = Workflow::new("   ".to_owned());

        assert_eq!(workflow, Err(ExecutionError::InvalidWorkflowName));
    }

    #[test]
    fn add_version_appends_first_version_starting_at_one() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();

        let version = workflow.add_version(single_task_definition()).unwrap();

        assert_eq!(version.version(), 1);
        assert_eq!(version.workflow_id(), workflow.id());
        assert_eq!(workflow.versions().len(), 1);
    }

    #[test]
    fn add_version_increments_version_number_across_calls() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();

        workflow.add_version(single_task_definition()).unwrap();
        workflow.add_version(single_task_definition()).unwrap();
        let third = workflow.add_version(single_task_definition()).unwrap();

        assert_eq!(third.version(), 3);
        assert_eq!(workflow.versions().len(), 3);
    }

    #[test]
    fn validate_succeeds_for_well_formed_workflow() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();
        workflow.add_version(single_task_definition()).unwrap();

        assert!(workflow.validate().is_ok());
    }
}
