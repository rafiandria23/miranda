pub mod definition;
pub mod task;
pub mod version;

pub use definition::WorkflowDefinition;
pub use task::WorkflowTask;
pub use version::WorkflowVersion;

use serde::{Deserialize, Serialize};

use crate::{error::ExecutionError, id::WorkflowId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use crate::id::WorkflowTaskId;

    use super::*;

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
    fn new_assigns_a_unique_id() {
        let first = Workflow::new("onboarding".to_owned()).unwrap();
        let second = Workflow::new("onboarding".to_owned()).unwrap();

        assert_ne!(first.id(), second.id());
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
    fn add_version_stores_the_given_definition() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();
        let definition = single_task_definition();

        workflow.add_version(definition.clone()).unwrap();

        assert_eq!(workflow.versions()[0].definition(), &definition);
    }

    #[test]
    fn validate_succeeds_for_well_formed_workflow() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();
        workflow.add_version(single_task_definition()).unwrap();

        assert!(workflow.validate().is_ok());
    }

    #[test]
    fn validate_succeeds_for_workflow_without_versions() {
        let workflow = Workflow::new("onboarding".to_owned()).unwrap();

        assert!(workflow.validate().is_ok());
    }

    #[test]
    fn validate_fails_when_name_is_invalid() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();
        workflow.name = "".to_owned();

        assert_eq!(
            workflow.validate(),
            Err(ExecutionError::InvalidWorkflowName)
        );
    }

    #[test]
    fn workflow_round_trips_through_json() {
        let mut workflow = Workflow::new("onboarding".to_owned()).unwrap();
        workflow.add_version(single_task_definition()).unwrap();

        let json = serde_json::to_string(&workflow).unwrap();
        let deserialized: Workflow = serde_json::from_str(&json).unwrap();

        assert_eq!(workflow, deserialized);
    }
}
