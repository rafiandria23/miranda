use crate::id::WorkflowId;

use super::{WorkflowDefinition, WorkflowError, WorkflowVersion};

pub struct Workflow {
    id: WorkflowId,
    name: String,
    versions: Vec<WorkflowVersion>,
}

impl Workflow {
    pub fn new(name: String) -> Result<Self, WorkflowError> {
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

    pub fn validate(&self) -> Result<(), WorkflowError> {
        self.validate_name()?;
        self.validate_versions()?;

        Ok(())
    }

    fn validate_name(&self) -> Result<(), WorkflowError> {
        if self.name.trim().is_empty() {
            return Err(WorkflowError::InvalidName);
        }

        Ok(())
    }

    fn validate_versions(&self) -> Result<(), WorkflowError> {
        for version in &self.versions {
            version.validate()?;
        }

        Ok(())
    }

    pub fn add_version(
        &mut self,
        definition: WorkflowDefinition,
    ) -> Result<&WorkflowVersion, WorkflowError> {
        let version = self.versions.len() as u32 + 1;

        let workflow_version = WorkflowVersion::new(self.id, version, definition)?;

        self.versions.push(workflow_version);

        Ok(self
            .versions
            .last()
            .expect("workflow version was just inserted"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow() {
        let workflow = Workflow::new("send_email".to_owned()).unwrap();

        assert_eq!(workflow.name(), "send_email");
        assert!(workflow.versions().is_empty());
    }

    #[test]
    fn rejects_empty_name() {
        let workflow = Workflow::new("".to_owned());

        assert!(workflow.is_err());
    }

    #[test]
    fn rejects_whitespace_name() {
        let workflow = Workflow::new("   ".to_owned());

        assert!(workflow.is_err());
    }

    #[test]
    fn adds_first_version() {
        let mut workflow = Workflow::new("send_email".to_owned()).unwrap();

        let definition = WorkflowDefinition::new(vec![]).unwrap();

        let version = workflow.add_version(definition).unwrap();

        assert_eq!(version.version(), 1);
    }

    #[test]
    fn adds_incrementing_versions() {
        let mut workflow = Workflow::new("send_email".to_owned()).unwrap();

        let first_definition = WorkflowDefinition::new(vec![]).unwrap();
        let second_definition = WorkflowDefinition::new(vec![]).unwrap();

        let first_version = workflow.add_version(first_definition).unwrap();

        assert_eq!(first_version.version(), 1);

        let second_version = workflow.add_version(second_definition).unwrap();

        assert_eq!(second_version.version(), 2);
    }
}
