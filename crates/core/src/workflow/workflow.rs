use crate::error::DomainError;
use crate::id::WorkflowId;

pub struct Workflow {
    id: WorkflowId,
    name: String,
}

impl Workflow {
    pub fn new(name: String) -> Result<Self, DomainError> {
        if name.trim().is_empty() {
            return Err(DomainError::EmptyWorkflowName);
        }

        Ok(Self {
            id: WorkflowId::new(),
            name,
        })
    }

    pub fn id(&self) -> WorkflowId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow() {
        let workflow = Workflow::new("send_email".to_owned()).unwrap();

        assert_eq!(workflow.name(), "send_email");
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
}
