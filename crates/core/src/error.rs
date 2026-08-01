use thiserror::Error;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("workflow name cannot be empty")]
    EmptyWorkflowName,
}

#[derive(Debug, Error)]
pub enum WorkflowVersionError {
    #[error("workflow version must be greater than zero")]
    InvalidVersion,
}
