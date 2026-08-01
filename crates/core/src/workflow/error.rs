use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("workflow name is invalid")]
    InvalidName,
}

#[derive(Debug, Error)]
pub enum WorkflowVersionError {
    #[error("workflow version is invalid")]
    InvalidVersion,
}

#[derive(Debug, Error)]
pub enum WorkflowDefinitionError {
    #[error("workflow task id is duplicated")]
    DuplicateTaskId,

    #[error("workflow task dependency does not exist")]
    UnknownDependency,

    #[error("workflow definition contains a cycle")]
    CyclicDependency,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkflowTaskError {
    #[error("workflow task type is invalid")]
    InvalidTaskType,

    #[error("workflow task dependency is duplicated")]
    DuplicateDependency,

    #[error("workflow task cannot depend on itself")]
    SelfDependency,
}
