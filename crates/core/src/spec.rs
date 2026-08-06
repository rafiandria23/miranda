mod dto;
mod error;
mod lower;

pub use dto::WorkflowSpec;
pub use error::SpecError;

use crate::workflow::{Workflow, WorkflowDefinition};

pub fn compile(yaml: &str) -> Result<(Workflow, WorkflowDefinition), SpecError> {
    let spec: WorkflowSpec = yaml_serde::from_str(yaml)?;

    lower::lower(spec)
}
