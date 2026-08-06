use thiserror::Error;

use crate::error::ExecutionError;

#[derive(Debug, Error)]
pub enum SpecError {
    #[error("invalid YAML: {0}")]
    Yaml(#[from] yaml_serde::Error),

    #[error("task '{0}' depends on unknown task '{1}'")]
    UnknownDependency(String, String),

    #[error(transparent)]
    Domain(#[from] ExecutionError),
}
