use thiserror::Error;

use crate::error::ExecutionError;

#[derive(Debug, Error)]
pub enum SpecError {
    #[error("invalid YAML: {0}")]
    Yaml(#[from] yaml_serde::Error),

    #[error("task '{0}' depends on unknown task '{1}'")]
    UnknownDependency(String, String),

    #[error("failed to serialize task config: {0}")]
    ConfigSerialization(#[from] serde_json::Error),

    #[error(transparent)]
    Domain(#[from] ExecutionError),
}
