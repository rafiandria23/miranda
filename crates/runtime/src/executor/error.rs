use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("task execution failed: {0}")]
    Failed(String),
}
