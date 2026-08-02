use crate::error::RuntimeError;

#[derive(Debug)]
pub enum TaskResult {
    Success,
    Failure(RuntimeError),
}
