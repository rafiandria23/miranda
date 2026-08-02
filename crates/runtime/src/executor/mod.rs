mod error;
mod executor;

pub use error::ExecutorError;
pub use executor::{NoopExecutor, TaskExecutor, TaskResult};
