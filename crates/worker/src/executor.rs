pub mod dispatch;
pub mod http;
pub mod in_process;
pub mod noop;
pub mod shell;
pub mod wait;

pub use dispatch::DispatchExecutor;
pub use http::HttpExecutor;
pub use in_process::InProcessExecutor;
pub use noop::NoopExecutor;
pub use shell::ShellExecutor;
pub use wait::WaitExecutor;

use miranda_core::workflow::WorkflowTask;
use std::future::Future;

use crate::WorkerError;

pub trait TaskExecutor: Send + Sync {
    fn execute(&self, task: &WorkflowTask) -> impl Future<Output = Result<(), WorkerError>> + Send;
}
