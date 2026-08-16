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

use miranda_core::{id::ExecutionId, workflow::WorkflowTask};
use std::{future::Future, time::Duration};

use crate::WorkerError;

pub trait TaskExecutor: Send + Sync {
    fn execute(
        &self,
        execution_id: ExecutionId,
        task: &WorkflowTask,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<(), WorkerError>> + Send;
}
