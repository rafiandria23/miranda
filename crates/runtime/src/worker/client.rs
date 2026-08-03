use miranda_core::{id::WorkerId, workflow::WorkflowTask};
use std::future::Future;

use crate::RuntimeError;

use super::TaskLease;

pub trait WorkerClient {
    fn dispatch(
        &self,
        worker_id: WorkerId,
        task: &WorkflowTask,
    ) -> impl Future<Output = Result<TaskLease, RuntimeError>> + Send;

    fn renew_lease(
        &self,
        lease: &mut TaskLease,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn cancel_task(
        &self,
        lease: &TaskLease,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;
}
