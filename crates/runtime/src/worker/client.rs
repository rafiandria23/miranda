use miranda_core::{definition::WorkflowTask, id::WorkerId};
use std::{future::Future, process::Output};

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
