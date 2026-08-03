use miranda_core::{execution::Execution, id::WorkerId, workflow::WorkflowDefinition};
use miranda_worker::WorkerError;
use std::future::Future;

use crate::{ControlPlaneError, queue::TaskAssignment};

pub trait ControlPlane: Send + Sync {
    fn submit_execution(
        &self,
        execution: Execution,
        definition: WorkflowDefinition,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;

    fn poll_task(
        &self,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<Option<TaskAssignment>, ControlPlaneError>> + Send;

    fn report_result(
        &self,
        worker_id: WorkerId,
        result: Result<(), WorkerError>,
    ) -> impl Future<Output = Result<(), ControlPlaneError>> + Send;
}
