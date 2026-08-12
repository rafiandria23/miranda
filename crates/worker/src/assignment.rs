use miranda_core::{id::WorkerId, workflow::WorkflowTask};
use std::{collections::HashSet, future::Future, pin::Pin, time::Duration};
use tokio_stream::Stream;

use crate::WorkerError;

pub struct TaskAssignment {
    pub lease_token: String,
    pub task: WorkflowTask,
    pub timeout: Option<Duration>,
}

pub trait ControlPlaneClient: Send + Sync + 'static {
    fn register(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
        token: &str,
    ) -> impl Future<Output = Result<(), WorkerError>> + Send;

    fn deregister(
        &self,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<(), WorkerError>> + Send;

    fn heartbeat(
        &self,
        worker_id: WorkerId,
        active_leases: &[String],
    ) -> impl Future<Output = Result<(), WorkerError>> + Send;

    fn poll_task(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
    ) -> impl Future<Output = Result<Option<TaskAssignment>, WorkerError>> + Send;

    fn report_result(
        &self,
        worker_id: WorkerId,
        lease_token: String,
        result: Result<(), WorkerError>,
    ) -> impl Future<Output = Result<(), WorkerError>> + Send;

    fn subscribe(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
    ) -> impl Future<Output = Result<Pin<Box<dyn Stream<Item = ()> + Send>>, WorkerError>> + Send;
}
