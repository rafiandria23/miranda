use miranda_control_plane::ControlPlaneError;
use miranda_core::id::WorkerId;
use miranda_worker::{ControlPlaneClient, WorkerError, assignment::TaskAssignment};
use std::{collections::HashSet, sync::Arc};

use crate::bootstrap::ServerControlPlane;

pub struct LocalControlPlaneClient {
    control_plane: Arc<ServerControlPlane>,
}

impl LocalControlPlaneClient {
    pub fn new(control_plane: Arc<ServerControlPlane>) -> Self {
        Self { control_plane }
    }
}

impl ControlPlaneClient for LocalControlPlaneClient {
    async fn register(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
    ) -> Result<(), WorkerError> {
        self.control_plane
            .register_worker(worker_id, capabilities.iter().cloned().collect())
            .await
            .map_err(to_worker_error)
    }

    async fn deregister(&self, worker_id: WorkerId) -> Result<(), WorkerError> {
        self.control_plane
            .deregister_worker(worker_id)
            .await
            .map_err(to_worker_error)
    }

    async fn heartbeat(
        &self,
        worker_id: WorkerId,
        active_leases: &[String],
    ) -> Result<(), WorkerError> {
        self.control_plane
            .heartbeat(worker_id, active_leases)
            .await
            .map_err(to_worker_error)
    }

    async fn poll_task(
        &self,
        worker_id: WorkerId,
        _capabilities: &HashSet<String>,
    ) -> Result<Option<TaskAssignment>, WorkerError> {
        self.control_plane
            .poll_task(worker_id)
            .await
            .map_err(to_worker_error)
    }

    async fn report_result(
        &self,
        worker_id: WorkerId,
        lease_token: String,
        result: Result<(), WorkerError>,
    ) -> Result<(), WorkerError> {
        self.control_plane
            .report_result(worker_id, lease_token, result)
            .await
            .map_err(to_worker_error)
    }
}

fn to_worker_error(err: ControlPlaneError) -> WorkerError {
    WorkerError::ExecutionFailed {
        message: err.to_string(),
    }
}
