pub mod control_plane_service;
pub mod error;
pub mod worker_service;

use miranda_control_plane::{
    control_plane::ControlPlane, dispatcher::DispatchStrategy, notifier::TaskNotifier,
    queue::TaskQueue, router::Router,
};
use miranda_storage::{
    artifact_store::ArtifactStore, lease_store::LeaseStore, workflow_store::WorkflowStore,
};
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::Server;

use control_plane_service::service::{
    ControlPlaneServiceImpl, proto::control_plane_service_server::ControlPlaneServiceServer,
};
use worker_service::{
    notifier::GrpcTaskNotifier,
    service::{WorkerServiceImpl, proto::worker_service_server::WorkerServiceServer},
};

pub async fn serve_all<Q, R, S, D, N, L, A>(
    control_plane: Arc<ControlPlane<Q, R, S, D, N, L, A>>,
    notifier: GrpcTaskNotifier,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error>
where
    Q: TaskQueue + 'static,
    R: Router + 'static,
    S: WorkflowStore + 'static,
    D: DispatchStrategy + 'static,
    N: TaskNotifier + 'static,
    L: LeaseStore + 'static,
    A: ArtifactStore + 'static,
{
    let worker_service = WorkerServiceImpl::new(control_plane, notifier.clone());
    let control_plane_service = ControlPlaneServiceImpl::new(notifier);

    Server::builder()
        .add_service(WorkerServiceServer::new(worker_service))
        .add_service(ControlPlaneServiceServer::new(control_plane_service))
        .serve(addr)
        .await
}
