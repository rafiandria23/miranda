use tonic::{Request, Response, Status};

use crate::grpc::worker_service::notifier::GrpcTaskNotifier;

pub mod proto {
    tonic::include_proto!("miranda.control_plane.v1");
}

use proto::{
    NotifyReadyRequest, NotifyReadyResponse, control_plane_service_server::ControlPlaneService,
};

pub struct ControlPlaneServiceImpl {
    notifier: GrpcTaskNotifier,
}

impl ControlPlaneServiceImpl {
    pub fn new(notifier: GrpcTaskNotifier) -> Self {
        Self { notifier }
    }
}

#[tonic::async_trait]
impl ControlPlaneService for ControlPlaneServiceImpl {
    async fn notify_ready(
        &self,
        _request: Request<NotifyReadyRequest>,
    ) -> Result<Response<NotifyReadyResponse>, Status> {
        self.notifier.notify_local().await;

        Ok(Response::new(NotifyReadyResponse {}))
    }
}
