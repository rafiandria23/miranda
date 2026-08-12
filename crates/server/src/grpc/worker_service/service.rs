use miranda_control_plane::{
    control_plane::ControlPlane, dispatcher::DispatchStrategy, notifier::TaskNotifier,
    queue::TaskQueue, router::Router,
};
use miranda_core::{id::WorkerId, workflow::WorkflowTask};
use miranda_storage::{lease_store::LeaseStore, workflow_store::WorkflowStore};
use miranda_worker::error::WorkerError;
use std::{net::SocketAddr, pin::Pin, sync::Arc};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Server};

use super::notifier::GrpcTaskNotifier;

pub mod proto {
    tonic::include_proto!("miranda.worker.v1");
}

use proto::{
    DeregisterRequest, DeregisterResponse, HeartbeatRequest, HeartbeatResponse, PollTaskRequest,
    PollTaskResponse, RegisterRequest, RegisterResponse, ReportResultRequest, ReportResultResponse,
    SubscribeRequest, TaskAssignment as ProtoTaskAssignment, TaskNotification,
    TaskResult as ProtoTaskResult, WorkflowTask as ProtoWorkflowTask,
    task_result::Outcome,
    worker_service_server::{WorkerService, WorkerServiceServer},
};

use super::super::error::to_status;

pub struct WorkerServiceImpl<Q, R, S, D, N, L> {
    control_plane: Arc<ControlPlane<Q, R, S, D, N, L>>,
    notifier: GrpcTaskNotifier,
}

impl<Q, R, S, D, N, L> WorkerServiceImpl<Q, R, S, D, N, L> {
    pub fn new(
        control_plane: Arc<ControlPlane<Q, R, S, D, N, L>>,
        notifier: GrpcTaskNotifier,
    ) -> Self {
        Self {
            control_plane,
            notifier,
        }
    }
}

#[tonic::async_trait]
impl<Q, R, S, D, N, L> WorkerService for WorkerServiceImpl<Q, R, S, D, N, L>
where
    Q: TaskQueue + 'static,
    R: Router + 'static,
    S: WorkflowStore + 'static,
    D: DispatchStrategy + 'static,
    N: TaskNotifier + 'static,
    L: LeaseStore + 'static,
{
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let worker_id = parse_worker_id(&req.worker_id)?;

        self.control_plane
            .validate_join_token(&req.token)
            .await
            .map_err(to_status)?;

        self.control_plane
            .register_worker(worker_id, req.capabilities)
            .await
            .map_err(to_status)?;

        Ok(Response::new(RegisterResponse {}))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let req = request.into_inner();
        let worker_id = parse_worker_id(&req.worker_id)?;

        self.control_plane
            .deregister_worker(worker_id)
            .await
            .map_err(to_status)?;

        self.notifier.unsubscribe(worker_id).await;

        Ok(Response::new(DeregisterResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let worker_id = parse_worker_id(&req.worker_id)?;

        self.control_plane
            .heartbeat(worker_id, &req.active_leases)
            .await
            .map_err(to_status)?;

        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> Result<Response<PollTaskResponse>, Status> {
        let req = request.into_inner();
        let worker_id = parse_worker_id(&req.worker_id)?;

        let assignment = self
            .control_plane
            .poll_task(worker_id)
            .await
            .map_err(to_status)?;

        let proto_assignment = assignment.map(|a| ProtoTaskAssignment {
            lease_token: a.lease_token,
            task: Some(to_proto_task(&a.task)),
            timeout_ms: a.timeout.map(|d| d.as_millis() as u64),
        });

        Ok(Response::new(PollTaskResponse {
            assignment: proto_assignment,
        }))
    }

    async fn report_result(
        &self,
        request: Request<ReportResultRequest>,
    ) -> Result<Response<ReportResultResponse>, Status> {
        let req = request.into_inner();
        let worker_id = parse_worker_id(&req.worker_id)?;

        let result = from_proto_result(req.result)?;

        self.control_plane
            .report_result(worker_id, req.lease_token, result)
            .await
            .map_err(to_status)?;

        Ok(Response::new(ReportResultResponse {}))
    }

    type SubscribeToTasksStream =
        Pin<Box<dyn Stream<Item = Result<TaskNotification, Status>> + Send + 'static>>;

    async fn subscribe_to_tasks(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeToTasksStream>, Status> {
        let req = request.into_inner();
        let worker_id = parse_worker_id(&req.worker_id)?;

        let rx = self.notifier.subscribe(worker_id).await;
        let stream = ReceiverStream::new(rx).map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}

pub async fn serve<Q, R, S, D, N, L>(
    control_plane: Arc<ControlPlane<Q, R, S, D, N, L>>,
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
{
    let service = WorkerServiceImpl::new(control_plane, notifier);

    Server::builder()
        .add_service(WorkerServiceServer::new(service))
        .serve(addr)
        .await
}

fn parse_worker_id(s: &str) -> Result<WorkerId, Status> {
    s.parse()
        .map_err(|_| Status::invalid_argument(format!("invalid worker_id: {s}")))
}

fn to_proto_task(task: &WorkflowTask) -> ProtoWorkflowTask {
    ProtoWorkflowTask {
        id: task.id().to_string(),
        task_type: task.task_type().to_owned(),
        config_json: task.config().to_string(),
        dependencies: task.dependencies().iter().map(|d| d.to_string()).collect(),
    }
}

fn from_proto_result(result: Option<ProtoTaskResult>) -> Result<Result<(), WorkerError>, Status> {
    let outcome = result
        .and_then(|r| r.outcome)
        .ok_or_else(|| Status::invalid_argument("missing task result outcome"))?;

    Ok(match outcome {
        Outcome::Success(_) => Ok(()),
        Outcome::Failure(failure) => Err(WorkerError::ExecutionFailed {
            message: failure.message,
        }),
    })
}
