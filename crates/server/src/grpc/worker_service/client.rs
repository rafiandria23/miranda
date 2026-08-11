use miranda_core::{id::WorkerId, workflow::WorkflowTask};
use miranda_worker::{ControlPlaneClient, WorkerError, assignment::TaskAssignment};
use std::{collections::HashSet, pin::Pin, time::Duration};
use tokio_stream::{Stream, StreamExt};
use tonic::{Status, transport::Channel};

use super::service::proto::{
    DeregisterRequest, Failure, HeartbeatRequest, PollTaskRequest, RegisterRequest,
    ReportResultRequest, SubscribeRequest, Success, TaskAssignment as ProtoTaskAssignment,
    TaskResult as ProtoTaskResult, WorkflowTask as ProtoWorkflowTask, task_result::Outcome,
    worker_service_client::WorkerServiceClient,
};

pub struct RemoteControlPlaneClient {
    client: WorkerServiceClient<Channel>,
}

impl RemoteControlPlaneClient {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = WorkerServiceClient::connect(addr).await?;

        Ok(Self { client })
    }
}

impl ControlPlaneClient for RemoteControlPlaneClient {
    async fn register(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
    ) -> Result<(), WorkerError> {
        let mut client = self.client.clone();

        client
            .register(RegisterRequest {
                worker_id: worker_id.to_string(),
                capabilities: capabilities.iter().cloned().collect(),
            })
            .await
            .map_err(to_worker_error)?;

        Ok(())
    }

    async fn deregister(&self, worker_id: WorkerId) -> Result<(), WorkerError> {
        let mut client = self.client.clone();

        client
            .deregister(DeregisterRequest {
                worker_id: worker_id.to_string(),
            })
            .await
            .map_err(to_worker_error)?;

        Ok(())
    }

    async fn heartbeat(
        &self,
        worker_id: WorkerId,
        active_leases: &[String],
    ) -> Result<(), WorkerError> {
        let mut client = self.client.clone();

        client
            .heartbeat(HeartbeatRequest {
                worker_id: worker_id.to_string(),
                active_leases: active_leases.to_vec(),
            })
            .await
            .map_err(to_worker_error)?;

        Ok(())
    }

    async fn poll_task(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
    ) -> Result<Option<TaskAssignment>, WorkerError> {
        let mut client = self.client.clone();

        let response = client
            .poll_task(PollTaskRequest {
                worker_id: worker_id.to_string(),
                capabilities: capabilities.iter().cloned().collect(),
            })
            .await
            .map_err(to_worker_error)?
            .into_inner();

        response.assignment.map(from_proto_assignment).transpose()
    }

    async fn report_result(
        &self,
        worker_id: WorkerId,
        lease_token: String,
        result: Result<(), WorkerError>,
    ) -> Result<(), WorkerError> {
        let mut client = self.client.clone();

        let proto_result = ProtoTaskResult {
            outcome: Some(match result {
                Ok(()) => Outcome::Success(Success {}),
                Err(e) => Outcome::Failure(Failure {
                    message: e.to_string(),
                }),
            }),
        };

        client
            .report_result(ReportResultRequest {
                worker_id: worker_id.to_string(),
                lease_token,
                result: Some(proto_result),
            })
            .await
            .map_err(to_worker_error)?;

        Ok(())
    }

    async fn subscribe(
        &self,
        worker_id: WorkerId,
        capabilities: &HashSet<String>,
    ) -> Result<Pin<Box<dyn Stream<Item = ()> + Send>>, WorkerError> {
        let mut client = self.client.clone();

        let response = client
            .subscribe_to_tasks(SubscribeRequest {
                worker_id: worker_id.to_string(),
                capabilities: capabilities.iter().cloned().collect(),
            })
            .await
            .map_err(to_worker_error)?;

        let stream = response.into_inner().filter_map(|t_n| t_n.ok().map(|_| ()));

        Ok(Box::pin(stream))
    }
}

fn from_proto_assignment(assignment: ProtoTaskAssignment) -> Result<TaskAssignment, WorkerError> {
    let task_proto = assignment
        .task
        .ok_or_else(|| WorkerError::ExecutionFailed {
            message: "poll_task response missing task".to_owned(),
        })?;

    let task = from_proto_task(task_proto)?;

    Ok(TaskAssignment {
        lease_token: assignment.lease_token,
        task,
        timeout: assignment.timeout_ms.map(Duration::from_millis),
    })
}

fn from_proto_task(task: ProtoWorkflowTask) -> Result<WorkflowTask, WorkerError> {
    let id = task.id.parse().map_err(|_| WorkerError::ExecutionFailed {
        message: format!("invalid task id in response: {}", task.id),
    })?;

    let dependencies = task
        .dependencies
        .iter()
        .map(|d| {
            d.parse().map_err(|_| WorkerError::ExecutionFailed {
                message: format!("invalid dependency id in response: {d}"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let config: serde_json::Value =
        serde_json::from_str(&task.config_json).map_err(|e| WorkerError::ExecutionFailed {
            message: format!("invalid config_json in response: {e}"),
        })?;

    let task = WorkflowTask::new(id, task.task_type, dependencies)
        .map_err(|e| WorkerError::ExecutionFailed {
            message: format!("invalid task from server: {e}"),
        })?
        .with_config(config);

    Ok(task)
}

fn to_worker_error(status: Status) -> WorkerError {
    WorkerError::ExecutionFailed {
        message: status.to_string(),
    }
}
