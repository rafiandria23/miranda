use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use miranda_control_plane::ControlPlaneError;
use miranda_core::{
    ExecutionError,
    execution::Execution,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::bootstrap::ServerControlPlane;

struct ApiError(StatusCode, String);

impl From<ControlPlaneError> for ApiError {
    fn from(err: ControlPlaneError) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    }
}

impl From<ExecutionError> for ApiError {
    fn from(err: ExecutionError) -> Self {
        ApiError(StatusCode::BAD_REQUEST, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

pub fn router(control_plane: Arc<ServerControlPlane>) -> Router {
    Router::new()
        .route("/workflows", post(register_workflow))
        .route("/executions", post(submit_execution))
        .route("/executions/{id}", get(get_execution_status))
        .with_state(control_plane)
}

#[derive(Debug, Deserialize)]
struct RegisterWorkflowRequest {
    workflow_id: Option<WorkflowId>,
    name: String,
    definition: WorkflowDefinition,
}

#[derive(Debug, Serialize)]
struct RegisterWorkflowResponse {
    workflow_id: WorkflowId,
    version_id: WorkflowVersionId,
}

async fn register_workflow(
    State(control_plane): State<Arc<ServerControlPlane>>,
    Json(req): Json<RegisterWorkflowRequest>,
) -> Result<Json<RegisterWorkflowResponse>, ApiError> {
    let workflow_id = req.workflow_id.unwrap_or_else(WorkflowId::new);

    let version_id = control_plane
        .register_workflow(workflow_id, &req.name, &req.definition)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(RegisterWorkflowResponse {
        workflow_id,
        version_id,
    }))
}

#[derive(Debug, Deserialize)]
struct SubmitExecutionRequest {
    workflow_version_id: WorkflowVersionId,
}

#[derive(Debug, Serialize)]
struct SubmitExecutionResponse {
    execution_id: ExecutionId,
}

async fn submit_execution(
    State(control_plane): State<Arc<ServerControlPlane>>,
    Json(req): Json<SubmitExecutionRequest>,
) -> Result<Json<SubmitExecutionResponse>, ApiError> {
    let definition = control_plane
        .get_definition(req.workflow_version_id)
        .await
        .map_err(ApiError::from)?;

    let execution =
        Execution::from_definition(req.workflow_version_id, &definition).map_err(ApiError::from)?;

    let execution_id = execution.id();

    control_plane
        .submit_execution(execution, definition)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(SubmitExecutionResponse { execution_id }))
}

#[derive(Debug, Serialize)]
struct ExecutionStatusResponse {
    execution_id: ExecutionId,
    status: String,
    version: u64,
}

async fn get_execution_status(
    State(control_plane): State<Arc<ServerControlPlane>>,
    Path(execution_id): Path<ExecutionId>,
) -> Result<Json<ExecutionStatusResponse>, ApiError> {
    let (execution, version) = control_plane
        .get_execution(execution_id)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(ExecutionStatusResponse {
        execution_id,
        status: execution.status().as_str().to_owned(),
        version,
    }))
}
