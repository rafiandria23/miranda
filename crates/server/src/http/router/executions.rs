use axum::{
    Json, Router,
    extract::{Path, State},
    http::header::CONTENT_TYPE,
    response::IntoResponse,
    routing::{get, post},
};
use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkflowTaskId, WorkflowVersionId},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::bootstrap::ServerControlPlane;

use super::super::error::HttpError;

pub fn routes() -> Router<Arc<ServerControlPlane>> {
    Router::new()
        .route("/executions", post(submit_execution))
        .route("/executions/{id}", get(get_execution_status))
        .route(
            "/executions/{id}/tasks/{task_id}/artifacts",
            get(list_task_artifacts),
        )
        .route(
            "/executions/{id}/tasks/{task_id}/artifacts/{*path}",
            get(get_task_artifact),
        )
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
) -> Result<Json<SubmitExecutionResponse>, HttpError> {
    let definition = control_plane
        .get_definition(req.workflow_version_id)
        .await
        .map_err(HttpError::from)?;

    let execution = Execution::from_definition(req.workflow_version_id, &definition)
        .map_err(HttpError::from)?;

    let execution_id = execution.id();

    control_plane
        .submit_execution(execution, definition)
        .await
        .map_err(HttpError::from)?;

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
) -> Result<Json<ExecutionStatusResponse>, HttpError> {
    let (execution, version) = control_plane
        .get_execution(execution_id)
        .await
        .map_err(HttpError::from)?;

    Ok(Json(ExecutionStatusResponse {
        execution_id,
        status: execution.status().as_str().to_owned(),
        version,
    }))
}

async fn list_task_artifacts(
    State(control_plane): State<Arc<ServerControlPlane>>,
    Path((execution_id, task_id)): Path<(ExecutionId, WorkflowTaskId)>,
) -> Result<Json<Vec<String>>, HttpError> {
    let paths = control_plane
        .list_artifacts(execution_id, task_id)
        .await
        .map_err(HttpError::from)?;

    Ok(Json(paths))
}

async fn get_task_artifact(
    State(control_plane): State<Arc<ServerControlPlane>>,
    Path((execution_id, task_id, path)): Path<(ExecutionId, WorkflowTaskId, String)>,
) -> Result<impl IntoResponse, HttpError> {
    let data = control_plane
        .get_artifact(execution_id, task_id, &path)
        .await
        .map_err(HttpError::from)?;

    Ok(([(CONTENT_TYPE, "application/octet-stream")], data))
}
