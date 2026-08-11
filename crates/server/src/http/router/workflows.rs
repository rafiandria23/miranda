use axum::{Json, Router, extract::State, routing::post};
use miranda_core::{
    id::{WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::bootstrap::ServerControlPlane;

use super::super::error::HttpError;

pub fn routes() -> Router<Arc<ServerControlPlane>> {
    Router::new().route("/workflows", post(register_workflow))
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
) -> Result<Json<RegisterWorkflowResponse>, HttpError> {
    let workflow_id = req.workflow_id.unwrap_or_else(WorkflowId::new);

    let version_id = control_plane
        .register_workflow(workflow_id, &req.name, &req.definition)
        .await
        .map_err(HttpError::from)?;

    Ok(Json(RegisterWorkflowResponse {
        workflow_id,
        version_id,
    }))
}
