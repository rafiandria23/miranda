use miranda_core::{
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Debug, Serialize)]
struct RegisterWorkflowRequest {
    workflow_id: Option<WorkflowId>,
    name: String,
    definition: WorkflowDefinition,
}

#[derive(Debug, Deserialize)]
pub struct RegisterWorkflowResponse {
    pub workflow_id: WorkflowId,
    pub version_id: WorkflowVersionId,
}

pub async fn register(
    server: &str,
    name: &str,
    definition: &WorkflowDefinition,
) -> Result<RegisterWorkflowResponse, Box<dyn Error>> {
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{server}/workflows"))
        .json(&RegisterWorkflowRequest {
            workflow_id: None,
            name: name.to_owned(),
            definition: definition.clone(),
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!(
            "register failed: {} - {}",
            response.status(),
            response.text().await?
        )
        .into());
    }

    Ok(response.json().await?)
}

#[derive(Debug, Serialize)]
struct SubmitExecutionRequest {
    workflow_version_id: WorkflowVersionId,
}

#[derive(Debug, Deserialize)]
pub struct SubmitExecutionResponse {
    pub execution_id: ExecutionId,
}

pub async fn submit(
    server: &str,
    workflow_version_id: WorkflowVersionId,
) -> Result<SubmitExecutionResponse, Box<dyn Error>> {
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{server}/executions"))
        .json(&SubmitExecutionRequest {
            workflow_version_id,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!(
            "submit failed: {} — {}",
            response.status(),
            response.text().await?
        )
        .into());
    }

    Ok(response.json().await?)
}

#[derive(Debug, Deserialize)]
pub struct ExecutionStatusResponse {
    pub execution_id: ExecutionId,
    pub version: u64,
    pub status: String,
}

pub async fn status(
    server: &str,
    execution_id: ExecutionId,
) -> Result<ExecutionStatusResponse, Box<dyn Error>> {
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{server}/executions/{execution_id}"))
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!(
            "status check failed: {} — {}",
            response.status(),
            response.text().await?
        )
        .into());
    }

    Ok(response.json().await?)
}
