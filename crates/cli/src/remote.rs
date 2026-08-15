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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    fn definition() -> WorkflowDefinition {
        WorkflowDefinition::new(vec![]).unwrap()
    }

    #[tokio::test]
    async fn register_returns_the_parsed_response_on_success() {
        let server = MockServer::start().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();

        Mock::given(method("POST"))
            .and(path("/workflows"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "workflow_id": workflow_id,
                "version_id": version_id,
            })))
            .mount(&server)
            .await;

        let response = register(&server.uri(), "my-workflow", &definition())
            .await
            .unwrap();

        assert_eq!(response.workflow_id, workflow_id);
        assert_eq!(response.version_id, version_id);
    }

    #[tokio::test]
    async fn register_sends_the_name_and_definition_with_no_workflow_id() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/workflows"))
            .and(wiremock::matchers::body_json(json!({
                "workflow_id": null,
                "name": "my-workflow",
                "definition": definition(),
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "workflow_id": WorkflowId::new(),
                "version_id": WorkflowVersionId::new(),
            })))
            .mount(&server)
            .await;

        let result = register(&server.uri(), "my-workflow", &definition()).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn register_returns_an_error_on_non_success_status() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/workflows"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;

        let error = register(&server.uri(), "my-workflow", &definition())
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("register failed"));
        assert!(message.contains("400"));
        assert!(message.contains("bad request"));
    }

    #[tokio::test]
    async fn register_returns_an_error_when_the_body_cannot_be_parsed() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/workflows"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let result = register(&server.uri(), "my-workflow", &definition()).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_returns_the_parsed_response_on_success() {
        let server = MockServer::start().await;
        let execution_id = ExecutionId::new();
        let version_id = WorkflowVersionId::new();

        Mock::given(method("POST"))
            .and(path("/executions"))
            .and(wiremock::matchers::body_json(json!({
                "workflow_version_id": version_id,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "execution_id": execution_id,
            })))
            .mount(&server)
            .await;

        let response = submit(&server.uri(), version_id).await.unwrap();

        assert_eq!(response.execution_id, execution_id);
    }

    #[tokio::test]
    async fn submit_returns_an_error_on_non_success_status() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/executions"))
            .respond_with(ResponseTemplate::new(404).set_body_string("workflow version not found"))
            .mount(&server)
            .await;

        let error = submit(&server.uri(), WorkflowVersionId::new())
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("submit failed"));
        assert!(message.contains("404"));
        assert!(message.contains("workflow version not found"));
    }

    #[tokio::test]
    async fn status_returns_the_parsed_response_on_success() {
        let server = MockServer::start().await;
        let execution_id = ExecutionId::new();

        Mock::given(method("GET"))
            .and(path(format!("/executions/{execution_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "execution_id": execution_id,
                "version": 3,
                "status": "running",
            })))
            .mount(&server)
            .await;

        let response = status(&server.uri(), execution_id).await.unwrap();

        assert_eq!(response.execution_id, execution_id);
        assert_eq!(response.version, 3);
        assert_eq!(response.status, "running");
    }

    #[tokio::test]
    async fn status_returns_an_error_on_non_success_status() {
        let server = MockServer::start().await;
        let execution_id = ExecutionId::new();

        Mock::given(method("GET"))
            .and(path(format!("/executions/{execution_id}")))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let error = status(&server.uri(), execution_id).await.unwrap_err();

        let message = error.to_string();
        assert!(message.contains("status check failed"));
        assert!(message.contains("500"));
        assert!(message.contains("internal error"));
    }

    #[tokio::test]
    async fn status_returns_an_error_when_the_server_is_unreachable() {
        let result = status("http://127.0.0.1:0", ExecutionId::new()).await;

        assert!(result.is_err());
    }
}
