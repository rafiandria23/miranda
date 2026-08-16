use miranda_core::{
    id::ExecutionId,
    spec::dto::{HttpMethod as SpecHttpMethod, TaskConfigSpec},
};
use reqwest::{Client as HttpClient, Method as HttpMethod};
use std::time::Duration;

use crate::{TaskExecutor, WorkerError};

pub struct HttpExecutor {
    client: HttpClient,
}

impl HttpExecutor {
    pub fn new() -> Self {
        Self {
            client: HttpClient::new(),
        }
    }
}

impl Default for HttpExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskExecutor for HttpExecutor {
    async fn execute(
        &self,
        _execution_id: ExecutionId,
        task: &miranda_core::workflow::WorkflowTask,
        timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        let config: TaskConfigSpec =
            serde_json::from_value(task.config().clone()).map_err(|e| {
                WorkerError::ExecutionFailed {
                    message: format!("invalid task config: {e}"),
                }
            })?;

        let TaskConfigSpec::Http {
            method,
            url,
            query,
            headers,
            body,
            success_codes,
        } = config
        else {
            return Err(WorkerError::UnsupportedTaskType {
                task_type: task.task_type().to_owned(),
            });
        };

        let mut request = self
            .client
            .request(to_reqwest_method(method), &url)
            .query(&query);

        if let Some(duration) = timeout {
            request = request.timeout(duration);
        }

        for (key, value) in &headers {
            request = request.header(key, value);
        }

        if let Some(body) = body {
            request = request.body(body);
        }

        let response = request
            .send()
            .await
            .map_err(|e| WorkerError::ExecutionFailed {
                message: format!("http request failed: {e}"),
            })?;

        let status = response.status().as_u16();

        if success_codes.iter().any(|s_c| s_c.matches(status)) {
            Ok(())
        } else {
            Err(WorkerError::ExecutionFailed {
                message: format!("unexpected status code: {status}"),
            })
        }
    }
}

fn to_reqwest_method(method: SpecHttpMethod) -> HttpMethod {
    match method {
        SpecHttpMethod::Get => HttpMethod::GET,
        SpecHttpMethod::Post => HttpMethod::POST,
        SpecHttpMethod::Put => HttpMethod::PUT,
        SpecHttpMethod::Patch => HttpMethod::PATCH,
        SpecHttpMethod::Delete => HttpMethod::DELETE,
        SpecHttpMethod::Head => HttpMethod::HEAD,
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::WorkflowTaskId,
        spec::dto::{HttpMethod as SpecHttpMethod, StatusMatcher, TaskConfigSpec},
        workflow::WorkflowTask,
    };
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path, query_param},
    };

    use super::*;

    fn task_with_config(config: TaskConfigSpec) -> WorkflowTask {
        WorkflowTask::new(WorkflowTaskId::new(), "http".to_owned(), vec![])
            .unwrap()
            .with_config(serde_json::to_value(config).unwrap())
    }

    fn http_config(url: String) -> TaskConfigSpec {
        TaskConfigSpec::Http {
            method: SpecHttpMethod::Get,
            url,
            query: Default::default(),
            headers: Default::default(),
            body: None,
            success_codes: vec![StatusMatcher::Range(200, 299)],
        }
    }

    #[test]
    fn to_reqwest_method_maps_every_spec_variant() {
        assert_eq!(to_reqwest_method(SpecHttpMethod::Get), HttpMethod::GET);
        assert_eq!(to_reqwest_method(SpecHttpMethod::Post), HttpMethod::POST);
        assert_eq!(to_reqwest_method(SpecHttpMethod::Put), HttpMethod::PUT);
        assert_eq!(to_reqwest_method(SpecHttpMethod::Patch), HttpMethod::PATCH);
        assert_eq!(
            to_reqwest_method(SpecHttpMethod::Delete),
            HttpMethod::DELETE
        );
        assert_eq!(to_reqwest_method(SpecHttpMethod::Head), HttpMethod::HEAD);
    }

    #[tokio::test]
    async fn execute_succeeds_when_response_status_matches_success_codes() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/ok"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let task = task_with_config(http_config(format!("{}/ok", server.uri())));
        let result = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_fails_when_response_status_is_not_a_success_code() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/not-found"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let task = task_with_config(http_config(format!("{}/not-found", server.uri())));
        let error = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await.unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("404"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_matches_an_exact_success_code() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/created"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let mut config = http_config(format!("{}/created", server.uri()));
        if let TaskConfigSpec::Http { success_codes, .. } = &mut config {
            *success_codes = vec![StatusMatcher::Exact(201)];
        }

        let task = task_with_config(config);
        let result = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_sends_the_configured_method() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/submit"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut config = http_config(format!("{}/submit", server.uri()));
        if let TaskConfigSpec::Http { method, .. } = &mut config {
            *method = SpecHttpMethod::Post;
        }

        let task = task_with_config(config);
        let result = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_sends_query_parameters() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/search"))
            .and(query_param("q", "miranda"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut config = http_config(format!("{}/search", server.uri()));
        if let TaskConfigSpec::Http { query, .. } = &mut config {
            query.insert("q".to_owned(), "miranda".to_owned());
        }

        let task = task_with_config(config);
        let result = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_sends_configured_headers() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/authed"))
            .and(header("x-api-key", "secret"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut config = http_config(format!("{}/authed", server.uri()));
        if let TaskConfigSpec::Http { headers, .. } = &mut config {
            headers.insert("x-api-key".to_owned(), "secret".to_owned());
        }

        let task = task_with_config(config);
        let result = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_sends_the_request_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/echo"))
            .and(wiremock::matchers::body_string("hello world"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut config = http_config(format!("{}/echo", server.uri()));
        if let TaskConfigSpec::Http { method, body, .. } = &mut config {
            *method = SpecHttpMethod::Post;
            *body = Some("hello world".to_owned());
        }

        let task = task_with_config(config);
        let result = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_returns_execution_failed_when_the_server_is_unreachable() {
        let task = task_with_config(http_config("http://127.0.0.1:0/unreachable".to_owned()));

        let error = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await.unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("http request failed"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_times_out_when_the_response_is_slower_than_the_deadline() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
            .mount(&server)
            .await;

        let task = task_with_config(http_config(format!("{}/slow", server.uri())));
        let error = HttpExecutor::new()
            .execute(ExecutionId::new(), &task, Some(Duration::from_millis(20)))
            .await
            .unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("http request failed"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_rejects_a_task_config_that_is_not_http() {
        let config = TaskConfigSpec::Wait {
            duration: Some(1),
            until: None,
        };
        let task = task_with_config(config);

        let error = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await.unwrap_err();

        assert_eq!(
            error,
            WorkerError::UnsupportedTaskType {
                task_type: "http".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn execute_rejects_an_invalid_task_config() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "http".to_owned(), vec![])
            .unwrap()
            .with_config(json!({ "not": "a valid http config" }));

        let error = HttpExecutor::new().execute(ExecutionId::new(), &task, None).await.unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("invalid task config"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }
}
