use miranda_core::spec::dto::{HttpMethod as SpecHttpMethod, TaskConfigSpec};
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
