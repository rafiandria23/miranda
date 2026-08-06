use miranda_core::{spec::dto::TaskConfigSpec, workflow::WorkflowTask};
use std::time::Duration;
use time::OffsetDateTime;

use crate::{TaskExecutor, WorkerError};

pub struct WaitExecutor;

impl TaskExecutor for WaitExecutor {
    async fn execute(&self, task: &WorkflowTask) -> Result<(), WorkerError> {
        let config: TaskConfigSpec =
            serde_json::from_value(task.config().clone()).map_err(|e| {
                WorkerError::ExecutionFailed {
                    message: format!("invalid task config: {e}"),
                }
            })?;

        let TaskConfigSpec::Wait { duration, until } = config else {
            return Err(WorkerError::UnsupportedTaskType {
                task_type: task.task_type().to_owned(),
            });
        };

        let sleep_duration = match (duration, until) {
            (Some(secs), None) => Duration::from_secs(secs),

            (None, Some(timestamp)) => {
                let target = OffsetDateTime::parse(
                    &timestamp,
                    &time::format_description::well_known::Rfc3339,
                )
                .map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("invalid 'until' timestamp: {e}"),
                })?;

                let now = OffsetDateTime::now_utc();
                let remaining = target - now;

                if remaining.is_negative() {
                    Duration::ZERO
                } else {
                    remaining.unsigned_abs()
                }
            }

            _ => {
                return Err(WorkerError::ExecutionFailed {
                    message: "wait task must specify exactly one of duration or until".to_owned(),
                });
            }
        };

        tokio::time::sleep(sleep_duration).await;

        Ok(())
    }
}
