use miranda_core::{id::ExecutionId, spec::dto::TaskConfigSpec, workflow::WorkflowTask};
use std::time::Duration;
use time::OffsetDateTime;

use crate::{TaskExecutor, WorkerError};

pub struct WaitExecutor;

impl TaskExecutor for WaitExecutor {
    async fn execute(
        &self,
        _execution_id: ExecutionId,
        task: &WorkflowTask,
        _timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::WorkflowTaskId;
    use serde_json::json;
    use std::time::Instant;

    use super::*;

    fn task_with_config(config: TaskConfigSpec) -> WorkflowTask {
        WorkflowTask::new(WorkflowTaskId::new(), "wait".to_owned(), vec![])
            .unwrap()
            .with_config(serde_json::to_value(config).unwrap())
    }

    #[tokio::test]
    async fn execute_sleeps_for_the_configured_duration() {
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: Some(0),
            until: None,
        });

        let result = WaitExecutor.execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_sleeps_at_least_the_configured_duration() {
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: Some(1),
            until: None,
        });

        let start = Instant::now();
        let result = WaitExecutor.execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
        assert!(start.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn execute_sleeps_until_a_future_rfc3339_timestamp() {
        let target = OffsetDateTime::now_utc() + time::Duration::milliseconds(100);
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: None,
            until: Some(
                target
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
            ),
        });

        let start = Instant::now();
        let result = WaitExecutor.execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn execute_does_not_sleep_when_the_until_timestamp_is_in_the_past() {
        let target = OffsetDateTime::now_utc() - time::Duration::seconds(60);
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: None,
            until: Some(
                target
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
            ),
        });

        let start = Instant::now();
        let result = WaitExecutor.execute(ExecutionId::new(), &task, None).await;

        assert!(result.is_ok());
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn execute_rejects_an_invalid_until_timestamp() {
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: None,
            until: Some("not-a-timestamp".to_owned()),
        });

        let error = WaitExecutor
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("invalid 'until' timestamp"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_rejects_when_neither_duration_nor_until_is_set() {
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: None,
            until: None,
        });

        let error = WaitExecutor
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("exactly one of duration or until"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_rejects_when_both_duration_and_until_are_set() {
        let task = task_with_config(TaskConfigSpec::Wait {
            duration: Some(1),
            until: Some(
                OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
            ),
        });

        let error = WaitExecutor
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("exactly one of duration or until"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_rejects_a_task_config_that_is_not_wait() {
        let task = task_with_config(TaskConfigSpec::Shell {
            command: "exit 0".to_owned(),
            env: Default::default(),
            cwd: None,
            shell: None,
            success_codes: vec![],
            outputs: Vec::new(),
            inputs: Vec::new(),
        });

        let error = WaitExecutor
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

        assert_eq!(
            error,
            WorkerError::UnsupportedTaskType {
                task_type: "wait".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn execute_rejects_an_invalid_task_config() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "wait".to_owned(), vec![])
            .unwrap()
            .with_config(json!({ "not": "a valid wait config" }));

        let error = WaitExecutor
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("invalid task config"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }
}
