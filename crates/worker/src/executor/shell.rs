use miranda_core::{spec::dto::TaskConfigSpec, workflow::WorkflowTask};
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};

use crate::{TaskExecutor, WorkerError};

pub struct ShellExecutor;

impl TaskExecutor for ShellExecutor {
    async fn execute(
        &self,
        task: &WorkflowTask,
        timeout: Option<Duration>,
    ) -> Result<(), WorkerError> {
        let config: TaskConfigSpec =
            serde_json::from_value(task.config().clone()).map_err(|e| {
                WorkerError::ExecutionFailed {
                    message: format!("invalid task config: {e}"),
                }
            })?;

        let TaskConfigSpec::Shell {
            command,
            env,
            cwd,
            shell,
            success_codes,
        } = config
        else {
            return Err(WorkerError::UnsupportedTaskType {
                task_type: task.task_type().to_owned(),
            });
        };

        let shell_bin = shell.unwrap_or_else(|| "/bin/sh".to_owned());

        let mut cmd = Command::new(&shell_bin);
        cmd.arg("-c").arg(&command);
        cmd.envs(env);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        if let Some(cwd) = &cwd {
            cmd.current_dir(cwd);
        }

        let mut child = cmd.spawn().map_err(|e| WorkerError::ExecutionFailed {
            message: format!("failed to spawn shell: {e}"),
        })?;

        let mut stdout_pipe = child.stdout.take();
        let mut stderr_pipe = child.stderr.take();

        let status = match timeout {
            Some(duration) => match tokio::time::timeout(duration, child.wait()).await {
                Ok(result) => result.map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("failed to wait for shell: {e}"),
                })?,

                Err(_elapsed) => {
                    let _ = child.start_kill();

                    return Err(WorkerError::Timeout {
                        duration: duration.as_millis() as u64,
                    });
                }
            },

            None => child
                .wait()
                .await
                .map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("failed to wait for shell: {e}"),
                })?,
        };

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        if let Some(pipe) = stdout_pipe.as_mut() {
            let _ = pipe.read_to_end(&mut stdout).await;
        }

        if let Some(pipe) = stderr_pipe.as_mut() {
            let _ = pipe.read_to_end(&mut stderr).await;
        }

        let exit_code = status.code().unwrap_or(-1);

        if !stdout.is_empty() {
            tracing::debug!(stdout = %String::from_utf8_lossy(&stdout), "shell task stdout");
        }

        if !stderr.is_empty() {
            tracing::debug!(stderr = %String::from_utf8_lossy(&stderr), "shell task stderr");
        }

        let succeeded = exit_code >= 0
            && success_codes
                .iter()
                .any(|s_c| s_c.matches(exit_code as u16));

        if succeeded {
            Ok(())
        } else {
            Err(WorkerError::ExecutionFailed {
                message: format!("command exited with code {exit_code}"),
            })
        }
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::WorkflowTaskId,
        spec::dto::{StatusMatcher, TaskConfigSpec},
        workflow::WorkflowTask,
    };
    use serde_json::json;
    use std::time::Duration;

    use super::*;

    fn task_with_config(config: TaskConfigSpec) -> WorkflowTask {
        WorkflowTask::new(WorkflowTaskId::new(), "shell".to_owned(), vec![])
            .unwrap()
            .with_config(serde_json::to_value(config).unwrap())
    }

    fn shell_config(command: String) -> TaskConfigSpec {
        TaskConfigSpec::Shell {
            command,
            env: Default::default(),
            cwd: None,
            shell: None,
            success_codes: vec![StatusMatcher::Exact(0)],
        }
    }

    #[tokio::test]
    async fn execute_succeeds_when_command_exits_zero() {
        let task = task_with_config(shell_config("exit 0".to_owned()));
        let result = ShellExecutor.execute(&task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_fails_when_exit_code_is_not_a_success_code() {
        let task = task_with_config(shell_config("exit 1".to_owned()));
        let error = ShellExecutor.execute(&task, None).await.unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains('1'));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_matches_a_range_success_code() {
        let mut config = shell_config("exit 3".to_owned());
        if let TaskConfigSpec::Shell { success_codes, .. } = &mut config {
            *success_codes = vec![StatusMatcher::Range(0, 5)];
        }

        let task = task_with_config(config);
        let result = ShellExecutor.execute(&task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_passes_environment_variables_to_the_command() {
        let mut config = shell_config("[ \"$MIRANDA_TEST_VAR\" = \"hello\" ] || exit 1".to_owned());
        if let TaskConfigSpec::Shell { env, .. } = &mut config {
            env.insert("MIRANDA_TEST_VAR".to_owned(), "hello".to_owned());
        }

        let task = task_with_config(config);
        let result = ShellExecutor.execute(&task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_runs_the_command_in_the_configured_cwd() {
        let dir = std::env::temp_dir();
        let mut config = shell_config("[ \"$(pwd -P)\" = \"$EXPECTED_DIR\" ]".to_owned());
        if let TaskConfigSpec::Shell { cwd, env, .. } = &mut config {
            *cwd = Some(dir.to_string_lossy().into_owned());
            env.insert(
                "EXPECTED_DIR".to_owned(),
                std::fs::canonicalize(&dir)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            );
        }

        let task = task_with_config(config);
        let result = ShellExecutor.execute(&task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_uses_the_configured_shell_binary() {
        let mut config = shell_config("exit 0".to_owned());
        if let TaskConfigSpec::Shell { shell, .. } = &mut config {
            *shell = Some("/bin/sh".to_owned());
        }

        let task = task_with_config(config);
        let result = ShellExecutor.execute(&task, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_fails_when_the_shell_binary_does_not_exist() {
        let mut config = shell_config("exit 0".to_owned());
        if let TaskConfigSpec::Shell { shell, .. } = &mut config {
            *shell = Some("/no/such/shell-binary".to_owned());
        }

        let task = task_with_config(config);
        let error = ShellExecutor.execute(&task, None).await.unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("failed to spawn shell"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_times_out_when_the_command_runs_longer_than_the_deadline() {
        let task = task_with_config(shell_config("sleep 5".to_owned()));

        let error = ShellExecutor
            .execute(&task, Some(Duration::from_millis(20)))
            .await
            .unwrap_err();

        assert_eq!(error, WorkerError::Timeout { duration: 20 });
    }

    #[tokio::test]
    async fn execute_succeeds_within_a_generous_timeout() {
        let task = task_with_config(shell_config("exit 0".to_owned()));

        let result = ShellExecutor
            .execute(&task, Some(Duration::from_secs(5)))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_rejects_a_task_config_that_is_not_shell() {
        let config = TaskConfigSpec::Wait {
            duration: Some(1),
            until: None,
        };
        let task = task_with_config(config);

        let error = ShellExecutor.execute(&task, None).await.unwrap_err();

        assert_eq!(
            error,
            WorkerError::UnsupportedTaskType {
                task_type: "shell".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn execute_rejects_an_invalid_task_config() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "shell".to_owned(), vec![])
            .unwrap()
            .with_config(json!({ "not": "a valid shell config" }));

        let error = ShellExecutor.execute(&task, None).await.unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("invalid task config"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }
}
