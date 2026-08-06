use miranda_core::{spec::dto::TaskConfigSpec, workflow::WorkflowTask};
use std::process::Stdio;
use tokio::process::Command;
use tracing::debug;

use crate::{TaskExecutor, WorkerError};

pub struct ShellExecutor;

impl TaskExecutor for ShellExecutor {
    async fn execute(&self, task: &WorkflowTask) -> Result<(), WorkerError> {
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
        cmd.envs(&env);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        if let Some(cwd) = &cwd {
            cmd.current_dir(cwd);
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| WorkerError::ExecutionFailed {
                message: format!("failed to spawn shell: {e}"),
            })?;

        let exit_code = output.status.code().unwrap_or(-1);

        if !output.stdout.is_empty() {
            debug!(stdout = %String::from_utf8_lossy(&output.stdout), "shell task stdout");
        }

        if !output.stderr.is_empty() {
            debug!(stderr = %String::from_utf8_lossy(&output.stderr), "shell task stderr");
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
