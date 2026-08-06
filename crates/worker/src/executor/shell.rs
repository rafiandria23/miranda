use miranda_core::{spec::dto::TaskConfigSpec, workflow::WorkflowTask};
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};
use tracing::debug;

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
            debug!(stdout = %String::from_utf8_lossy(&stdout), "shell task stdout");
        }

        if !stderr.is_empty() {
            debug!(stderr = %String::from_utf8_lossy(&stderr), "shell task stderr");
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
