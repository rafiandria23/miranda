use miranda_core::{
    id::{ExecutionId, WorkflowTaskId},
    spec::dto::TaskConfigSpec,
    workflow::WorkflowTask,
};
use miranda_storage::{artifact_store::ArtifactStore, error::StorageError};
use std::{
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{fs, io::AsyncReadExt, process::Command};

use crate::{TaskExecutor, WorkerError};

pub struct ShellExecutor {
    artifact_store: Arc<dyn ArtifactStore>,
    work_dir_root: PathBuf,
}

impl ShellExecutor {
    pub fn new(artifact_store: Arc<dyn ArtifactStore>, work_dir_root: PathBuf) -> Self {
        Self {
            artifact_store,
            work_dir_root,
        }
    }

    fn task_work_dir(&self, execution_id: ExecutionId, task_id: WorkflowTaskId) -> PathBuf {
        self.work_dir_root
            .join(execution_id.to_string())
            .join(task_id.to_string())
    }

    fn upload_dir<'a>(
        &'a self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        work_dir: &'a Path,
        dir: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send + 'a>> {
        Box::pin(async move {
            let mut entries =
                fs::read_dir(dir)
                    .await
                    .map_err(|e| WorkerError::ExecutionFailed {
                        message: format!("failed to read output dir: {e}"),
                    })?;

            while let Some(entry) =
                entries
                    .next_entry()
                    .await
                    .map_err(|e| WorkerError::ExecutionFailed {
                        message: format!("failed to read output dir entry: {e}"),
                    })?
            {
                let path = entry.path();
                let file_type =
                    entry
                        .file_type()
                        .await
                        .map_err(|e| WorkerError::ExecutionFailed {
                            message: format!("failed to stat output dir entry: {e}"),
                        })?;

                if file_type.is_dir() {
                    self.upload_dir(execution_id, task_id, work_dir, &path)
                        .await?;
                } else {
                    let relative = path
                        .strip_prefix(work_dir)
                        .map_err(|_| WorkerError::ExecutionFailed {
                            message: "output path escaped work_dir".to_owned(),
                        })?
                        .to_string_lossy()
                        .into_owned();

                    let data = fs::read(&path)
                        .await
                        .map_err(|e| WorkerError::ExecutionFailed {
                            message: format!("failed to read output file: {e}"),
                        })?;

                    self.artifact_store
                        .save_artifact(execution_id, task_id, &relative, &data)
                        .await
                        .map_err(|e| WorkerError::ExecutionFailed {
                            message: format!("failed to upload output file: {e}"),
                        })?;
                }
            }

            Ok(())
        })
    }

    async fn upload_output(
        &self,
        execution_id: ExecutionId,
        task_id: WorkflowTaskId,
        work_dir: &Path,
        output_path: &str,
    ) -> Result<(), WorkerError> {
        let full_path = work_dir.join(output_path);

        let metadata = match fs::metadata(&full_path).await {
            Ok(m) => m,
            Err(e) => {
                return Err(WorkerError::ExecutionFailed {
                    message: format!(
                        "declared output '{output_path}' not found after task ran: {e}"
                    ),
                });
            }
        };

        if metadata.is_dir() {
            self.upload_dir(execution_id, task_id, work_dir, &full_path)
                .await
        } else {
            let data = fs::read(&full_path)
                .await
                .map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("failed to read output '{output_path}': {e}"),
                })?;

            self.artifact_store
                .save_artifact(execution_id, task_id, output_path, &data)
                .await
                .map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("failed to upload output '{output_path}': {e}"),
                })
        }
    }

    async fn download_input(
        &self,
        execution_id: ExecutionId,
        from_task_id: WorkflowTaskId,
        input_path: &str,
        work_dir: &Path,
    ) -> Result<(), WorkerError> {
        match self
            .artifact_store
            .load_artifact(execution_id, from_task_id, input_path)
            .await
        {
            Ok(data) => {
                let dest = work_dir.join(input_path);

                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| WorkerError::ExecutionFailed {
                            message: format!("failed to create input dir: {e}"),
                        })?;
                }

                fs::write(&dest, data)
                    .await
                    .map_err(|e| WorkerError::ExecutionFailed {
                        message: format!("failed to write input artifact: {e}"),
                    })?;

                return Ok(());
            }
            Err(StorageError::ArtifactIsDirectory { .. }) => {}
            Err(e) => {
                return Err(WorkerError::ExecutionFailed {
                    message: format!(
                        "failed to fetch input artifact '{input_path}' from task {from_task_id}: {e}"
                    ),
                });
            }
        }

        let all_paths = self
            .artifact_store
            .list_artifacts(execution_id, from_task_id)
            .await
            .map_err(|e| WorkerError::ExecutionFailed {
                message: format!("failed to list artifacts for input '{input_path}': {e}"),
            })?;

        let prefix = if input_path.ends_with('/') {
            input_path.to_owned()
        } else {
            format!("{input_path}/")
        };

        let matching: Vec<&String> = all_paths
            .iter()
            .filter(|p| p.starts_with(&prefix))
            .collect();

        if matching.is_empty() {
            return Err(WorkerError::ExecutionFailed {
                message: format!(
                    "declared input '{input_path}' from task {from_task_id} matched no artifacts"
                ),
            });
        }

        for artifact_path in matching {
            let data = self
                .artifact_store
                .load_artifact(execution_id, from_task_id, artifact_path)
                .await
                .map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("failed to fetch input artifact '{artifact_path}': {e}"),
                })?;

            let dest = work_dir.join(artifact_path);

            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| WorkerError::ExecutionFailed {
                        message: format!("failed to create input dir: {e}"),
                    })?;
            }

            fs::write(&dest, data)
                .await
                .map_err(|e| WorkerError::ExecutionFailed {
                    message: format!("failed to write input artifact: {e}"),
                })?;
        }

        Ok(())
    }
}

impl TaskExecutor for ShellExecutor {
    async fn execute(
        &self,
        execution_id: ExecutionId,
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
            outputs,
            inputs,
        } = config
        else {
            return Err(WorkerError::UnsupportedTaskType {
                task_type: task.task_type().to_owned(),
            });
        };

        let work_dir = self.task_work_dir(execution_id, task.id());

        fs::create_dir_all(&work_dir)
            .await
            .map_err(|e| WorkerError::ExecutionFailed {
                message: format!("failed to create task work dir: {e}"),
            })?;

        for input in &inputs {
            let from_task_id: WorkflowTaskId =
                input
                    .from_task
                    .parse()
                    .map_err(|_| WorkerError::ExecutionFailed {
                        message: format!("invalid from_task id: {}", input.from_task),
                    })?;

            self.download_input(execution_id, from_task_id, &input.path, &work_dir)
                .await?;
        }

        let shell_bin = shell.unwrap_or_else(|| "/bin/sh".to_owned());

        let mut cmd = Command::new(&shell_bin);
        cmd.arg("-c").arg(&command);
        cmd.envs(env);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        let effective_cwd = match &cwd {
            Some(sub) => work_dir.join(sub),
            None => work_dir.clone(),
        };

        cmd.current_dir(&effective_cwd);

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

        let result = if succeeded {
            Ok(())
        } else {
            Err(WorkerError::ExecutionFailed {
                message: format!("command exited with code {exit_code}"),
            })
        };

        if result.is_ok() {
            for output_path in &outputs {
                self.upload_output(execution_id, task.id(), &work_dir, output_path)
                    .await?;
            }

            let _ = fs::remove_dir_all(&work_dir).await;
        } else {
            tracing::warn!(work_dir = %work_dir.display(), "task failed, leaving work_dir for debugging");
        }

        result
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::{ExecutionId, WorkflowTaskId},
        spec::dto::{StatusMatcher, TaskConfigSpec},
        workflow::WorkflowTask,
    };
    use miranda_storage::filesystem::FilesystemStore;
    use serde_json::json;
    use std::time::Duration;

    use super::*;

    fn unique_temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("miranda-shell-test-{label}-{}", ExecutionId::new()))
    }

    fn test_executor() -> ShellExecutor {
        ShellExecutor::new(
            Arc::new(FilesystemStore::new(unique_temp_dir("store"))),
            unique_temp_dir("work"),
        )
    }

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
            outputs: Vec::new(),
            inputs: Vec::new(),
        }
    }

    #[tokio::test]
    async fn execute_succeeds_when_command_exits_zero() {
        let task = task_with_config(shell_config("exit 0".to_owned()));
        let result = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_fails_when_exit_code_is_not_a_success_code() {
        let task = task_with_config(shell_config("exit 1".to_owned()));
        let error = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

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
        let result = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_passes_environment_variables_to_the_command() {
        let mut config = shell_config("[ \"$MIRANDA_TEST_VAR\" = \"hello\" ] || exit 1".to_owned());
        if let TaskConfigSpec::Shell { env, .. } = &mut config {
            env.insert("MIRANDA_TEST_VAR".to_owned(), "hello".to_owned());
        }

        let task = task_with_config(config);
        let result = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await;

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
        let result = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_uses_the_configured_shell_binary() {
        let mut config = shell_config("exit 0".to_owned());
        if let TaskConfigSpec::Shell { shell, .. } = &mut config {
            *shell = Some("/bin/sh".to_owned());
        }

        let task = task_with_config(config);
        let result = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_fails_when_the_shell_binary_does_not_exist() {
        let mut config = shell_config("exit 0".to_owned());
        if let TaskConfigSpec::Shell { shell, .. } = &mut config {
            *shell = Some("/no/such/shell-binary".to_owned());
        }

        let task = task_with_config(config);
        let error = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

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

        let error = test_executor()
            .execute(ExecutionId::new(), &task, Some(Duration::from_millis(20)))
            .await
            .unwrap_err();

        assert_eq!(error, WorkerError::Timeout { duration: 20 });
    }

    #[tokio::test]
    async fn execute_succeeds_within_a_generous_timeout() {
        let task = task_with_config(shell_config("exit 0".to_owned()));

        let result = test_executor()
            .execute(ExecutionId::new(), &task, Some(Duration::from_secs(5)))
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

        let error = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

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

        let error = test_executor()
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

    #[tokio::test]
    async fn execute_uploads_declared_outputs_as_artifacts() {
        let store_dir = unique_temp_dir("store");
        let work_dir_root = unique_temp_dir("work");
        let artifact_store = Arc::new(FilesystemStore::new(store_dir));
        let executor = ShellExecutor::new(artifact_store.clone(), work_dir_root);

        let mut config = shell_config("echo hello > out.txt".to_owned());
        if let TaskConfigSpec::Shell { outputs, .. } = &mut config {
            *outputs = vec!["out.txt".to_owned()];
        }

        let task = task_with_config(config);
        let execution_id = ExecutionId::new();

        let result = executor.execute(execution_id, &task, None).await;
        assert!(result.is_ok());

        let data = artifact_store
            .load_artifact(execution_id, task.id(), "out.txt")
            .await
            .unwrap();

        assert_eq!(String::from_utf8(data).unwrap().trim(), "hello");
    }

    #[tokio::test]
    async fn execute_fails_when_a_declared_output_is_missing() {
        let task = task_with_config({
            let mut config = shell_config("exit 0".to_owned());
            if let TaskConfigSpec::Shell { outputs, .. } = &mut config {
                *outputs = vec!["missing.txt".to_owned()];
            }
            config
        });

        let error = test_executor()
            .execute(ExecutionId::new(), &task, None)
            .await
            .unwrap_err();

        match error {
            WorkerError::ExecutionFailed { message } => {
                assert!(message.contains("missing.txt"));
            }
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_downloads_input_artifacts_before_running() {
        let store_dir = unique_temp_dir("store");
        let work_dir_root = unique_temp_dir("work");
        let artifact_store = Arc::new(FilesystemStore::new(store_dir));
        let executor = ShellExecutor::new(artifact_store.clone(), work_dir_root);

        let execution_id = ExecutionId::new();
        let from_task_id = WorkflowTaskId::new();

        artifact_store
            .save_artifact(execution_id, from_task_id, "in.txt", b"hello-input")
            .await
            .unwrap();

        let mut config =
            shell_config("[ \"$(cat in.txt)\" = \"hello-input\" ] || exit 1".to_owned());
        if let TaskConfigSpec::Shell { inputs, .. } = &mut config {
            *inputs = vec![miranda_core::spec::dto::ArtifactInput {
                from_task: from_task_id.to_string(),
                path: "in.txt".to_owned(),
            }];
        }

        let task = task_with_config(config);
        let result = executor.execute(execution_id, &task, None).await;

        assert!(result.is_ok());
    }
}
