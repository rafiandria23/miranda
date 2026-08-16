use miranda_core::{execution::Execution, id::WorkflowVersionId, spec};
use miranda_engine::EmbeddedEngine;
// use miranda_storage::InMemoryStore;
// use miranda_storage_mysql::{MySqlConfig, MySqlStore};
// use miranda_storage_postgres::{PostgresConfig, PostgresStore};
use miranda_storage::{WorkflowStore, artifact_store::ArtifactStore, filesystem::FilesystemStore};
use miranda_storage_sqlite::{SqliteConfig, SqliteStore};
use miranda_worker::DispatchExecutor;
use std::{error::Error, path::Path, sync::Arc};

use crate::config_dir;

pub async fn run_from_yaml(yaml: &str) -> Result<(), Box<dyn Error>> {
    let config_dir = config_dir::resolve()?;
    let db_path = config_dir.join("miranda.sqlite");
    let artifact_dir = config_dir.join("artifacts");
    let work_dir_root = config_dir.join("work");

    run_from_yaml_with_db_path(
        yaml,
        &db_path.to_string_lossy(),
        &artifact_dir,
        &work_dir_root,
    )
    .await
}

async fn run_from_yaml_with_db_path(
    yaml: &str,
    db_path: &str,
    artifact_dir: &Path,
    work_dir_root: &Path,
) -> Result<(), Box<dyn Error>> {
    let (workflow, definition) = spec::compile(yaml)?;

    let version_id = WorkflowVersionId::new();
    let execution = Execution::from_definition(version_id, &definition)?;

    let store = SqliteStore::connect(SqliteConfig::new(db_path)).await?;

    store
        .save_definition(workflow.id(), workflow.name(), version_id, 1, &definition)
        .await?;

    let artifact_store: Arc<dyn ArtifactStore> = Arc::new(FilesystemStore::new(artifact_dir));

    let engine = EmbeddedEngine::new(
        DispatchExecutor::new(artifact_store, work_dir_root.to_path_buf()),
        store,
    );

    let result = engine.run(execution, &definition).await?;

    println!("execution finished: {:?}", result.status());

    Ok(())
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::ExecutionId;

    use super::*;

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "miranda-embedded-test-{label}-{}",
            ExecutionId::new()
        ))
    }

    #[tokio::test]
    async fn run_from_yaml_persists_a_noop_workflow() {
        let yaml = r#"
name: noop-workflow
tasks:
  first:
    type: noop
"#;

        let result = run_from_yaml_with_db_path(
            yaml,
            ":memory:",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn run_from_yaml_persists_a_shell_workflow() {
        let yaml = r#"
name: shell-workflow
tasks:
  first:
    type: shell
    command: "true"
"#;

        let result = run_from_yaml_with_db_path(
            yaml,
            ":memory:",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_from_yaml_persists_dependent_tasks() {
        let yaml = r#"
name: dependent-workflow
tasks:
  first:
    type: noop
  second:
    type: noop
    depends_on: [first]
"#;

        let result = run_from_yaml_with_db_path(
            yaml,
            ":memory:",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_from_yaml_returns_an_error_when_a_shell_task_exits_nonzero() {
        let yaml = r#"
name: failing-shell-workflow
tasks:
  first:
    type: shell
    command: "false"
"#;

        let result = run_from_yaml_with_db_path(
            yaml,
            ":memory:",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_from_yaml_returns_an_error_for_invalid_yaml() {
        let yaml = "not: [valid, workflow";

        let result = run_from_yaml_with_db_path(
            yaml,
            ":memory:",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_from_yaml_returns_an_error_for_an_unknown_task_type() {
        let yaml = r#"
name: unknown-task-workflow
tasks:
  first:
    type: carrier_pigeon
"#;

        let result = run_from_yaml_with_db_path(
            yaml,
            ":memory:",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_from_yaml_returns_an_error_for_an_invalid_db_path() {
        let yaml = r#"
name: noop-workflow
tasks:
  first:
    type: noop
"#;

        let result = run_from_yaml_with_db_path(
            yaml,
            "/nonexistent/dir/miranda.sqlite",
            &unique_temp_dir("artifacts"),
            &unique_temp_dir("work"),
        )
        .await;

        assert!(result.is_err());
    }
}
