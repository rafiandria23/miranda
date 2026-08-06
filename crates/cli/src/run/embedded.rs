use miranda_core::{execution::Execution, id::WorkflowVersionId, spec, workflow::WorkflowTask};
use miranda_engine::EmbeddedEngine;
// use miranda_storage::MemoryStore;
// use miranda_storage_mysql::{MySqlConfig, MySqlStore};
// use miranda_storage_postgres::{PostgresConfig, PostgresStore};
use miranda_storage_sqlite::{SqliteConfig, SqliteStore};
use miranda_worker::{InProcessExecutor, TaskExecutor, WorkerError};
use std::error::Error;

use crate::config_dir;

pub async fn run_from_yaml(yaml: &str) -> Result<(), Box<dyn Error>> {
    let (_workflow, definition) = spec::compile(yaml)?;

    let execution = Execution::from_definition(WorkflowVersionId::new(), &definition)?;

    let config_dir = config_dir::resolve()?;
    let db_path = config_dir.join("miranda.db");

    let store = SqliteStore::connect(SqliteConfig {
        path: db_path.to_string_lossy().into_owned(),
        ..SqliteConfig::default()
    })
    .await?;

    let executor = default_executor();
    let engine = EmbeddedEngine::new(executor, store);

    let result = engine.run(execution, &definition).await?;

    println!("execution finished: {:?}", result.status());

    Ok(())
}

fn default_executor() -> impl TaskExecutor {
    InProcessExecutor::new(|task: &WorkflowTask| {
        let task_type = task.task_type().to_owned();

        async move {
            println!("running task: {task_type}");

            Ok::<(), WorkerError>(())
        }
    })
}
