use miranda_core::{
    execution::{Execution, ExecutionStatus},
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use miranda_storage::{StorageError, WorkflowStore};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::{future::Future, pin::Pin};

use crate::MySqlConfig;

#[derive(Debug, Clone)]
pub struct MySqlStore {
    pool: MySqlPool,
}

impl MySqlStore {
    pub async fn connect(config: MySqlConfig) -> Result<Self, sqlx::Error> {
        ensure_database_exists(&config).await?;

        let pool = MySqlPoolOptions::new()
            .min_connections(config.min_connections)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .idle_timeout(config.idle_timeout)
            .max_lifetime(config.max_lifetime)
            .connect(&config.url)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

async fn ensure_database_exists(config: &MySqlConfig) -> Result<(), sqlx::Error> {
    let admin_url = config
        .admin_url()
        .map_err(|e| sqlx::Error::Configuration(e.into()))?;
    let database_name = config
        .database_name()
        .map_err(|e| sqlx::Error::Configuration(e.into()))?;

    let admin_pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await?;

    let exists: Option<(String,)> =
        sqlx::query_as("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?")
            .bind(&database_name)
            .fetch_optional(&admin_pool)
            .await?;

    if exists.is_none() {
        if database_name.contains('`') {
            return Err(sqlx::Error::Configuration(
                format!("invalid database name: {database_name}").into(),
            ));
        }

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE DATABASE `{database_name}`"
        )))
        .execute(&admin_pool)
        .await?;
    }

    admin_pool.close().await;

    Ok(())
}

impl WorkflowStore for MySqlStore {
    fn save_definition<'a>(
        &'a self,
        workflow_id: WorkflowId,
        name: &'a str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &'a WorkflowDefinition,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let definition_json = serde_json::to_string(definition)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let workflow_id_str = workflow_id.to_string();
            let version_id_str = version_id.to_string();
            let version = version as i64;

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT IGNORE INTO workflows (id, name)
                VALUES (?, ?)
                "#,
                workflow_id_str,
                name,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES (?, ?, ?, ?)
                "#,
                version_id_str,
                workflow_id_str,
                version,
                definition_json,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn get_versions<'a>(
        &'a self,
        workflow_id: WorkflowId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkflowVersionId>, StorageError>> + Send + 'a>>
    {
        Box::pin(async move {
            let workflow_id_str = workflow_id.to_string();

            let rows = sqlx::query!(
                r#"
                SELECT id FROM workflow_versions
                WHERE workflow_id = ?
                ORDER BY version ASC
                "#,
                workflow_id_str,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            rows.into_iter()
                .map(|row| {
                    row.id
                        .parse::<WorkflowVersionId>()
                        .map_err(|e| StorageError::Serialization(e.to_string()))
                })
                .collect()
        })
    }

    fn get_definition<'a>(
        &'a self,
        version_id: WorkflowVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<WorkflowDefinition, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let version_id_str = version_id.to_string();

            let row = sqlx::query!(
                r#"
                SELECT definition FROM workflow_versions
                WHERE id = ?
                "#,
                version_id_str,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let definition_json = row
                .map(|r| r.definition)
                .ok_or(StorageError::WorkflowVersionNotFound(version_id))?;

            serde_json::from_value(definition_json)
                .map_err(|e| StorageError::Serialization(e.to_string()))
        })
    }

    fn save_execution<'a>(
        &'a self,
        execution: &'a Execution,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_value = serde_json::to_value(execution)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let id_str = execution.id().to_string();
            let version_id_str = execution.workflow_version_id().to_string();
            let status = execution.status().as_str();

            sqlx::query!(
                r#"
                INSERT INTO workflow_executions (id, workflow_version_id, status, state)
                VALUES (?, ?, ?, ?)
                "#,
                id_str,
                version_id_str,
                status,
                execution_value,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn update_execution<'a>(
        &'a self,
        execution: &'a Execution,
        expected_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_value = serde_json::to_value(execution)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let id_str = execution.id().to_string();
            let status = execution.status().as_str();
            let expected_version = expected_version as i64;

            let result = sqlx::query!(
                r#"
                UPDATE workflow_executions
                SET state = ?, status = ?, version = version + 1
                WHERE id = ? AND version = ?
                "#,
                execution_value,
                status,
                id_str,
                expected_version,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            if result.rows_affected() == 0 {
                let current = sqlx::query!(
                    r#"SELECT version FROM workflow_executions WHERE id = ?"#,
                    id_str,
                )
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

                return match current {
                    Some(row) => Err(StorageError::OptimisticLockFailed {
                        id: execution.id(),
                        expected: expected_version as u64,
                        actual: row.version as u64,
                    }),
                    None => Err(StorageError::ExecutionNotFound(execution.id())),
                };
            }

            Ok(())
        })
    }

    fn get_execution<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(Execution, u64), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = execution_id.to_string();

            let row = sqlx::query!(
                r#"SELECT state, version FROM workflow_executions WHERE id = ?"#,
                id_str,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let row = row.ok_or(StorageError::ExecutionNotFound(execution_id))?;

            let execution: Execution = serde_json::from_value(row.state)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            Ok((execution, row.version as u64))
        })
    }

    fn get_active_executions<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Execution>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let pending = ExecutionStatus::Pending.as_str();
            let running = ExecutionStatus::Running.as_str();

            let rows = sqlx::query!(
                r#"SELECT state FROM workflow_executions WHERE status IN (?, ?)"#,
                pending,
                running,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            rows.into_iter()
                .map(|row| {
                    serde_json::from_value(row.state)
                        .map_err(|e| StorageError::Serialization(e.to_string()))
                })
                .collect()
        })
    }
}
