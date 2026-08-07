use miranda_core::{
    execution::{Execution, ExecutionStatus},
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use miranda_storage::{StorageError, WorkflowStore};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::{future::Future, pin::Pin};

use crate::PostgresConfig;

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(config: PostgresConfig) -> Result<Self, sqlx::Error> {
        ensure_database_exists(&config).await?;

        let pool = PgPoolOptions::new()
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

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

async fn ensure_database_exists(config: &PostgresConfig) -> Result<(), sqlx::Error> {
    let admin_url = config
        .admin_url()
        .map_err(|e| sqlx::Error::Configuration(e.into()))?;
    let database_name = config
        .database_name()
        .map_err(|e| sqlx::Error::Configuration(e.into()))?;

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await?;

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&database_name)
            .fetch_one(&admin_pool)
            .await?;

    if !exists {
        if database_name.contains('"') {
            return Err(sqlx::Error::Configuration(
                format!("invalid database name: {database_name}").into(),
            ));
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"CREATE DATABASE "{database_name}""#
        )))
        .execute(&admin_pool)
        .await?;
    }

    admin_pool.close().await;

    Ok(())
}

impl WorkflowStore for PostgresStore {
    fn save_definition<'a>(
        &'a self,
        workflow_id: WorkflowId,
        name: &'a str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &'a WorkflowDefinition,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let definition_value = serde_json::to_value(definition)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT INTO workflows (id, name)
                VALUES ($1, $2)
                ON CONFLICT (id) DO NOTHING
                "#,
                workflow_id.as_uuid(),
                name,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES ($1, $2, $3, $4)
                "#,
                version_id.as_uuid(),
                workflow_id.as_uuid(),
                version as i64,
                definition_value
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
            let version_rows = sqlx::query!(
                r#"
                SELECT id FROM workflow_versions
                WHERE workflow_id = $1
                ORDER BY version ASC
                "#,
                workflow_id.as_uuid()
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(version_rows
                .into_iter()
                .map(|row| WorkflowVersionId::from_uuid(row.id))
                .collect())
        })
    }

    fn get_definition<'a>(
        &'a self,
        version_id: WorkflowVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<WorkflowDefinition, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let definition_row = sqlx::query!(
                r#"
                SELECT definition FROM workflow_versions
                WHERE id = $1
                "#,
                version_id.as_uuid()
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let definition_value = definition_row
                .map(|row| row.definition)
                .ok_or(StorageError::WorkflowVersionNotFound(version_id))?;

            let definition: WorkflowDefinition = serde_json::from_value(definition_value)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            Ok(definition)
        })
    }

    fn save_execution<'a>(
        &'a self,
        execution: &'a Execution,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_value = serde_json::to_value(execution)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let execution_id = execution.id();
            let workflow_version_id = execution.workflow_version_id();

            sqlx::query!(
                r#"
                INSERT INTO workflow_executions (id, workflow_version_id, status, state)
                VALUES ($1, $2, $3, $4)
                "#,
                execution_id.as_uuid(),
                workflow_version_id.as_uuid(),
                execution.status().as_str(),
                execution_value
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

            let execution_id = execution.id();

            let update_result = sqlx::query!(
                r#"
                UPDATE workflow_executions
                SET state = $1, status = $2, version = version + 1
                WHERE id = $3 AND version = $4
                "#,
                execution_value,
                execution.status().as_str(),
                execution_id.as_uuid(),
                expected_version as i64
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            if update_result.rows_affected() == 0 {
                let current_version_row = sqlx::query!(
                    r#"
                    SELECT version FROM workflow_executions
                    WHERE id = $1
                    "#,
                    execution_id.as_uuid()
                )
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

                match current_version_row {
                    Some(row) => Err(StorageError::OptimisticLockFailed {
                        id: execution.id(),
                        expected: expected_version,
                        actual: row.version as u64,
                    }),
                    None => Err(StorageError::ExecutionNotFound(execution.id())),
                }
            } else {
                Ok(())
            }
        })
    }

    fn get_execution<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(Execution, u64), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_row = sqlx::query!(
                r#"
                SELECT state, version FROM workflow_executions
                WHERE id = $1
                "#,
                execution_id.as_uuid()
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let row = execution_row.ok_or(StorageError::ExecutionNotFound(execution_id))?;

            let execution: Execution = serde_json::from_value(row.state)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            Ok((execution, row.version as u64))
        })
    }

    fn get_active_executions<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Execution>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let active_statuses = vec![
                ExecutionStatus::Pending.as_str().to_string(),
                ExecutionStatus::Running.as_str().to_string(),
            ];

            let active_rows = sqlx::query!(
                r#"
                SELECT state FROM workflow_executions
                WHERE status = ANY($1)
                "#,
                &active_statuses[..],
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let mut executions = Vec::with_capacity(active_rows.len());

            for row in active_rows {
                let execution: Execution = serde_json::from_value(row.state)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                executions.push(execution);
            }

            Ok(executions)
        })
    }
}
