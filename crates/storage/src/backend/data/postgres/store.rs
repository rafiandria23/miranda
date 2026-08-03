use miranda_core::{
    execution::Execution,
    execution::status::ExecutionStatus,
    id::{ExecutionId, WorkflowId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::{error::StorageError, workflow_store::WorkflowStore};

use super::PostgresConfig;

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(config: PostgresConfig) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .min_connections(config.min_connections)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .idle_timeout(config.idle_timeout)
            .max_lifetime(config.max_lifetime)
            .connect(&config.url)
            .await?;

        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl WorkflowStore for PostgresStore {
    async fn save_definition(
        &self,
        workflow_id: WorkflowId,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &WorkflowDefinition,
    ) -> Result<(), StorageError> {
        let definition_value = serde_json::to_value(definition)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

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
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn get_versions(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<WorkflowVersionId>, StorageError> {
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
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(version_rows
            .into_iter()
            .map(|row| WorkflowVersionId::from_uuid(row.id))
            .collect())
    }

    async fn get_definition(
        &self,
        version_id: WorkflowVersionId,
    ) -> Result<WorkflowDefinition, StorageError> {
        let definition_row = sqlx::query!(
            r#"
            SELECT definition FROM workflow_versions
            WHERE id = $1
            "#,
            version_id.as_uuid()
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let definition_value = definition_row
            .map(|row| row.definition)
            .ok_or(StorageError::WorkflowVersionNotFound(version_id))?;

        let definition: WorkflowDefinition = serde_json::from_value(definition_value)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(definition)
    }

    async fn save_execution(&self, execution: &Execution) -> Result<(), StorageError> {
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
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn update_execution(
        &self,
        execution: &Execution,
        expected_version: u64,
    ) -> Result<(), StorageError> {
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
        .map_err(|e| StorageError::Database(e.to_string()))?;

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
            .map_err(|e| StorageError::Database(e.to_string()))?;

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
    }

    async fn get_execution(
        &self,
        execution_id: ExecutionId,
    ) -> Result<(Execution, u64), StorageError> {
        let execution_row = sqlx::query!(
            r#"
        SELECT state, version FROM workflow_executions
        WHERE id = $1
        "#,
            execution_id.as_uuid()
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let row = execution_row.ok_or(StorageError::ExecutionNotFound(execution_id))?;

        let execution: Execution = serde_json::from_value(row.state)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok((execution, row.version as u64))
    }

    async fn get_active_executions(&self) -> Result<Vec<Execution>, StorageError> {
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
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut executions = Vec::with_capacity(active_rows.len());

        for row in active_rows {
            let execution: Execution = serde_json::from_value(row.state)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            executions.push(execution);
        }

        Ok(executions)
    }
}
