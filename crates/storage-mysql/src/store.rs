use miranda_core::{
    execution::{Execution, ExecutionStatus},
    id::{ExecutionId, TaskQueueEntryId, WorkerId, WorkflowId, WorkflowVersionId},
    lease::Lease,
    queue::QueuedTask,
    workflow::WorkflowDefinition,
};
use miranda_storage::{
    error::StorageError,
    join_token_store::JoinTokenStore,
    leadership_store::LeadershipStore,
    lease_store::LeaseStore,
    peer_store::{PeerInfo, PeerStore},
    router_store::RouterStore,
    task_queue_store::TaskQueueStore,
    workflow_store::WorkflowStore,
};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::{future::Future, pin::Pin};
use time::{Duration, OffsetDateTime};

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

fn map_insert_error(err: sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(db_err) = &err {
        if db_err.is_unique_violation() {
            return StorageError::Conflict(db_err.to_string());
        }
    }

    StorageError::Backend(err.to_string())
}

// =========================================================================
// Queue Store Implementation
// =========================================================================

impl TaskQueueStore for MySqlStore {
    fn enqueue<'a>(
        &'a self,
        task: miranda_core::queue::QueuedTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = task.id().to_string();
            let execution_id_str = task.execution_id().to_string();
            let workflow_task_id_str = task.workflow_task_id().to_string();

            sqlx::query!(
                r#"
                INSERT INTO task_queue (id, execution_id, workflow_task_id)
                VALUES (?, ?, ?)
                "#,
                id_str,
                execution_id_str,
                workflow_task_id_str,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn dequeue<'a>(
        &'a self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<miranda_core::queue::QueuedTask>, StorageError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            let row = sqlx::query!(
                r#"
                SELECT id, execution_id, workflow_task_id FROM task_queue
                ORDER BY enqueued_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                "#
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let Some(row) = row else {
                return Ok(None);
            };

            sqlx::query!(r#"DELETE FROM task_queue WHERE id = ?"#, row.id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            let id: TaskQueueEntryId = row
                .id
                .parse()
                .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;
            let execution_id: ExecutionId = row
                .execution_id
                .parse()
                .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;
            let workflow_task_id = row
                .workflow_task_id
                .parse()
                .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;

            Ok(Some(QueuedTask::from_parts(
                id,
                execution_id,
                workflow_task_id,
            )))
        })
    }
}

// =========================================================================
// Router Store Implementation
// =========================================================================

impl RouterStore for MySqlStore {
    fn register_worker<'a>(
        &'a self,
        registration: miranda_core::router::WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = registration.id().to_string();
            let capabilities_json = serde_json::to_value(registration.capabilities())
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT INTO workers (id, capabilities, last_heartbeat)
                VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE capabilities = VALUES(capabilities), last_heartbeat = VALUES(last_heartbeat)
                "#,
                id_str,
                capabilities_json,
                registration.last_heartbeat(),
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn deregister_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = id.to_string();

            sqlx::query!(r#"DELETE FROM workers WHERE id = ?"#, id_str)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn touch_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = id.to_string();
            let now = OffsetDateTime::now_utc();

            sqlx::query!(
                r#"UPDATE workers SET last_heartbeat = ? WHERE id = ?"#,
                now,
                id_str,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<WorkerId>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let capability_json = serde_json::Value::String(capability.to_owned());

            let row = sqlx::query!(
                r#"SELECT id FROM workers WHERE JSON_CONTAINS(capabilities, ?) LIMIT 1"#,
                capability_json,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            row.map(|r| {
                r.id.parse()
                    .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))
            })
            .transpose()
        })
    }

    fn worker_has_capability<'a>(
        &'a self,
        id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = id.to_string();
            let capability_json = serde_json::Value::String(capability.to_owned());

            let row: Option<(i64,)> = sqlx::query_as(
                r#"SELECT 1 FROM workers WHERE id = ? AND JSON_CONTAINS(capabilities, ?)"#,
            )
            .bind(&id_str)
            .bind(&capability_json)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(row.is_some())
        })
    }

    fn reap_stale_workers<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkerId>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc() - threshold;

            let rows = sqlx::query!(r#"SELECT id FROM workers WHERE last_heartbeat < ?"#, cutoff)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(r#"DELETE FROM workers WHERE last_heartbeat < ?"#, cutoff)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            rows.into_iter()
                .map(|r| {
                    r.id.parse()
                        .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))
                })
                .collect()
        })
    }
}

// =========================================================================
// Workflow Store Implementation
// =========================================================================

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
            .map_err(map_insert_error)?;

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

// =========================================================================
// Lease Store Implementation
// =========================================================================

impl LeaseStore for MySqlStore {
    fn create<'a>(
        &'a self,
        lease: Lease,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(
                r#"
                INSERT INTO leases (token, execution_id, workflow_task_id, worker_id, expires_at)
                VALUES (?, ?, ?, ?, ?)
                "#,
                lease.token,
                lease.execution_id.to_string(),
                lease.workflow_task_id.to_string(),
                lease.worker_id.to_string(),
                lease.expires_at,
            )
            .execute(&self.pool)
            .await
            .map_err(map_insert_error)?;

            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Lease>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let row = sqlx::query!(
                r#"SELECT token, execution_id, workflow_task_id, worker_id, expires_at FROM leases WHERE token = ?"#,
                token,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let Some(row) = row else {
                return Ok(None);
            };

            let execution_id = row
                .execution_id
                .parse()
                .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;
            let workflow_task_id = row
                .workflow_task_id
                .parse()
                .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;
            let worker_id = row
                .worker_id
                .parse()
                .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;

            Ok(Some(Lease {
                token: row.token,
                execution_id,
                workflow_task_id,
                worker_id,
                expires_at: row.expires_at,
            }))
        })
    }

    fn renew<'a>(
        &'a self,
        token: &'a str,
        new_expires_at: OffsetDateTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(
                r#"UPDATE leases SET expires_at = ? WHERE token = ?"#,
                new_expires_at,
                token,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn release<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(r#"DELETE FROM leases WHERE token = ?"#, token)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn active_for_worker<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let worker_id_str = worker_id.to_string();

            let rows = sqlx::query!(
                r#"SELECT token FROM leases WHERE worker_id = ? AND expires_at > ?"#,
                worker_id_str,
                OffsetDateTime::now_utc(),
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(rows.into_iter().map(|r| r.token).collect())
        })
    }

    fn reap_expired<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Lease>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();

            let rows = sqlx::query!(
                r#"SELECT token, execution_id, workflow_task_id, worker_id, expires_at FROM leases WHERE expires_at < ?"#,
                now,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(r#"DELETE FROM leases WHERE expires_at < ?"#, now)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            rows.into_iter()
                .map(|row| {
                    let execution_id = row
                        .execution_id
                        .parse()
                        .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;
                    let workflow_task_id = row
                        .workflow_task_id
                        .parse()
                        .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;
                    let worker_id = row
                        .worker_id
                        .parse()
                        .map_err(|e: uuid::Error| StorageError::Serialization(e.to_string()))?;

                    Ok(Lease {
                        token: row.token,
                        execution_id,
                        workflow_task_id,
                        worker_id,
                        expires_at: row.expires_at,
                    })
                })
                .collect()
        })
    }
}

// =========================================================================
// Join Token Store Implementation
// =========================================================================

impl JoinTokenStore for MySqlStore {
    fn set_token<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(r#"DELETE FROM join_tokens"#)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(r#"INSERT INTO join_tokens (token) VALUES (?)"#, token)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn get_token<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let row = sqlx::query!(r#"SELECT token FROM join_tokens LIMIT 1"#)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(row.map(|r| r.token))
        })
    }
}

// =========================================================================
// Leadership Store Implementation
// =========================================================================

const LEADERSHIP_ROW_ID: &str = "control-plane";

impl LeadershipStore for MySqlStore {
    fn try_acquire<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let expires_at = OffsetDateTime::now_utc() + ttl;
            let now = OffsetDateTime::now_utc();

            let result = sqlx::query!(
                r#"
                INSERT INTO leadership (id, holder_id, expires_at)
                VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    holder_id = IF(expires_at < ?, VALUES(holder_id), holder_id),
                    expires_at = IF(expires_at < ?, VALUES(expires_at), expires_at)
                "#,
                LEADERSHIP_ROW_ID,
                holder_id,
                expires_at,
                now,
                now,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let _ = result;

            let row = sqlx::query!(
                r#"SELECT holder_id FROM leadership WHERE id = ?"#,
                LEADERSHIP_ROW_ID,
            )
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(row.holder_id == holder_id)
        })
    }

    fn renew<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let expires_at = OffsetDateTime::now_utc() + ttl;
            let now = OffsetDateTime::now_utc();

            let result = sqlx::query!(
                r#"
                UPDATE leadership
                SET expires_at = ?
                WHERE id = ? AND holder_id = ? AND expires_at > ?
                "#,
                expires_at,
                LEADERSHIP_ROW_ID,
                holder_id,
                now,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(result.rows_affected() > 0)
        })
    }

    fn release<'a>(
        &'a self,
        holder_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(
                r#"DELETE FROM leadership WHERE id = ? AND holder_id = ?"#,
                LEADERSHIP_ROW_ID,
                holder_id,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn current_holder<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();

            let row = sqlx::query!(
                r#"SELECT holder_id FROM leadership WHERE id = ? AND expires_at > ?"#,
                LEADERSHIP_ROW_ID,
                now,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(row.map(|r| r.holder_id))
        })
    }
}

// =========================================================================
// Peer Store Implementation
// =========================================================================

impl PeerStore for MySqlStore {
    fn register<'a>(
        &'a self,
        id: &'a str,
        grpc_address: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();

            sqlx::query!(
                r#"
                INSERT INTO control_plane_instances (id, grpc_address, last_heartbeat)
                VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE grpc_address = VALUES(grpc_address), last_heartbeat = VALUES(last_heartbeat)
                "#,
                id,
                grpc_address,
                now,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn touch<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();

            sqlx::query!(
                r#"UPDATE control_plane_instances SET last_heartbeat = ? WHERE id = ?"#,
                now,
                id,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn deregister<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(r#"DELETE FROM control_plane_instances WHERE id = ?"#, id)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn list_active<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc() - threshold;

            let rows = sqlx::query!(
                r#"SELECT id, grpc_address FROM control_plane_instances WHERE last_heartbeat > ?"#,
                cutoff,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(|r| PeerInfo {
                    id: r.id,
                    grpc_address: r.grpc_address,
                })
                .collect())
        })
    }

    fn reap_stale<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc() - threshold;

            let rows = sqlx::query!(
                r#"SELECT id FROM control_plane_instances WHERE last_heartbeat < ?"#,
                cutoff,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(
                r#"DELETE FROM control_plane_instances WHERE last_heartbeat < ?"#,
                cutoff,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(rows.into_iter().map(|r| r.id).collect())
        })
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use sqlx::error::{DatabaseError, ErrorKind};
    use std::{error::Error as StdError, fmt};

    use super::*;

    #[derive(Debug)]
    struct FakeDbError {
        kind: ErrorKind,
    }

    impl fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "fake db error")
        }
    }

    impl StdError for FakeDbError {}

    impl DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            "fake db error"
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> ErrorKind {
            match self.kind {
                ErrorKind::UniqueViolation => ErrorKind::UniqueViolation,
                ErrorKind::ForeignKeyViolation => ErrorKind::ForeignKeyViolation,
                _ => ErrorKind::Other,
            }
        }
    }

    unsafe impl Send for FakeDbError {}
    unsafe impl Sync for FakeDbError {}

    #[test]
    fn map_insert_error_treats_unique_violation_as_conflict() {
        let err = sqlx::Error::Database(Box::new(FakeDbError {
            kind: ErrorKind::UniqueViolation,
        }));

        assert!(matches!(map_insert_error(err), StorageError::Conflict(_)));
    }

    #[test]
    fn map_insert_error_treats_other_db_errors_as_backend() {
        let err = sqlx::Error::Database(Box::new(FakeDbError {
            kind: ErrorKind::ForeignKeyViolation,
        }));

        assert!(matches!(map_insert_error(err), StorageError::Backend(_)));
    }

    #[test]
    fn map_insert_error_treats_non_database_errors_as_backend() {
        let err = sqlx::Error::PoolClosed;

        assert!(matches!(map_insert_error(err), StorageError::Backend(_)));
    }
}
