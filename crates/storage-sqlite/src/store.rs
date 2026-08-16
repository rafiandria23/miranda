use miranda_core::{
    execution::{Execution, ExecutionStatus},
    id::{ExecutionId, TaskQueueEntryId, WorkerId, WorkflowId, WorkflowTaskId, WorkflowVersionId},
    lease::Lease,
    queue::QueuedTask,
    router::WorkerRegistration,
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
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
use std::{future::Future, pin::Pin};
use time::{Duration, OffsetDateTime};

use crate::SqliteConfig;

#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub async fn connect(config: SqliteConfig) -> Result<Self, sqlx::Error> {
        let connect_url = format!("sqlite://{}?mode=rwc", config.path);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&connect_url)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }
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

impl TaskQueueStore for SqliteStore {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id = task.id();
            let execution_id = task.execution_id();
            let workflow_task_id = task.workflow_task_id();

            let id_str = id.to_string();
            let execution_id_str = execution_id.to_string();
            let workflow_task_id_str = workflow_task_id.to_string();

            sqlx::query!(
                r#"
                INSERT INTO task_queue (id, execution_id, workflow_task_id)
                VALUES (?1, ?2, ?3)
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<QueuedTask>, StorageError>> + Send + 'a>> {
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
                "#
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let Some(row) = row else {
                return Ok(None);
            };

            sqlx::query!(r#"DELETE FROM task_queue WHERE id = ?1"#, row.id)
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
            let workflow_task_id: WorkflowTaskId = row
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

impl RouterStore for SqliteStore {
    fn register_worker<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id_str = registration.id().to_string();
            let capabilities_json = serde_json::to_string(registration.capabilities())
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let heartbeat = registration.last_heartbeat();

            sqlx::query!(
                r#"
                INSERT INTO workers (id, capabilities, last_heartbeat)
                VALUES (?1, ?2, ?3)
                ON CONFLICT (id) DO UPDATE SET capabilities = ?2, last_heartbeat = ?3
                "#,
                id_str,
                capabilities_json,
                heartbeat,
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

            sqlx::query!(r#"DELETE FROM workers WHERE id = ?1"#, id_str)
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
                r#"UPDATE workers SET last_heartbeat = ?1 WHERE id = ?2"#,
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
            let row = sqlx::query!(
                r#"
                SELECT w.id FROM workers w, json_each(w.capabilities) c
                WHERE c.value = ?1
                LIMIT 1
                "#,
                capability,
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

            let row: Option<(i64,)> = sqlx::query_as(
                r#"
                SELECT 1 FROM workers w, json_each(w.capabilities) c
                WHERE w.id = ? AND c.value = ?
                "#,
            )
            .bind(&id_str)
            .bind(capability)
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

            let rows = sqlx::query!(
                r#"SELECT id FROM workers WHERE last_heartbeat < ?1"#,
                cutoff
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(r#"DELETE FROM workers WHERE last_heartbeat < ?1"#, cutoff)
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

impl WorkflowStore for SqliteStore {
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
                INSERT OR IGNORE INTO workflows (id, name)
                VALUES (?1, ?2)
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
                VALUES (?1, ?2, ?3, ?4)
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
                WHERE workflow_id = ?1
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
                WHERE id = ?1
                "#,
                version_id_str,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let definition_json = row
                .map(|r| r.definition)
                .ok_or(StorageError::WorkflowVersionNotFound(version_id))?;

            serde_json::from_str(&definition_json)
                .map_err(|e| StorageError::Serialization(e.to_string()))
        })
    }

    fn save_execution<'a>(
        &'a self,
        execution: &'a Execution,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_json = serde_json::to_string(execution)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let id_str = execution.id().to_string();
            let version_id_str = execution.workflow_version_id().to_string();
            let status = execution.status().as_str();

            sqlx::query!(
                r#"
                INSERT INTO workflow_executions (id, workflow_version_id, status, state)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                id_str,
                version_id_str,
                status,
                execution_json,
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
            let execution_json = serde_json::to_string(execution)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let id_str = execution.id().to_string();
            let status = execution.status().as_str();
            let expected_version = expected_version as i64;

            let result = sqlx::query!(
                r#"
                UPDATE workflow_executions
                SET state = ?1, status = ?2, version = version + 1
                WHERE id = ?3 AND version = ?4
                "#,
                execution_json,
                status,
                id_str,
                expected_version,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            if result.rows_affected() == 0 {
                let current = sqlx::query!(
                    r#"SELECT version FROM workflow_executions WHERE id = ?1"#,
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
                r#"SELECT state, version FROM workflow_executions WHERE id = ?1"#,
                id_str,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let row = row.ok_or(StorageError::ExecutionNotFound(execution_id))?;

            let execution: Execution = serde_json::from_str(&row.state)
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
                r#"SELECT state FROM workflow_executions WHERE status IN (?1, ?2)"#,
                pending,
                running,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            rows.into_iter()
                .map(|row| {
                    serde_json::from_str(&row.state)
                        .map_err(|e| StorageError::Serialization(e.to_string()))
                })
                .collect()
        })
    }
}

// =========================================================================
// Lease Store Implementation
// =========================================================================

impl LeaseStore for SqliteStore {
    fn create<'a>(
        &'a self,
        lease: Lease,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_id_str = lease.execution_id.to_string();
            let workflow_task_id_str = lease.workflow_task_id.to_string();
            let worker_id_str = lease.worker_id.to_string();
            let expires_at_str = lease
                .expires_at
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT INTO leases (token, execution_id, workflow_task_id, worker_id, expires_at)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                lease.token,
                execution_id_str,
                workflow_task_id_str,
                worker_id_str,
                expires_at_str,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Lease>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let row = sqlx::query!(
                r#"SELECT token, execution_id, workflow_task_id, worker_id, expires_at FROM leases WHERE token = ?1"#,
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
            let expires_at = OffsetDateTime::parse(
                &row.expires_at,
                &time::format_description::well_known::Rfc3339,
            )
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

            Ok(Some(Lease {
                token: row.token,
                execution_id,
                workflow_task_id,
                worker_id,
                expires_at,
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
                r#"UPDATE leases SET expires_at = ?1 WHERE token = ?2"#,
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
            sqlx::query!(r#"DELETE FROM leases WHERE token = ?1"#, token)
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
            let now = OffsetDateTime::now_utc();

            let rows = sqlx::query!(
                r#"SELECT token FROM leases WHERE worker_id = ?1 AND expires_at > ?2"#,
                worker_id_str,
                now,
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
                r#"SELECT token, execution_id, workflow_task_id, worker_id, expires_at FROM leases WHERE expires_at < ?1"#,
                now,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(r#"DELETE FROM leases WHERE expires_at < ?1"#, now)
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
                    let expires_at = OffsetDateTime::parse(
                        &row.expires_at,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                    Ok(Lease {
                        token: row.token,
                        execution_id,
                        workflow_task_id,
                        worker_id,
                        expires_at,
                    })
                })
                .collect()
        })
    }
}

// =========================================================================
// Join Token Store Implementation
// =========================================================================

impl JoinTokenStore for SqliteStore {
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

            sqlx::query!(r#"INSERT INTO join_tokens (token) VALUES (?1)"#, token)
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

impl LeadershipStore for SqliteStore {
    fn try_acquire<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let expires_at_str = (OffsetDateTime::now_utc() + ttl)
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let now_str = OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let row = sqlx::query!(
                r#"
                INSERT INTO leadership (id, holder_id, expires_at)
                VALUES (?1, ?2, ?3)
                ON CONFLICT (id) DO UPDATE
                SET holder_id = ?2, expires_at = ?3
                WHERE leadership.expires_at < ?4
                RETURNING holder_id
                "#,
                LEADERSHIP_ROW_ID,
                holder_id,
                expires_at_str,
                now_str,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(row.is_some())
        })
    }

    fn renew<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let expires_at_str = (OffsetDateTime::now_utc() + ttl)
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let now_str = OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let result = sqlx::query!(
                r#"
                UPDATE leadership
                SET expires_at = ?1
                WHERE id = ?2 AND holder_id = ?3 AND expires_at > ?4
                "#,
                expires_at_str,
                LEADERSHIP_ROW_ID,
                holder_id,
                now_str,
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
                r#"DELETE FROM leadership WHERE id = ?1 AND holder_id = ?2"#,
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
            let now_str = OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let row = sqlx::query!(
                r#"SELECT holder_id FROM leadership WHERE id = ?1 AND expires_at > ?2"#,
                LEADERSHIP_ROW_ID,
                now_str,
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

impl PeerStore for SqliteStore {
    fn register<'a>(
        &'a self,
        id: &'a str,
        grpc_address: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let now_str = OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            sqlx::query!(
                r#"
                INSERT INTO control_plane_instances (id, grpc_address, last_heartbeat)
                VALUES (?1, ?2, ?3)
                ON CONFLICT (id) DO UPDATE
                SET grpc_address = ?2, last_heartbeat = ?3
                "#,
                id,
                grpc_address,
                now_str,
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
            let now_str = OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            sqlx::query!(
                r#"UPDATE control_plane_instances SET last_heartbeat = ?1 WHERE id = ?2"#,
                now_str,
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
            sqlx::query!(r#"DELETE FROM control_plane_instances WHERE id = ?1"#, id)
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
            let cutoff_str = (OffsetDateTime::now_utc() - threshold)
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let rows = sqlx::query!(
                r#"SELECT id, grpc_address FROM control_plane_instances WHERE last_heartbeat > ?1"#,
                cutoff_str,
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
            let cutoff_str = (OffsetDateTime::now_utc() - threshold)
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let rows = sqlx::query!(
                r#"SELECT id FROM control_plane_instances WHERE last_heartbeat < ?1"#,
                cutoff_str,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            sqlx::query!(
                r#"DELETE FROM control_plane_instances WHERE last_heartbeat < ?1"#,
                cutoff_str,
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
    use miranda_core::{id::WorkflowTaskId, workflow::WorkflowTask};

    use super::*;

    async fn test_store() -> SqliteStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        SqliteStore::from_pool(pool)
    }

    fn definition() -> WorkflowDefinition {
        let task_id = WorkflowTaskId::new();
        let task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![]).unwrap();

        WorkflowDefinition::new(vec![task]).unwrap()
    }

    fn worker(capabilities: &[&str]) -> WorkerRegistration {
        WorkerRegistration::new(
            WorkerId::new(),
            capabilities.iter().map(|c| c.to_string()).collect(),
            OffsetDateTime::now_utc(),
        )
    }

    // ---------------------------------------------------------------
    // TaskQueueStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn dequeue_on_empty_queue_returns_none() {
        let store = test_store().await;

        assert!(store.dequeue().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn enqueue_then_dequeue_returns_fifo_order() {
        let store = test_store().await;
        let execution_id = ExecutionId::new();

        let first = QueuedTask::new(execution_id, WorkflowTaskId::new());
        let second = QueuedTask::new(execution_id, WorkflowTaskId::new());

        store.enqueue(first).await.unwrap();
        store.enqueue(second).await.unwrap();

        assert_eq!(store.dequeue().await.unwrap().unwrap().id(), first.id());
        assert_eq!(store.dequeue().await.unwrap().unwrap().id(), second.id());
        assert!(store.dequeue().await.unwrap().is_none());
    }

    // ---------------------------------------------------------------
    // RouterStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn register_then_select_worker_finds_matching_capability() {
        let store = test_store().await;
        let reg = worker(&["email"]);
        let id = reg.id();

        store.register_worker(reg).await.unwrap();

        assert_eq!(store.select_worker("email").await.unwrap(), Some(id));
        assert_eq!(store.select_worker("sms").await.unwrap(), None);
    }

    #[tokio::test]
    async fn register_worker_upserts_existing_id() {
        let store = test_store().await;
        let id = WorkerId::new();

        let reg = WorkerRegistration::new(
            id,
            vec!["email".to_string()],
            OffsetDateTime::now_utc(),
        );
        store.register_worker(reg).await.unwrap();

        let updated = WorkerRegistration::new(
            id,
            vec!["sms".to_string()],
            OffsetDateTime::now_utc(),
        );
        store.register_worker(updated).await.unwrap();

        assert_eq!(store.select_worker("email").await.unwrap(), None);
        assert_eq!(store.select_worker("sms").await.unwrap(), Some(id));
    }

    #[tokio::test]
    async fn worker_has_capability_reflects_registration() {
        let store = test_store().await;
        let reg = worker(&["email"]);
        let id = reg.id();

        store.register_worker(reg).await.unwrap();

        assert!(store.worker_has_capability(id, "email").await.unwrap());
        assert!(!store.worker_has_capability(id, "sms").await.unwrap());
        assert!(
            !store
                .worker_has_capability(WorkerId::new(), "email")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn deregister_worker_removes_it() {
        let store = test_store().await;
        let reg = worker(&["email"]);
        let id = reg.id();

        store.register_worker(reg).await.unwrap();
        store.deregister_worker(id).await.unwrap();

        assert_eq!(store.select_worker("email").await.unwrap(), None);
    }

    #[tokio::test]
    async fn touch_worker_updates_heartbeat() {
        let store = test_store().await;
        let old_heartbeat = OffsetDateTime::now_utc() - Duration::minutes(5);
        let reg =
            WorkerRegistration::new(WorkerId::new(), vec!["email".to_string()], old_heartbeat);
        let id = reg.id();

        store.register_worker(reg).await.unwrap();
        store.touch_worker(id).await.unwrap();

        assert!(
            !store
                .reap_stale_workers(Duration::minutes(1))
                .await
                .unwrap()
                .contains(&id)
        );
    }

    #[tokio::test]
    async fn reap_stale_workers_removes_only_stale_entries() {
        let store = test_store().await;
        let stale = WorkerRegistration::new(
            WorkerId::new(),
            vec![],
            OffsetDateTime::now_utc() - Duration::minutes(10),
        );
        let fresh = worker(&[]);
        let stale_id = stale.id();
        let fresh_id = fresh.id();

        store.register_worker(stale).await.unwrap();
        store.register_worker(fresh).await.unwrap();

        let reaped = store
            .reap_stale_workers(Duration::minutes(1))
            .await
            .unwrap();

        assert_eq!(reaped, vec![stale_id]);
        assert!(store.worker_has_capability(fresh_id, "").await.is_ok());
    }

    // ---------------------------------------------------------------
    // WorkflowStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn save_definition_maps_duplicate_version_to_conflict() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let definition = definition();

        store
            .save_definition(
                workflow_id,
                "test_workflow",
                WorkflowVersionId::new(),
                1,
                &definition,
            )
            .await
            .unwrap();

        let err = store
            .save_definition(
                workflow_id,
                "test_workflow",
                WorkflowVersionId::new(),
                1,
                &definition,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, StorageError::Conflict(_)),
            "expected a Conflict error for a duplicate (workflow_id, version) pair, got {err:?}"
        );
    }

    #[tokio::test]
    async fn save_definition_succeeds_for_distinct_versions_of_same_workflow() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let definition = definition();

        store
            .save_definition(
                workflow_id,
                "test_workflow",
                WorkflowVersionId::new(),
                1,
                &definition,
            )
            .await
            .unwrap();

        store
            .save_definition(
                workflow_id,
                "test_workflow",
                WorkflowVersionId::new(),
                2,
                &definition,
            )
            .await
            .unwrap();

        let versions = store.get_versions(workflow_id).await.unwrap();
        assert_eq!(versions.len(), 2);
    }

    #[tokio::test]
    async fn get_definition_missing_returns_not_found() {
        let store = test_store().await;
        let version_id = WorkflowVersionId::new();

        let err = store.get_definition(version_id).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::WorkflowVersionNotFound(id) if id == version_id
        ));
    }

    #[tokio::test]
    async fn save_and_get_definition_roundtrips() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = definition();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let loaded = store.get_definition(version_id).await.unwrap();

        assert_eq!(loaded, definition);
    }

    #[tokio::test]
    async fn save_and_get_execution_roundtrips_with_version_one() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = definition();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let execution = Execution::from_definition(version_id, &definition).unwrap();
        store.save_execution(&execution).await.unwrap();

        let (loaded, version) = store.get_execution(execution.id()).await.unwrap();

        assert_eq!(loaded.id(), execution.id());
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn get_execution_missing_returns_not_found() {
        let store = test_store().await;
        let execution_id = ExecutionId::new();

        let err = store.get_execution(execution_id).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::ExecutionNotFound(id) if id == execution_id
        ));
    }

    #[tokio::test]
    async fn update_execution_bumps_version_on_expected_match() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = definition();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let execution = Execution::from_definition(version_id, &definition).unwrap();
        store.save_execution(&execution).await.unwrap();
        store.update_execution(&execution, 1).await.unwrap();

        let (_, version) = store.get_execution(execution.id()).await.unwrap();

        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn update_execution_missing_returns_not_found() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = definition();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let execution = Execution::from_definition(version_id, &definition).unwrap();

        let err = store.update_execution(&execution, 1).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::ExecutionNotFound(id) if id == execution.id()
        ));
    }

    #[tokio::test]
    async fn update_execution_with_stale_version_fails_optimistic_lock() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = definition();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let execution = Execution::from_definition(version_id, &definition).unwrap();
        store.save_execution(&execution).await.unwrap();

        let err = store.update_execution(&execution, 99).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::OptimisticLockFailed {
                expected: 99,
                actual: 1,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn get_active_executions_excludes_finished() {
        let store = test_store().await;
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = definition();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let active = Execution::from_definition(version_id, &definition).unwrap();
        store.save_execution(&active).await.unwrap();

        let active_executions = store.get_active_executions().await.unwrap();

        assert!(active_executions.iter().any(|e| e.id() == active.id()));
    }

    // ---------------------------------------------------------------
    // LeaseStore
    // ---------------------------------------------------------------

    fn lease(token: &str, expires_in: Duration) -> Lease {
        Lease {
            worker_id: WorkerId::new(),
            workflow_task_id: WorkflowTaskId::new(),
            execution_id: ExecutionId::new(),
            token: token.to_owned(),
            expires_at: OffsetDateTime::now_utc() + expires_in,
        }
    }

    #[tokio::test]
    async fn create_then_get_lease_roundtrips() {
        let store = test_store().await;
        let l = lease("token-1", Duration::minutes(5));

        store.create(l.clone()).await.unwrap();

        let loaded = store.get("token-1").await.unwrap().unwrap();

        assert_eq!(loaded.token, l.token);
        assert_eq!(loaded.worker_id, l.worker_id);
    }

    #[tokio::test]
    async fn get_missing_lease_returns_none() {
        let store = test_store().await;

        assert!(store.get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn renew_updates_expiry() {
        let store = test_store().await;
        let l = lease("token-1", Duration::minutes(5));

        store.create(l).await.unwrap();

        let new_expiry = OffsetDateTime::now_utc() + Duration::hours(1);
        LeaseStore::renew(&store, "token-1", new_expiry)
            .await
            .unwrap();

        let loaded = store.get("token-1").await.unwrap().unwrap();

        assert!((loaded.expires_at - new_expiry).whole_seconds().abs() <= 1);
    }

    #[tokio::test]
    async fn release_removes_lease() {
        let store = test_store().await;
        let l = lease("token-1", Duration::minutes(5));

        store.create(l).await.unwrap();
        LeaseStore::release(&store, "token-1").await.unwrap();

        assert!(store.get("token-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn active_for_worker_excludes_expired_and_other_workers() {
        let store = test_store().await;
        let worker_id = WorkerId::new();

        let active = Lease {
            worker_id,
            workflow_task_id: WorkflowTaskId::new(),
            execution_id: ExecutionId::new(),
            token: "active".to_owned(),
            expires_at: OffsetDateTime::now_utc() + Duration::minutes(5),
        };
        let expired = Lease {
            worker_id,
            workflow_task_id: WorkflowTaskId::new(),
            execution_id: ExecutionId::new(),
            token: "expired".to_owned(),
            expires_at: OffsetDateTime::now_utc() - Duration::minutes(5),
        };
        let other_worker = lease("other", Duration::minutes(5));

        store.create(active).await.unwrap();
        store.create(expired).await.unwrap();
        store.create(other_worker).await.unwrap();

        let tokens = store.active_for_worker(worker_id).await.unwrap();

        assert_eq!(tokens, vec!["active".to_string()]);
    }

    #[tokio::test]
    async fn reap_expired_removes_and_returns_expired_leases() {
        let store = test_store().await;
        let active = lease("active", Duration::minutes(5));
        let expired = lease("expired", -Duration::minutes(5));

        store.create(active).await.unwrap();
        store.create(expired).await.unwrap();

        let reaped = store.reap_expired().await.unwrap();

        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].token, "expired");
        assert!(store.get("expired").await.unwrap().is_none());
        assert!(store.get("active").await.unwrap().is_some());
    }

    // ---------------------------------------------------------------
    // JoinTokenStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn get_token_with_none_set_returns_none() {
        let store = test_store().await;

        assert!(store.get_token().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_then_get_token_roundtrips() {
        let store = test_store().await;

        store.set_token("secret").await.unwrap();

        assert_eq!(store.get_token().await.unwrap(), Some("secret".to_string()));
    }

    #[tokio::test]
    async fn set_token_overwrites_previous_value() {
        let store = test_store().await;

        store.set_token("first").await.unwrap();
        store.set_token("second").await.unwrap();

        assert_eq!(store.get_token().await.unwrap(), Some("second".to_string()));
    }

    // ---------------------------------------------------------------
    // LeadershipStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn try_acquire_succeeds_when_unheld() {
        let store = test_store().await;

        assert!(
            store
                .try_acquire("node-1", Duration::minutes(1))
                .await
                .unwrap()
        );
        assert_eq!(
            store.current_holder().await.unwrap(),
            Some("node-1".to_string())
        );
    }

    #[tokio::test]
    async fn try_acquire_fails_when_already_held_and_not_expired() {
        let store = test_store().await;

        store
            .try_acquire("node-1", Duration::minutes(1))
            .await
            .unwrap();

        assert!(
            !store
                .try_acquire("node-2", Duration::minutes(1))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn try_acquire_succeeds_after_expiry() {
        let store = test_store().await;

        store
            .try_acquire("node-1", -Duration::seconds(1))
            .await
            .unwrap();

        assert!(
            store
                .try_acquire("node-2", Duration::minutes(1))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn renew_extends_holder_lease() {
        let store = test_store().await;

        store
            .try_acquire("node-1", Duration::minutes(1))
            .await
            .unwrap();

        assert!(
            LeadershipStore::renew(&store, "node-1", Duration::minutes(5))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn renew_fails_for_non_holder() {
        let store = test_store().await;

        store
            .try_acquire("node-1", Duration::minutes(1))
            .await
            .unwrap();

        assert!(
            !LeadershipStore::renew(&store, "node-2", Duration::minutes(5))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn release_clears_holder_when_current_holder() {
        let store = test_store().await;

        store
            .try_acquire("node-1", Duration::minutes(1))
            .await
            .unwrap();
        LeadershipStore::release(&store, "node-1").await.unwrap();

        assert_eq!(store.current_holder().await.unwrap(), None);
    }

    #[tokio::test]
    async fn release_is_noop_for_non_holder() {
        let store = test_store().await;

        store
            .try_acquire("node-1", Duration::minutes(1))
            .await
            .unwrap();
        LeadershipStore::release(&store, "node-2").await.unwrap();

        assert_eq!(
            store.current_holder().await.unwrap(),
            Some("node-1".to_string())
        );
    }

    #[tokio::test]
    async fn current_holder_none_when_expired() {
        let store = test_store().await;

        store
            .try_acquire("node-1", -Duration::seconds(1))
            .await
            .unwrap();

        assert_eq!(store.current_holder().await.unwrap(), None);
    }

    // ---------------------------------------------------------------
    // PeerStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn register_then_list_active_returns_peer() {
        let store = test_store().await;

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();

        let peers = store.list_active(Duration::minutes(1)).await.unwrap();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer-1");
        assert_eq!(peers[0].grpc_address, "127.0.0.1:9000");
    }

    #[tokio::test]
    async fn list_active_excludes_stale_peers() {
        let store = test_store().await;

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        store.register("peer-2", "127.0.0.1:9001").await.unwrap();

        sqlx::query(
            "UPDATE control_plane_instances SET last_heartbeat = datetime('now', '-10 minutes') WHERE id = 'peer-2'"
        )
        .execute(&store.pool)
        .await
        .unwrap();

        let peers = store.list_active(Duration::minutes(1)).await.unwrap();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer-1");
    }

    #[tokio::test]
    async fn touch_updates_last_heartbeat() {
        let store = test_store().await;

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        sqlx::query(
            "UPDATE control_plane_instances SET last_heartbeat = datetime('now', '-10 minutes') WHERE id = 'peer-1'"
        )
        .execute(&store.pool)
        .await
        .unwrap();

        store.touch("peer-1").await.unwrap();

        let peers = store.list_active(Duration::minutes(1)).await.unwrap();

        assert_eq!(peers.len(), 1);
    }

    #[tokio::test]
    async fn touch_on_unknown_peer_is_ok() {
        let store = test_store().await;

        assert!(store.touch("missing").await.is_ok());
    }

    #[tokio::test]
    async fn deregister_removes_peer() {
        let store = test_store().await;

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        store.deregister("peer-1").await.unwrap();

        assert!(
            store
                .list_active(Duration::minutes(1))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn reap_stale_removes_and_returns_stale_peer_ids() {
        let store = test_store().await;

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        store.register("peer-2", "127.0.0.1:9001").await.unwrap();
        sqlx::query(
            "UPDATE control_plane_instances SET last_heartbeat = datetime('now', '-10 minutes') WHERE id = 'peer-2'"
        )
        .execute(&store.pool)
        .await
        .unwrap();

        let reaped = store.reap_stale(Duration::minutes(1)).await.unwrap();

        assert_eq!(reaped, vec!["peer-2".to_string()]);
        assert!(
            store
                .list_active(Duration::minutes(1))
                .await
                .unwrap()
                .iter()
                .any(|p| p.id == "peer-1")
        );
    }
}
