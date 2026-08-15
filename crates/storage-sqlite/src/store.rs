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
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::workflow::{WorkflowDefinition, WorkflowTask};

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
        let task_id = miranda_core::id::WorkflowTaskId::new();
        let task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![]).unwrap();

        WorkflowDefinition::new(vec![task]).unwrap()
    }

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
}
