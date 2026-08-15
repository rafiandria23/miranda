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
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::{future::Future, pin::Pin};
use time::{Duration, OffsetDateTime};

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

impl TaskQueueStore for PostgresStore {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id = task.id();
            let execution_id = task.execution_id();
            let workflow_task_id = task.workflow_task_id();

            sqlx::query!(
                r#"
                INSERT INTO task_queue (id, execution_id, workflow_task_id)
                VALUES ($1, $2, $3)
                "#,
                id.as_uuid(),
                execution_id.as_uuid(),
                workflow_task_id.as_uuid(),
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
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                "#
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let Some(row) = row else {
                return Ok(None);
            };

            sqlx::query!(r#"DELETE FROM task_queue WHERE id = $1"#, row.id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(Some(QueuedTask::from_parts(
                TaskQueueEntryId::from_uuid(row.id),
                ExecutionId::from_uuid(row.execution_id),
                WorkflowTaskId::from_uuid(row.workflow_task_id),
            )))
        })
    }
}

// =========================================================================
// Router Store Implementation
// =========================================================================

impl RouterStore for PostgresStore {
    fn register_worker<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let id = registration.id();

            sqlx::query!(
                r#"
                INSERT INTO workers (id, capabilities, last_heartbeat)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET capabilities = $2, last_heartbeat = $3
                "#,
                id.as_uuid(),
                registration.capabilities(),
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
            sqlx::query!(r#"DELETE FROM workers WHERE id = $1"#, id.as_uuid())
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
            sqlx::query!(
                r#"UPDATE workers SET last_heartbeat = $1 WHERE id = $2"#,
                OffsetDateTime::now_utc(),
                id.as_uuid(),
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
                r#"SELECT id FROM workers WHERE $1 = ANY(capabilities) LIMIT 1"#,
                capability,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(row.map(|r| WorkerId::from_uuid(r.id)))
        })
    }

    fn worker_has_capability<'a>(
        &'a self,
        id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query_scalar!(
                r#"SELECT EXISTS(SELECT 1 FROM workers WHERE id = $1 AND $2 = ANY(capabilities)) as "exists!""#,
                id.as_uuid(),
                capability,
            )
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))
        })
    }

    fn reap_stale_workers<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkerId>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc() - threshold;

            let rows = sqlx::query!(
                r#"DELETE FROM workers WHERE last_heartbeat < $1 RETURNING id"#,
                cutoff,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(|r| WorkerId::from_uuid(r.id))
                .collect())
        })
    }
}

// =========================================================================
// Workflow Store Implementation
// =========================================================================

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

// =========================================================================
// Lease Store Implementation
// =========================================================================

impl LeaseStore for PostgresStore {
    fn create<'a>(
        &'a self,
        lease: Lease,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(
                r#"
                INSERT INTO leases (token, execution_id, workflow_task_id, worker_id, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                "#,
                lease.token,
                lease.execution_id.as_uuid(),
                lease.workflow_task_id.as_uuid(),
                lease.worker_id.as_uuid(),
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
                r#"SELECT token, execution_id, workflow_task_id, worker_id, expires_at FROM leases WHERE token = $1"#,
                token,
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            let Some(row) = row else {
                return Ok(None);
            };

            Ok(Some(Lease {
                token: row.token,
                execution_id: ExecutionId::from_uuid(row.execution_id),
                workflow_task_id: WorkflowTaskId::from_uuid(row.workflow_task_id),
                worker_id: WorkerId::from_uuid(row.worker_id),
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
                r#"UPDATE leases SET expires_at = $1 WHERE token = $2"#,
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
            sqlx::query!(r#"DELETE FROM leases WHERE token = $1"#, token)
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
            let rows = sqlx::query!(
                r#"SELECT token FROM leases WHERE worker_id = $1 AND expires_at > $2"#,
                worker_id.as_uuid(),
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
                r#"DELETE FROM leases WHERE expires_at < $1 RETURNING token, execution_id, workflow_task_id, worker_id, expires_at"#,
                now,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(|row| Lease {
                    token: row.token,
                    execution_id: ExecutionId::from_uuid(row.execution_id),
                    workflow_task_id: WorkflowTaskId::from_uuid(row.workflow_task_id),
                    worker_id: WorkerId::from_uuid(row.worker_id),
                    expires_at: row.expires_at,
                })
                .collect())
        })
    }
}

// =========================================================================
// Join Token Store Implementation
// =========================================================================

impl JoinTokenStore for PostgresStore {
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

            sqlx::query!(r#"INSERT INTO join_tokens (token) VALUES ($1)"#, token)
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

impl LeadershipStore for PostgresStore {
    fn try_acquire<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let expires_at = OffsetDateTime::now_utc() + ttl;
            let now = OffsetDateTime::now_utc();

            let row = sqlx::query!(
                r#"
                INSERT INTO leadership (id, holder_id, expires_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE
                SET holder_id = $2, expires_at = $3
                WHERE leadership.expires_at < $4
                RETURNING holder_id
                "#,
                LEADERSHIP_ROW_ID,
                holder_id,
                expires_at,
                now,
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
            let expires_at = OffsetDateTime::now_utc() + ttl;
            let now = OffsetDateTime::now_utc();

            let result = sqlx::query!(
                r#"
                UPDATE leadership
                SET expires_at = $1
                WHERE id = $2 AND holder_id = $3 AND expires_at > $4
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
                r#"DELETE FROM leadership WHERE id = $1 AND holder_id = $2"#,
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
                r#"SELECT holder_id FROM leadership WHERE id = $1 AND expires_at > $2"#,
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

impl PeerStore for PostgresStore {
    fn register<'a>(
        &'a self,
        id: &'a str,
        grpc_address: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            sqlx::query!(
                r#"
                INSERT INTO control_plane_instances (id, grpc_address, last_heartbeat)
                VALUES ($1, $2, now())
                ON CONFLICT (id) DO UPDATE
                SET grpc_address = $2, last_heartbeat = now()
                "#,
                id,
                grpc_address,
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
            sqlx::query!(
                r#"UPDATE control_plane_instances SET last_heartbeat = now() WHERE id = $1"#,
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
            sqlx::query!(r#"DELETE FROM control_plane_instances WHERE id = $1"#, id)
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
                r#"SELECT id, grpc_address FROM control_plane_instances WHERE last_heartbeat > $1"#,
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
                r#"DELETE FROM control_plane_instances WHERE last_heartbeat < $1 RETURNING id"#,
                cutoff,
            )
            .fetch_all(&self.pool)
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
