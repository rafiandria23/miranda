use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkerId, WorkflowId, WorkflowVersionId},
    lease::Lease,
    queue::QueuedTask,
    router::WorkerRegistration,
    workflow::WorkflowDefinition,
};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use time::{Duration, OffsetDateTime};
use tokio::sync::RwLock;

use crate::{
    error::StorageError,
    join_token_store::JoinTokenStore,
    leadership_store::LeadershipStore,
    lease_store::LeaseStore,
    peer_store::{PeerInfo, PeerStore},
    router_store::RouterStore,
    task_queue_store::TaskQueueStore,
    workflow_store::WorkflowStore,
};

#[derive(Default, Clone)]
pub struct InMemoryStore {
    definitions: Arc<RwLock<HashMap<WorkflowVersionId, (WorkflowId, u64, WorkflowDefinition)>>>,
    executions: Arc<RwLock<HashMap<ExecutionId, (Execution, u64)>>>,
    queue: Arc<RwLock<VecDeque<QueuedTask>>>,
    workers: Arc<RwLock<HashMap<WorkerId, WorkerRegistration>>>,
    leases: Arc<RwLock<HashMap<String, Lease>>>,
    join_token: Arc<RwLock<Option<String>>>,
    leadership: Arc<RwLock<Option<(String, OffsetDateTime)>>>,
    peers: Arc<RwLock<HashMap<String, (String, OffsetDateTime)>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

// =========================================================================
// Queue Store Implementation
// =========================================================================

impl TaskQueueStore for InMemoryStore {
    fn enqueue<'a>(
        &'a self,
        task: QueuedTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.queue.write().await.push_back(task);

            Ok(())
        })
    }

    fn dequeue<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<QueuedTask>, StorageError>> + Send + 'a>> {
        Box::pin(async move { Ok(self.queue.write().await.pop_front()) })
    }
}

// =========================================================================
// Router Store Implementation
// =========================================================================

impl RouterStore for InMemoryStore {
    fn register_worker<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.workers
                .write()
                .await
                .insert(registration.id(), registration);

            Ok(())
        })
    }

    fn deregister_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.workers.write().await.remove(&id);

            Ok(())
        })
    }

    fn touch_worker<'a>(
        &'a self,
        id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(reg) = self.workers.write().await.get_mut(&id) {
                *reg = reg.clone().with_heartbeat(OffsetDateTime::now_utc());
            }

            Ok(())
        })
    }

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<WorkerId>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .workers
                .read()
                .await
                .values()
                .find(|reg| reg.has_capability(capability))
                .map(|reg| reg.id()))
        })
    }

    fn worker_has_capability<'a>(
        &'a self,
        id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .workers
                .read()
                .await
                .get(&id)
                .is_some_and(|reg| reg.has_capability(capability)))
        })
    }

    fn reap_stale_workers<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkerId>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut workers = self.workers.write().await;

            let stale: Vec<WorkerId> = workers
                .values()
                .filter(|reg| reg.is_stale(threshold))
                .map(|reg| reg.id())
                .collect();

            for id in &stale {
                workers.remove(id);
            }

            Ok(stale)
        })
    }
}

// =========================================================================
// Workflow Store Implementation
// =========================================================================

impl WorkflowStore for InMemoryStore {
    fn save_definition<'a>(
        &'a self,
        workflow_id: WorkflowId,
        name: &'a str,
        version_id: WorkflowVersionId,
        version: u64,
        definition: &'a WorkflowDefinition,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let _ = name;

            let mut definitions = self.definitions.write().await;

            definitions.insert(version_id, (workflow_id, version, definition.clone()));

            Ok(())
        })
    }

    fn get_versions<'a>(
        &'a self,
        workflow_id: WorkflowId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WorkflowVersionId>, StorageError>> + Send + 'a>>
    {
        Box::pin(async move {
            let definitions = self.definitions.read().await;

            let versions: Vec<_> = definitions
                .iter()
                .filter(|(_, (wf_id, _, _))| *wf_id == workflow_id)
                .map(|(version_id, _)| *version_id)
                .collect();

            Ok(versions)
        })
    }

    fn get_definition<'a>(
        &'a self,
        version_id: WorkflowVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<WorkflowDefinition, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let definitions = self.definitions.read().await;

            definitions
                .get(&version_id)
                .map(|(_, _, definition)| definition.clone())
                .ok_or(StorageError::WorkflowVersionNotFound(version_id))
        })
    }

    fn save_execution<'a>(
        &'a self,
        execution: &'a Execution,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut executions = self.executions.write().await;

            executions.insert(execution.id(), (execution.clone(), 1));

            Ok(())
        })
    }

    fn update_execution<'a>(
        &'a self,
        execution: &'a Execution,
        expected_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut executions = self.executions.write().await;

            let current_version = match executions.get(&execution.id()) {
                Some((_, version)) => *version,
                None => return Err(StorageError::ExecutionNotFound(execution.id())),
            };

            if current_version != expected_version {
                return Err(StorageError::OptimisticLockFailed {
                    id: execution.id(),
                    expected: expected_version,
                    actual: current_version,
                });
            }

            executions.insert(execution.id(), (execution.clone(), current_version + 1));

            Ok(())
        })
    }

    fn get_execution<'a>(
        &'a self,
        execution_id: ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<(Execution, u64), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let executions = self.executions.read().await;

            executions
                .get(&execution_id)
                .map(|(execution, version)| (execution.clone(), *version))
                .ok_or(StorageError::ExecutionNotFound(execution_id))
        })
    }

    fn get_active_executions<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Execution>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let executions = self.executions.read().await;

            let active_executions = executions
                .values()
                .map(|(execution, _)| execution.clone())
                .filter(|execution| !execution.is_finished())
                .collect();

            Ok(active_executions)
        })
    }
}

// =========================================================================
// Lease Store Implementation
// =========================================================================

impl LeaseStore for InMemoryStore {
    fn create<'a>(
        &'a self,
        lease: Lease,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.leases.write().await.insert(lease.token.clone(), lease);

            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Lease>, StorageError>> + Send + 'a>> {
        Box::pin(async move { Ok(self.leases.read().await.get(token).cloned()) })
    }

    fn renew<'a>(
        &'a self,
        token: &'a str,
        new_expires_at: OffsetDateTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(lease) = self.leases.write().await.get_mut(token) {
                lease.expires_at = new_expires_at;
            }

            Ok(())
        })
    }

    fn release<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.leases.write().await.remove(token);

            Ok(())
        })
    }

    fn active_for_worker<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .leases
                .read()
                .await
                .values()
                .filter(|l| l.worker_id == worker_id && !l.is_expired())
                .map(|l| l.token.clone())
                .collect())
        })
    }

    fn reap_expired<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Lease>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut leases = self.leases.write().await;

            let expired_tokens: Vec<String> = leases
                .values()
                .filter(|l| l.is_expired())
                .map(|l| l.token.clone())
                .collect();

            Ok(expired_tokens
                .into_iter()
                .filter_map(|token| leases.remove(&token))
                .collect())
        })
    }
}

// =========================================================================
// Join Token Store Implementation
// =========================================================================

impl JoinTokenStore for InMemoryStore {
    fn set_token<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            *self.join_token.write().await = Some(token.to_owned());

            Ok(())
        })
    }

    fn get_token<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move { Ok(self.join_token.read().await.clone()) })
    }
}

// =========================================================================
// Leadership Store Implementation
// =========================================================================

impl LeadershipStore for InMemoryStore {
    fn try_acquire<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut leadership = self.leadership.write().await;

            let now = OffsetDateTime::now_utc();

            let currently_held = leadership
                .as_ref()
                .is_some_and(|(_, expires_at)| *expires_at > now);

            if currently_held {
                return Ok(false);
            }

            *leadership = Some((holder_id.to_owned(), now + ttl));

            Ok(true)
        })
    }

    fn renew<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut leadership = self.leadership.write().await;

            let now = OffsetDateTime::now_utc();

            let is_current_holder = leadership
                .as_ref()
                .is_some_and(|(holder, expires_at)| holder == holder_id && *expires_at > now);

            if !is_current_holder {
                return Ok(false);
            }

            *leadership = Some((holder_id.to_owned(), now + ttl));

            Ok(true)
        })
    }

    fn release<'a>(
        &'a self,
        holder_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut leadership = self.leadership.write().await;

            let is_current_holder = leadership
                .as_ref()
                .is_some_and(|(holder, _)| holder == holder_id);

            if is_current_holder {
                *leadership = None;
            }

            Ok(())
        })
    }

    fn current_holder<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let leadership = self.leadership.read().await;

            let now = OffsetDateTime::now_utc();

            Ok(leadership
                .as_ref()
                .filter(|(_, expires_at)| *expires_at > now)
                .map(|(holder, _)| holder.clone()))
        })
    }
}

// =========================================================================
// Peer Store Implementation
// =========================================================================

impl PeerStore for InMemoryStore {
    fn register<'a>(
        &'a self,
        id: &'a str,
        grpc_address: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.peers.write().await.insert(
                id.to_owned(),
                (grpc_address.to_owned(), OffsetDateTime::now_utc()),
            );

            Ok(())
        })
    }

    fn touch<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(entry) = self.peers.write().await.get_mut(id) {
                entry.1 = OffsetDateTime::now_utc();
            }

            Ok(())
        })
    }

    fn deregister<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>> {
        Box::pin(async move {
            self.peers.write().await.remove(id);

            Ok(())
        })
    }

    fn list_active<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();

            Ok(self
                .peers
                .read()
                .await
                .iter()
                .filter(|(_, (_, last_heartbeat))| now - *last_heartbeat < threshold)
                .map(|(id, (grpc_address, _))| PeerInfo {
                    id: id.clone(),
                    grpc_address: grpc_address.clone(),
                })
                .collect())
        })
    }
}
