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

type DefinitionsMap = HashMap<WorkflowVersionId, (WorkflowId, u64, WorkflowDefinition)>;

#[derive(Default, Clone)]
pub struct InMemoryStore {
    definitions: Arc<RwLock<DefinitionsMap>>,
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

    fn reap_stale<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, StorageError>> + Send + 'a>> {
        Box::pin(async move {
            let mut peers = self.peers.write().await;
            let now = OffsetDateTime::now_utc();

            let stale_ids: Vec<String> = peers
                .iter()
                .filter(|(_, (_, last_heartbeat))| now - *last_heartbeat >= threshold)
                .map(|(id, _)| id.clone())
                .collect();

            for id in &stale_ids {
                peers.remove(id);
            }

            Ok(stale_ids)
        })
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{
        id::WorkflowTaskId,
        workflow::{WorkflowDefinition, WorkflowTask},
    };

    use super::*;

    fn worker(capabilities: &[&str]) -> WorkerRegistration {
        WorkerRegistration::new(
            WorkerId::new(),
            capabilities.iter().map(|c| c.to_string()).collect(),
            OffsetDateTime::now_utc(),
        )
    }

    fn lease(token: &str, expires_in: Duration) -> Lease {
        Lease {
            worker_id: WorkerId::new(),
            workflow_task_id: WorkflowTaskId::new(),
            execution_id: ExecutionId::new(),
            token: token.to_owned(),
            expires_at: OffsetDateTime::now_utc() + expires_in,
        }
    }

    // ---------------------------------------------------------------
    // TaskQueueStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn dequeue_on_empty_queue_returns_none() {
        let store = InMemoryStore::new();

        assert!(store.dequeue().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn enqueue_then_dequeue_returns_fifo_order() {
        let store = InMemoryStore::new();
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
        let store = InMemoryStore::new();
        let reg = worker(&["email"]);
        let id = reg.id();

        store.register_worker(reg).await.unwrap();

        assert_eq!(store.select_worker("email").await.unwrap(), Some(id));
        assert_eq!(store.select_worker("sms").await.unwrap(), None);
    }

    #[tokio::test]
    async fn worker_has_capability_reflects_registration() {
        let store = InMemoryStore::new();
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
        let store = InMemoryStore::new();
        let reg = worker(&["email"]);
        let id = reg.id();

        store.register_worker(reg).await.unwrap();
        store.deregister_worker(id).await.unwrap();

        assert_eq!(store.select_worker("email").await.unwrap(), None);
    }

    #[tokio::test]
    async fn touch_worker_updates_heartbeat() {
        let store = InMemoryStore::new();
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
    async fn touch_worker_on_unknown_id_is_ok() {
        let store = InMemoryStore::new();

        assert!(store.touch_worker(WorkerId::new()).await.is_ok());
    }

    #[tokio::test]
    async fn reap_stale_workers_removes_only_stale_entries() {
        let store = InMemoryStore::new();
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
        assert!(!store.worker_has_capability(stale_id, "").await.unwrap());
    }

    // ---------------------------------------------------------------
    // WorkflowStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn save_and_get_definition_roundtrips() {
        let store = InMemoryStore::new();
        let workflow_id = WorkflowId::new();
        let version_id = WorkflowVersionId::new();
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        store
            .save_definition(workflow_id, "wf", version_id, 1, &definition)
            .await
            .unwrap();

        let loaded = store.get_definition(version_id).await.unwrap();

        assert_eq!(loaded, definition);
    }

    #[tokio::test]
    async fn get_definition_missing_returns_not_found() {
        let store = InMemoryStore::new();
        let version_id = WorkflowVersionId::new();

        let err = store.get_definition(version_id).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::WorkflowVersionNotFound(id) if id == version_id
        ));
    }

    #[tokio::test]
    async fn get_versions_returns_only_matching_workflow_id() {
        let store = InMemoryStore::new();
        let workflow_a = WorkflowId::new();
        let workflow_b = WorkflowId::new();
        let version_a = WorkflowVersionId::new();
        let version_b = WorkflowVersionId::new();
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        store
            .save_definition(workflow_a, "a", version_a, 1, &definition)
            .await
            .unwrap();
        store
            .save_definition(workflow_b, "b", version_b, 1, &definition)
            .await
            .unwrap();

        let versions = store.get_versions(workflow_a).await.unwrap();

        assert_eq!(versions, vec![version_a]);
    }

    #[tokio::test]
    async fn save_and_get_execution_roundtrips_with_version_one() {
        let store = InMemoryStore::new();
        let execution = Execution::new(WorkflowVersionId::new());

        store.save_execution(&execution).await.unwrap();

        let (loaded, version) = store.get_execution(execution.id()).await.unwrap();

        assert_eq!(loaded.id(), execution.id());
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn get_execution_missing_returns_not_found() {
        let store = InMemoryStore::new();
        let execution_id = ExecutionId::new();

        let err = store.get_execution(execution_id).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::ExecutionNotFound(id) if id == execution_id
        ));
    }

    #[tokio::test]
    async fn update_execution_bumps_version_on_expected_match() {
        let store = InMemoryStore::new();
        let execution = Execution::new(WorkflowVersionId::new());

        store.save_execution(&execution).await.unwrap();
        store.update_execution(&execution, 1).await.unwrap();

        let (_, version) = store.get_execution(execution.id()).await.unwrap();

        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn update_execution_missing_returns_not_found() {
        let store = InMemoryStore::new();
        let execution = Execution::new(WorkflowVersionId::new());

        let err = store.update_execution(&execution, 1).await.unwrap_err();

        assert!(matches!(
            err,
            StorageError::ExecutionNotFound(id) if id == execution.id()
        ));
    }

    #[tokio::test]
    async fn update_execution_with_stale_version_fails_optimistic_lock() {
        let store = InMemoryStore::new();
        let execution = Execution::new(WorkflowVersionId::new());

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
        let store = InMemoryStore::new();
        let task = WorkflowTask::new(WorkflowTaskId::new(), "noop".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();
        let active = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        let finished = Execution::new(WorkflowVersionId::new());

        store.save_execution(&active).await.unwrap();
        store.save_execution(&finished).await.unwrap();

        let active_executions = store.get_active_executions().await.unwrap();

        assert!(active_executions.iter().any(|e| e.id() == active.id()));
    }

    // ---------------------------------------------------------------
    // LeaseStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn create_then_get_lease_roundtrips() {
        let store = InMemoryStore::new();
        let l = lease("token-1", Duration::minutes(5));

        store.create(l.clone()).await.unwrap();

        let loaded = store.get("token-1").await.unwrap().unwrap();

        assert_eq!(loaded.token, l.token);
    }

    #[tokio::test]
    async fn get_missing_lease_returns_none() {
        let store = InMemoryStore::new();

        assert!(store.get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn renew_updates_expiry() {
        let store = InMemoryStore::new();
        let l = lease("token-1", Duration::minutes(5));

        store.create(l).await.unwrap();

        let new_expiry = OffsetDateTime::now_utc() + Duration::hours(1);
        LeaseStore::renew(&store, "token-1", new_expiry)
            .await
            .unwrap();

        let loaded = store.get("token-1").await.unwrap().unwrap();

        assert_eq!(loaded.expires_at, new_expiry);
    }

    #[tokio::test]
    async fn renew_on_unknown_token_is_ok() {
        let store = InMemoryStore::new();

        assert!(
            LeaseStore::renew(&store, "missing", OffsetDateTime::now_utc())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn release_removes_lease() {
        let store = InMemoryStore::new();
        let l = lease("token-1", Duration::minutes(5));

        store.create(l).await.unwrap();
        LeaseStore::release(&store, "token-1").await.unwrap();

        assert!(store.get("token-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn active_for_worker_excludes_expired_and_other_workers() {
        let store = InMemoryStore::new();
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
        let store = InMemoryStore::new();
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
        let store = InMemoryStore::new();

        assert!(store.get_token().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_then_get_token_roundtrips() {
        let store = InMemoryStore::new();

        store.set_token("secret").await.unwrap();

        assert_eq!(store.get_token().await.unwrap(), Some("secret".to_string()));
    }

    #[tokio::test]
    async fn set_token_overwrites_previous_value() {
        let store = InMemoryStore::new();

        store.set_token("first").await.unwrap();
        store.set_token("second").await.unwrap();

        assert_eq!(store.get_token().await.unwrap(), Some("second".to_string()));
    }

    // ---------------------------------------------------------------
    // LeadershipStore
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn try_acquire_succeeds_when_unheld() {
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

        store
            .try_acquire("node-1", Duration::minutes(1))
            .await
            .unwrap();
        LeadershipStore::release(&store, "node-1").await.unwrap();

        assert_eq!(store.current_holder().await.unwrap(), None);
    }

    #[tokio::test]
    async fn release_is_noop_for_non_holder() {
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();

        let peers = store.list_active(Duration::minutes(1)).await.unwrap();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer-1");
        assert_eq!(peers[0].grpc_address, "127.0.0.1:9000");
    }

    #[tokio::test]
    async fn list_active_excludes_stale_peers() {
        let store = InMemoryStore::new();

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        store.peers.write().await.insert(
            "peer-2".to_string(),
            (
                "127.0.0.1:9001".to_string(),
                OffsetDateTime::now_utc() - Duration::minutes(10),
            ),
        );

        let peers = store.list_active(Duration::minutes(1)).await.unwrap();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer-1");
    }

    #[tokio::test]
    async fn touch_updates_last_heartbeat() {
        let store = InMemoryStore::new();

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        store.peers.write().await.get_mut("peer-1").unwrap().1 =
            OffsetDateTime::now_utc() - Duration::minutes(10);

        store.touch("peer-1").await.unwrap();

        let peers = store.list_active(Duration::minutes(1)).await.unwrap();

        assert_eq!(peers.len(), 1);
    }

    #[tokio::test]
    async fn touch_on_unknown_peer_is_ok() {
        let store = InMemoryStore::new();

        assert!(store.touch("missing").await.is_ok());
    }

    #[tokio::test]
    async fn deregister_removes_peer() {
        let store = InMemoryStore::new();

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
        let store = InMemoryStore::new();

        store.register("peer-1", "127.0.0.1:9000").await.unwrap();
        store.peers.write().await.insert(
            "peer-2".to_string(),
            (
                "127.0.0.1:9001".to_string(),
                OffsetDateTime::now_utc() - Duration::minutes(10),
            ),
        );

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
