use miranda_core::{
    execution::Execution,
    id::{ExecutionId, WorkerId, WorkflowId, WorkflowVersionId},
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
    error::StorageError, router_store::RouterStore, task_queue_store::TaskQueueStore,
    workflow_store::WorkflowStore,
};

#[derive(Default, Clone)]
pub struct MemoryStore {
    definitions: Arc<RwLock<HashMap<WorkflowVersionId, (WorkflowId, u64, WorkflowDefinition)>>>,
    executions: Arc<RwLock<HashMap<ExecutionId, (Execution, u64)>>>,
    queue: Arc<RwLock<VecDeque<QueuedTask>>>,
    workers: Arc<RwLock<HashMap<WorkerId, WorkerRegistration>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

// =========================================================================
// Queue Store Implementation
// =========================================================================

impl TaskQueueStore for MemoryStore {
    async fn enqueue(&self, task: QueuedTask) -> Result<(), StorageError> {
        self.queue.write().await.push_back(task);

        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<QueuedTask>, StorageError> {
        Ok(self.queue.write().await.pop_front())
    }
}

// =========================================================================
// Router Store Implementation
// =========================================================================

impl RouterStore for MemoryStore {
    async fn register_worker(&self, registration: WorkerRegistration) -> Result<(), StorageError> {
        self.workers
            .write()
            .await
            .insert(registration.id(), registration);

        Ok(())
    }

    async fn deregister_worker(&self, id: WorkerId) -> Result<(), StorageError> {
        self.workers.write().await.remove(&id);

        Ok(())
    }

    async fn touch_worker(&self, id: WorkerId) -> Result<(), StorageError> {
        if let Some(reg) = self.workers.write().await.get_mut(&id) {
            *reg = reg.clone().with_heartbeat(OffsetDateTime::now_utc());
        }

        Ok(())
    }

    async fn select_worker(&self, capability: &str) -> Result<Option<WorkerId>, StorageError> {
        Ok(self
            .workers
            .read()
            .await
            .values()
            .find(|reg| reg.has_capability(capability))
            .map(|reg| reg.id()))
    }

    async fn worker_has_capability(
        &self,
        id: WorkerId,
        capability: &str,
    ) -> Result<bool, StorageError> {
        Ok(self
            .workers
            .read()
            .await
            .get(&id)
            .is_some_and(|reg| reg.has_capability(capability)))
    }

    async fn reap_stale_workers(&self, threshold: Duration) -> Result<Vec<WorkerId>, StorageError> {
        let mut workers = self.workers.write().await;

        let stale: Vec<WorkerId> = workers
            .values()
            .filter(|reg| reg.is_stale(threshold))
            .map(|reg| reg.id())
            .collect();

        for id in &stale {
            workers.remove(&id);
        }

        Ok(stale)
    }
}

// =========================================================================
// Workflow Store Implementation
// =========================================================================

impl WorkflowStore for MemoryStore {
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
