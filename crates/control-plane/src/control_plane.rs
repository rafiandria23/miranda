use miranda_core::{
    event::{Event, EventPayload},
    execution::{Execution, TaskStatus},
    id::{ExecutionId, WorkerId, WorkflowId, WorkflowTaskId, WorkflowVersionId},
    queue::QueuedTask,
    router::WorkerRegistration,
    workflow::WorkflowDefinition,
};
use miranda_engine::{
    error::EngineError,
    retry::RetryPolicy,
    task_runner::{TaskOutcome, TaskOutcomeResult},
};
use miranda_storage::{
    error::StorageError, join_token_store::JoinTokenStore, lease_store::LeaseStore,
    workflow_store::WorkflowStore,
};
use miranda_worker::{assignment::TaskAssignment, error::WorkerError};
use std::{collections::HashMap, sync::Arc};
use time::{Duration, OffsetDateTime};

use crate::{
    dispatcher::DispatchStrategy,
    error::ControlPlaneError,
    lease_manager::{DEFAULT_LEASE_TTL, LeaseManager},
    notifier::TaskNotifier,
    queue::TaskQueue,
    router::{DEFAULT_WORKER_STALENESS_THRESHOLD, Router},
};

pub struct ControlPlane<Q, R, S, D, N, L> {
    queue: Q,
    router: R,
    store: S,
    dispatch: D,
    notifier: N,
    leases: LeaseManager<L>,
    join_tokens: Option<Arc<dyn JoinTokenStore>>,
    retry_policy: RetryPolicy,
    lease_ttl: Duration,
    worker_staleness_threshold: Duration,
}

impl<Q, R, S, D, N, L> ControlPlane<Q, R, S, D, N, L>
where
    Q: TaskQueue,
    R: Router,
    S: WorkflowStore,
    D: DispatchStrategy,
    N: TaskNotifier,
    L: LeaseStore,
{
    pub fn new(
        queue: Q,
        router: R,
        store: S,
        dispatch: D,
        notifier: N,
        leases: LeaseManager<L>,
    ) -> Self {
        Self {
            queue,
            router,
            store,
            dispatch,
            notifier,
            leases,
            join_tokens: None,
            retry_policy: RetryPolicy::default(),
            lease_ttl: DEFAULT_LEASE_TTL,
            worker_staleness_threshold: DEFAULT_WORKER_STALENESS_THRESHOLD,
        }
    }

    pub fn with_join_tokens(mut self, store: Arc<dyn JoinTokenStore>) -> Self {
        self.join_tokens = Some(store);
        self
    }

    pub async fn validate_join_token(&self, token: &str) -> Result<(), ControlPlaneError> {
        let Some(store) = &self.join_tokens else {
            return Ok(());
        };

        let expected = store.get_token().await?;

        match expected {
            Some(expected) if expected == token => Ok(()),

            _ => Err(ControlPlaneError::InvalidRequest(
                "invalid join token".to_string(),
            )),
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn with_lease_ttl(mut self, lease_ttl: Duration) -> Self {
        self.lease_ttl = lease_ttl;
        self
    }

    pub fn with_worker_staleness_threshold(mut self, worker_staleness_threshold: Duration) -> Self {
        self.worker_staleness_threshold = worker_staleness_threshold;
        self
    }

    async fn resolve_and_enqueue_ready(
        &self,
        mut execution: Execution,
        mut version: u64,
        definition: &Arc<WorkflowDefinition>,
    ) -> Result<(Execution, u64), ControlPlaneError> {
        self.store.update_execution(&execution, version).await?;
        version += 1;

        loop {
            let ready = execution.ready_tasks(definition);

            let (noop_ready, dispatchable_ready): (Vec<_>, Vec<_>) =
                ready.into_iter().partition(|t_id| {
                    definition
                        .task(*t_id)
                        .map(|t| t.task_type() == "noop")
                        .unwrap_or(false)
                });

            for workflow_task_id in &dispatchable_ready {
                let task = QueuedTask::new(execution.id(), *workflow_task_id);
                self.queue.enqueue(task, definition.clone()).await?;
            }

            if !dispatchable_ready.is_empty() {
                self.notifier.notify_ready().await;
            }

            if noop_ready.is_empty() {
                if dispatchable_ready.is_empty()
                    && execution
                        .tasks()
                        .iter()
                        .all(|t| t.status() == TaskStatus::Completed)
                {
                    execution.apply(
                        Event::new(execution.id(), EventPayload::ExecutionCompleted),
                        definition,
                    )?;

                    self.store.update_execution(&execution, version).await?;
                    version += 1;
                }

                return Ok((execution, version));
            }

            for workflow_task_id in noop_ready {
                execution.apply(
                    Event::new(
                        execution.id(),
                        EventPayload::TaskStarted { workflow_task_id },
                    ),
                    definition,
                )?;
                execution.apply(
                    Event::new(
                        execution.id(),
                        EventPayload::TaskCompleted { workflow_task_id },
                    ),
                    definition,
                )?;
            }

            self.store.update_execution(&execution, version).await?;
            version += 1;
        }
    }

    pub async fn register_workflow(
        &self,
        workflow_id: WorkflowId,
        name: &str,
        definition: &WorkflowDefinition,
    ) -> Result<WorkflowVersionId, ControlPlaneError> {
        const MAX_RETRIES: u32 = 6;

        for attempt in 0..MAX_RETRIES {
            let existing_versions = self.store.get_versions(workflow_id).await?;
            let next_version = existing_versions.len() as u64 + 1;
            let version_id = WorkflowVersionId::new();

            match self
                .store
                .save_definition(workflow_id, name, version_id, next_version, definition)
                .await
            {
                Ok(()) => return Ok(version_id),
                Err(StorageError::Conflict(_)) if attempt < MAX_RETRIES => {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(ControlPlaneError::InvalidRequest(format!(
            "failed to register workflow {workflow_id} after {MAX_RETRIES} attempts (concurrent registration contention)"
        )))
    }

    pub async fn submit_execution(
        &self,
        mut execution: Execution,
        definition: WorkflowDefinition,
    ) -> Result<(), ControlPlaneError> {
        execution.apply(
            Event::new(execution.id(), EventPayload::ExecutionStarted),
            &definition,
        )?;

        self.store.save_execution(&execution).await?;

        let definition = Arc::new(definition);

        self.resolve_and_enqueue_ready(execution, 1, &definition)
            .await?;

        Ok(())
    }

    pub async fn poll_task(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<TaskAssignment>, ControlPlaneError> {
        loop {
            let Some((task, definition)) = self.dispatch.next(worker_id).await? else {
                return Ok(None);
            };

            let (mut execution, version) = self.store.get_execution(task.execution_id()).await?;

            let task_status = execution
                .task(task.workflow_task_id())
                .ok_or_else(|| {
                    ControlPlaneError::InvalidRequest("task not found in execution".to_string())
                })?
                .status();

            let payload = match task_status {
                TaskStatus::Pending => EventPayload::TaskStarted {
                    workflow_task_id: task.workflow_task_id(),
                },
                TaskStatus::Failed => EventPayload::TaskRetried {
                    workflow_task_id: task.workflow_task_id(),
                },
                _stale => {
                    continue;
                }
            };

            execution.apply(Event::new(execution.id(), payload), &definition)?;
            self.store.update_execution(&execution, version).await?;

            let lease_token = self
                .leases
                .create(
                    task.execution_id(),
                    task.workflow_task_id(),
                    worker_id,
                    self.lease_ttl,
                )
                .await?;

            let workflow_task = definition
                .task(task.workflow_task_id())
                .expect("dequeued task must exist in its own definition")
                .clone();

            let timeout = definition.effective_timeout(&workflow_task);

            return Ok(Some(TaskAssignment {
                execution_id: task.execution_id(),
                lease_token,
                task: workflow_task,
                timeout,
            }));
        }
    }

    pub async fn report_result(
        &self,
        worker_id: WorkerId,
        lease_token: String,
        result: Result<(), WorkerError>,
    ) -> Result<(), ControlPlaneError> {
        let lease = self.leases.validate(&lease_token, worker_id).await?;

        const MAX_RETRIES: u32 = 6;

        for attempt in 0..MAX_RETRIES {
            let (mut execution, version) = self.store.get_execution(lease.execution_id).await?;
            let definition = Arc::new(
                self.store
                    .get_definition(execution.workflow_version_id())
                    .await?,
            );

            let outcome = TaskOutcome::new(&self.retry_policy);
            let outcome_result = outcome
                .apply(
                    &mut execution,
                    &definition,
                    lease.workflow_task_id,
                    result.clone(),
                )
                .await;

            let write_result: Result<(), ControlPlaneError> = match &outcome_result {
                Ok(TaskOutcomeResult::Completed) => self
                    .resolve_and_enqueue_ready(execution.clone(), version, &definition)
                    .await
                    .map(|_| ()),

                Ok(TaskOutcomeResult::Retried) | Err(EngineError::ExecutionFailed(_)) => self
                    .store
                    .update_execution(&execution, version)
                    .await
                    .map_err(Into::into),

                Err(_other) => Ok(()),
            };

            match write_result {
                Ok(()) => {
                    match outcome_result {
                        Ok(TaskOutcomeResult::Completed) => {
                            self.leases.release(&lease.token).await?;
                        }

                        Ok(TaskOutcomeResult::Retried) => {
                            self.leases.release(&lease.token).await?;

                            let task = QueuedTask::new(lease.execution_id, lease.workflow_task_id);
                            self.queue.enqueue(task, definition).await?;
                        }

                        Err(EngineError::ExecutionFailed(_)) => {
                            self.leases.release(&lease.token).await?;
                        }

                        Err(other) => {
                            return Err(ControlPlaneError::from(other));
                        }
                    }

                    return Ok(());
                }

                Err(ControlPlaneError::Storage(StorageError::OptimisticLockFailed { .. }))
                    if attempt < MAX_RETRIES =>
                {
                    continue;
                }

                Err(e) => return Err(e),
            }
        }

        Err(ControlPlaneError::InvalidRequest(format!(
            "report_result for task {:?} failed after {MAX_RETRIES} retries (concurrent write contention)",
            lease.workflow_task_id
        )))
    }

    pub async fn heartbeat(
        &self,
        worker_id: WorkerId,
        active_leases: &[String],
    ) -> Result<(), ControlPlaneError> {
        self.router.touch(worker_id).await?;

        for token in active_leases {
            let _ = self.leases.renew(token, self.lease_ttl).await;
        }

        Ok(())
    }

    pub async fn register_worker(
        &self,
        worker_id: WorkerId,
        capabilities: Vec<String>,
    ) -> Result<(), ControlPlaneError> {
        let registration =
            WorkerRegistration::new(worker_id, capabilities, OffsetDateTime::now_utc());

        self.router.register(registration).await
    }

    pub async fn deregister_worker(&self, worker_id: WorkerId) -> Result<(), ControlPlaneError> {
        self.router.deregister(worker_id).await
    }

    pub async fn reap_expired_leases(&self) -> Result<usize, ControlPlaneError> {
        let expired = self.leases.reap_expired().await;

        if expired.is_empty() {
            return Ok(0);
        }

        let mut by_execution: HashMap<ExecutionId, Vec<WorkflowTaskId>> = HashMap::new();

        for lease in &expired {
            by_execution
                .entry(lease.execution_id)
                .or_default()
                .push(lease.workflow_task_id);
        }

        let mut recovered_count = 0;

        for (execution_id, _leased_task_ids) in by_execution {
            let (mut execution, version) = self.store.get_execution(execution_id).await?;
            let definition = Arc::new(
                self.store
                    .get_definition(execution.workflow_version_id())
                    .await?,
            );

            let recovered_ids = execution.recover_abandoned_tasks(&definition)?;
            recovered_count += recovered_ids.len();

            self.store.update_execution(&execution, version).await?;

            for workflow_task_id in recovered_ids {
                let task = QueuedTask::new(execution_id, workflow_task_id);
                self.queue.enqueue(task, definition.clone()).await?;
            }
        }

        Ok(recovered_count)
    }

    pub async fn reap_stale_workers(&self) -> Result<usize, ControlPlaneError> {
        let stale = self
            .router
            .reap_stale(self.worker_staleness_threshold)
            .await;

        Ok(stale.len())
    }

    pub async fn get_execution(
        &self,
        execution_id: ExecutionId,
    ) -> Result<(Execution, u64), ControlPlaneError> {
        self.store
            .get_execution(execution_id)
            .await
            .map_err(ControlPlaneError::from)
    }

    pub async fn get_definition(
        &self,
        version_id: WorkflowVersionId,
    ) -> Result<WorkflowDefinition, ControlPlaneError> {
        self.store
            .get_definition(version_id)
            .await
            .map_err(ControlPlaneError::from)
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::{execution::ExecutionStatus, id::WorkflowId, workflow::WorkflowTask};
    use miranda_storage::InMemoryStore;

    use crate::{
        dispatcher::RoutedDispatcher, notifier::NullTaskNotifier, queue::InMemoryTaskQueue,
        router::InMemoryRouter,
    };

    use super::*;

    type TestControlPlane = ControlPlane<
        InMemoryTaskQueue,
        InMemoryRouter,
        InMemoryStore,
        RoutedDispatcher<InMemoryTaskQueue, InMemoryRouter>,
        NullTaskNotifier,
        InMemoryStore,
    >;

    fn control_plane() -> TestControlPlane {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let store = InMemoryStore::new();
        let dispatch = RoutedDispatcher::new(queue.clone(), router.clone());
        let leases = LeaseManager::new(InMemoryStore::new());

        ControlPlane::new(queue, router, store, dispatch, NullTaskNotifier, leases)
    }

    fn definition_with_task(task_type: &str) -> (WorkflowDefinition, WorkflowTaskId) {
        let task = WorkflowTask::new(WorkflowTaskId::new(), task_type.to_string(), Vec::new())
            .expect("valid task");
        let task_id = task.id();
        let definition = WorkflowDefinition::new(vec![task]).expect("valid definition");

        (definition, task_id)
    }

    #[tokio::test]
    async fn register_workflow_persists_the_first_version() {
        let control_plane = control_plane();
        let (definition, _) = definition_with_task("send_email");
        let workflow_id = WorkflowId::new();

        let version_id = control_plane
            .register_workflow(workflow_id, "wf", &definition)
            .await
            .expect("register succeeds");

        let fetched = control_plane
            .get_definition(version_id)
            .await
            .expect("get_definition succeeds");

        assert_eq!(fetched, definition);
    }

    #[tokio::test]
    async fn poll_task_returns_none_when_nothing_is_queued() {
        let control_plane = control_plane();

        let result = control_plane
            .poll_task(WorkerId::new())
            .await
            .expect("poll succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn submit_execution_then_poll_task_dispatches_the_ready_task_to_a_capable_worker() {
        let control_plane = control_plane();
        let (definition, task_id) = definition_with_task("send_email");
        let workflow_id = WorkflowId::new();

        let version_id = control_plane
            .register_workflow(workflow_id, "wf", &definition)
            .await
            .expect("register succeeds");

        let execution =
            Execution::from_definition(version_id, &definition).expect("valid execution");
        let execution_id = execution.id();

        control_plane
            .submit_execution(execution, definition)
            .await
            .expect("submit succeeds");

        let worker_id = WorkerId::new();
        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .expect("register_worker succeeds");

        let assignment = control_plane
            .poll_task(worker_id)
            .await
            .expect("poll succeeds")
            .expect("task is assigned");

        assert_eq!(assignment.task.id(), task_id);

        let (execution, _) = control_plane
            .get_execution(execution_id)
            .await
            .expect("get_execution succeeds");

        assert_eq!(
            execution.task(task_id).expect("task present").status(),
            TaskStatus::Running
        );
    }

    #[tokio::test]
    async fn report_result_completing_the_only_task_completes_the_execution() {
        let control_plane = control_plane();
        let (definition, task_id) = definition_with_task("send_email");
        let workflow_id = WorkflowId::new();

        let version_id = control_plane
            .register_workflow(workflow_id, "wf", &definition)
            .await
            .expect("register succeeds");

        let execution =
            Execution::from_definition(version_id, &definition).expect("valid execution");
        let execution_id = execution.id();

        control_plane
            .submit_execution(execution, definition)
            .await
            .expect("submit succeeds");

        let worker_id = WorkerId::new();
        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .expect("register_worker succeeds");

        let assignment = control_plane
            .poll_task(worker_id)
            .await
            .expect("poll succeeds")
            .expect("task is assigned");

        control_plane
            .report_result(worker_id, assignment.lease_token, Ok(()))
            .await
            .expect("report_result succeeds");

        let (execution, _) = control_plane
            .get_execution(execution_id)
            .await
            .expect("get_execution succeeds");

        assert_eq!(
            execution.task(task_id).expect("task present").status(),
            TaskStatus::Completed
        );
        assert_eq!(execution.status(), ExecutionStatus::Completed);
    }

    #[tokio::test]
    async fn heartbeat_touches_the_worker_so_it_is_not_reaped() {
        let control_plane = control_plane();
        let worker_id = WorkerId::new();

        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .expect("register_worker succeeds");

        control_plane
            .heartbeat(worker_id, &[])
            .await
            .expect("heartbeat succeeds");

        let reaped = control_plane
            .reap_stale_workers()
            .await
            .expect("reap succeeds");

        assert_eq!(reaped, 0);
    }

    #[tokio::test]
    async fn deregister_worker_prevents_future_dispatch_to_it() {
        let control_plane = control_plane();
        let worker_id = WorkerId::new();

        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .expect("register_worker succeeds");

        control_plane
            .deregister_worker(worker_id)
            .await
            .expect("deregister_worker succeeds");

        let (definition, _) = definition_with_task("send_email");
        let workflow_id = WorkflowId::new();
        let version_id = control_plane
            .register_workflow(workflow_id, "wf", &definition)
            .await
            .expect("register succeeds");
        let execution =
            Execution::from_definition(version_id, &definition).expect("valid execution");

        control_plane
            .submit_execution(execution, definition)
            .await
            .expect("submit succeeds");

        let result = control_plane
            .poll_task(worker_id)
            .await
            .expect("poll succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn validate_join_token_allows_any_token_when_no_store_is_configured() {
        let control_plane = control_plane();

        control_plane
            .validate_join_token("anything")
            .await
            .expect("validation succeeds without a configured store");
    }

    #[tokio::test]
    async fn reap_expired_leases_returns_zero_when_no_leases_exist() {
        let control_plane = control_plane();

        let recovered = control_plane
            .reap_expired_leases()
            .await
            .expect("reap succeeds");

        assert_eq!(recovered, 0);
    }
}
