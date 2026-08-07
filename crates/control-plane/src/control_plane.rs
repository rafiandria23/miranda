use miranda_core::{
    event::{Event, EventPayload},
    execution::{Execution, TaskStatus},
    id::{ExecutionId, WorkerId, WorkflowId, WorkflowTaskId, WorkflowVersionId},
    workflow::WorkflowDefinition,
};
use miranda_engine::{
    EngineError,
    retry::RetryPolicy,
    task_runner::{TaskOutcome, TaskOutcomeResult},
};
use miranda_storage::WorkflowStore;
use miranda_worker::assignment::TaskAssignment;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    ControlPlaneError,
    dispatcher::DispatchStrategy,
    lease_manager::{DEFAULT_LEASE_TTL, LeaseManager, LeaseToken},
    queue::{QueueItem, TaskQueue},
    router::{DEFAULT_WORKER_STALENESS_THRESHOLD, Router, WorkerInfo},
};

pub struct ControlPlane<Q, R, S, D> {
    queue: Q,
    router: R,
    store: S,
    dispatch: D,
    leases: LeaseManager,
    retry_policy: RetryPolicy,
    lease_ttl: Duration,
    worker_staleness_threshold: Duration,
}

impl<Q, R, S, D> ControlPlane<Q, R, S, D>
where
    Q: TaskQueue,
    R: Router,
    S: WorkflowStore,
    D: DispatchStrategy,
{
    pub fn new(queue: Q, router: R, store: S, dispatch: D) -> Self {
        Self {
            queue,
            router,
            store,
            dispatch,
            leases: LeaseManager::new(),
            retry_policy: RetryPolicy::default(),
            lease_ttl: DEFAULT_LEASE_TTL,
            worker_staleness_threshold: DEFAULT_WORKER_STALENESS_THRESHOLD,
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
                self.queue
                    .enqueue(QueueItem {
                        execution_id: execution.id(),
                        workflow_task_id: *workflow_task_id,
                        definition: definition.clone(),
                    })
                    .await?;
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
        let existing_versions = self.store.get_versions(workflow_id).await?;
        let next_version = existing_versions.len() as u64 + 1;

        let version_id = WorkflowVersionId::new();

        self.store
            .save_definition(workflow_id, name, version_id, next_version, definition)
            .await?;

        Ok(version_id)
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

        let (execution, version) = self
            .resolve_and_enqueue_ready(execution, 1, &definition)
            .await?;

        self.store.update_execution(&execution, version).await?;

        Ok(())
    }

    pub async fn poll_task(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<TaskAssignment>, ControlPlaneError> {
        let Some(item) = self.dispatch.next(worker_id).await? else {
            return Ok(None);
        };

        let (mut execution, version) = self.store.get_execution(item.execution_id).await?;

        let task_status = execution
            .task(item.workflow_task_id)
            .ok_or_else(|| {
                ControlPlaneError::InvalidRequest("task not found in execution".to_string())
            })?
            .status();

        let payload = match task_status {
            TaskStatus::Pending => EventPayload::TaskStarted {
                workflow_task_id: item.workflow_task_id,
            },
            TaskStatus::Failed => EventPayload::TaskRetried {
                workflow_task_id: item.workflow_task_id,
            },
            other => {
                return Err(ControlPlaneError::InvalidRequest(format!(
                    "task in unexpected status for dispatch: {other:?}"
                )));
            }
        };

        execution.apply(Event::new(execution.id(), payload), &item.definition)?;
        self.store.update_execution(&execution, version).await?;

        let lease_token = self
            .leases
            .create(
                item.execution_id,
                item.workflow_task_id,
                worker_id,
                self.lease_ttl,
            )
            .await;

        let workflow_task = item
            .definition
            .task(item.workflow_task_id)
            .expect("dequeued task must exist in its own definition")
            .clone();

        let timeout = item.definition.effective_timeout(&workflow_task);

        Ok(Some(TaskAssignment {
            lease_token: lease_token.0,
            task: workflow_task,
            timeout,
        }))
    }

    pub async fn report_result(
        &self,
        worker_id: WorkerId,
        lease_token: String,
        result: Result<(), miranda_worker::WorkerError>,
    ) -> Result<(), ControlPlaneError> {
        let token = LeaseToken(lease_token);
        let lease = self.leases.validate(&token, worker_id).await?;

        let (mut execution, version) = self.store.get_execution(lease.execution_id).await?;
        let definition = Arc::new(
            self.store
                .get_definition(execution.workflow_version_id())
                .await?,
        );

        let outcome = TaskOutcome::new(&self.retry_policy);

        match outcome
            .apply(&mut execution, &definition, lease.workflow_task_id, result)
            .await
        {
            Ok(TaskOutcomeResult::Completed) => {
                let (execution, version) = self
                    .resolve_and_enqueue_ready(execution, version, &definition)
                    .await?;

                self.store.update_execution(&execution, version).await?;
                self.leases.release(&token).await?;
            }

            Ok(TaskOutcomeResult::Retried) => {
                self.store.update_execution(&execution, version).await?;
                self.leases.release(&token).await?;

                self.queue
                    .enqueue(QueueItem {
                        execution_id: lease.execution_id,
                        workflow_task_id: lease.workflow_task_id,
                        definition,
                    })
                    .await?;
            }

            Err(EngineError::ExecutionFailed(_)) => {
                self.store.update_execution(&execution, version).await?;
                self.leases.release(&token).await?;
            }

            Err(other) => {
                return Err(ControlPlaneError::from(other));
            }
        }

        Ok(())
    }

    pub async fn heartbeat(
        &self,
        worker_id: WorkerId,
        active_leases: &[String],
    ) -> Result<(), ControlPlaneError> {
        self.router.touch(worker_id).await?;

        for token_str in active_leases {
            let token = LeaseToken(token_str.clone());
            let _ = self.leases.renew(&token).await;
        }

        Ok(())
    }

    pub async fn register_worker(
        &self,
        worker_id: WorkerId,
        capabilities: Vec<String>,
    ) -> Result<(), ControlPlaneError> {
        self.router
            .register(WorkerInfo {
                id: worker_id,
                capabilities: capabilities.into_iter().collect(),
                last_heartbeat: Instant::now(),
            })
            .await
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
                self.queue
                    .enqueue(QueueItem {
                        execution_id,
                        workflow_task_id,
                        definition: definition.clone(),
                    })
                    .await?;
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

#[cfg(test)]
mod tests {
    use miranda_core::{execution::ExecutionStatus, id::WorkflowVersionId, workflow::WorkflowTask};
    use miranda_engine::retry::Backoff;
    use miranda_storage::MemoryStore;

    use crate::{dispatcher::Dispatcher, queue::InMemoryTaskQueue, router::InMemoryRouter};

    use super::*;

    type TestControlPlane =
        ControlPlane<InMemoryTaskQueue, InMemoryRouter, MemoryStore, Dispatcher<InMemoryTaskQueue>>;

    fn harness() -> (
        TestControlPlane,
        InMemoryTaskQueue,
        InMemoryRouter,
        MemoryStore,
    ) {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let store = MemoryStore::new();
        let dispatch = Dispatcher::new(queue.clone());

        let control_plane =
            ControlPlane::new(queue.clone(), router.clone(), store.clone(), dispatch);

        (control_plane, queue, router, store)
    }

    fn fast_retry_control_plane(
        max_attempts: u32,
    ) -> (
        TestControlPlane,
        InMemoryTaskQueue,
        InMemoryRouter,
        MemoryStore,
    ) {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let store = MemoryStore::new();
        let dispatch = Dispatcher::new(queue.clone());

        let control_plane =
            ControlPlane::new(queue.clone(), router.clone(), store.clone(), dispatch)
                .with_retry_policy(RetryPolicy::new(
                    max_attempts,
                    Backoff::Fixed(Duration::ZERO),
                ));

        (control_plane, queue, router, store)
    }

    fn single_task_definition() -> (WorkflowDefinition, miranda_core::id::WorkflowTaskId) {
        let task_id = miranda_core::id::WorkflowTaskId::new();
        let task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        (definition, task_id)
    }

    // `report_result` looks the definition back up from the store via
    // `workflow_version_id`, so tests exercising that path must persist it
    // themselves (unlike `submit_execution`, nothing else does this).
    async fn save_definition(
        store: &MemoryStore,
        workflow_version_id: WorkflowVersionId,
        definition: &WorkflowDefinition,
    ) {
        store
            .save_definition(
                miranda_core::id::WorkflowId::new(),
                "test_workflow",
                workflow_version_id,
                1,
                definition,
            )
            .await
            .unwrap();
    }

    fn chained_task_definition() -> (
        WorkflowDefinition,
        miranda_core::id::WorkflowTaskId,
        miranda_core::id::WorkflowTaskId,
    ) {
        let dependency_id = miranda_core::id::WorkflowTaskId::new();
        let task_id = miranda_core::id::WorkflowTaskId::new();

        let dependency = WorkflowTask::new(dependency_id, "validate".to_owned(), vec![]).unwrap();
        let task =
            WorkflowTask::new(task_id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![dependency, task]).unwrap();

        (definition, dependency_id, task_id)
    }

    #[tokio::test]
    async fn register_workflow_persists_first_version_and_returns_its_id() {
        let (control_plane, _queue, _router, store) = harness();
        let (definition, _task_id) = single_task_definition();
        let workflow_id = miranda_core::id::WorkflowId::new();

        let version_id = control_plane
            .register_workflow(workflow_id, "test_workflow", &definition)
            .await
            .unwrap();

        let versions = store.get_versions(workflow_id).await.unwrap();
        assert_eq!(versions, vec![version_id]);

        let stored = store.get_definition(version_id).await.unwrap();
        assert_eq!(stored, definition);
    }

    #[tokio::test]
    async fn register_workflow_appends_subsequent_versions_for_same_workflow() {
        let (control_plane, _queue, _router, store) = harness();
        let (first_definition, _task_id) = single_task_definition();
        let (second_definition, _task_id) = single_task_definition();
        let workflow_id = miranda_core::id::WorkflowId::new();

        let first_version_id = control_plane
            .register_workflow(workflow_id, "test_workflow", &first_definition)
            .await
            .unwrap();
        let second_version_id = control_plane
            .register_workflow(workflow_id, "test_workflow", &second_definition)
            .await
            .unwrap();

        assert_ne!(first_version_id, second_version_id);

        let mut versions = store.get_versions(workflow_id).await.unwrap();
        versions.sort();
        let mut expected = vec![first_version_id, second_version_id];
        expected.sort();
        assert_eq!(versions, expected);
    }

    #[tokio::test]
    async fn register_workflow_keeps_versions_independent_per_workflow_id() {
        let (control_plane, _queue, _router, store) = harness();
        let (definition, _task_id) = single_task_definition();
        let first_workflow_id = miranda_core::id::WorkflowId::new();
        let second_workflow_id = miranda_core::id::WorkflowId::new();

        control_plane
            .register_workflow(first_workflow_id, "workflow_a", &definition)
            .await
            .unwrap();
        control_plane
            .register_workflow(second_workflow_id, "workflow_b", &definition)
            .await
            .unwrap();

        assert_eq!(
            store.get_versions(first_workflow_id).await.unwrap().len(),
            1
        );
        assert_eq!(
            store.get_versions(second_workflow_id).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn submit_execution_persists_and_enqueues_ready_task() {
        let (control_plane, _queue, _router, store) = harness();
        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();

        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.status(), ExecutionStatus::Running);

        let assignment = control_plane
            .poll_task(WorkerId::new())
            .await
            .unwrap()
            .expect("ready task should have been enqueued");

        assert_eq!(assignment.task.id(), task_id);
    }

    #[tokio::test]
    async fn submit_execution_only_enqueues_tasks_whose_dependencies_are_met() {
        let (control_plane, _queue, _router, _store) = harness();
        let (definition, dependency_id, _task_id) = chained_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let first = control_plane
            .poll_task(WorkerId::new())
            .await
            .unwrap()
            .expect("dependency task should be ready");
        assert_eq!(first.task.id(), dependency_id);

        let second = control_plane.poll_task(WorkerId::new()).await.unwrap();
        assert!(
            second.is_none(),
            "dependent task should not be enqueued yet"
        );
    }

    #[tokio::test]
    async fn poll_task_returns_none_when_queue_is_empty() {
        let (control_plane, _queue, _router, _store) = harness();

        let result = control_plane.poll_task(WorkerId::new()).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn poll_task_transitions_task_to_running_and_issues_lease() {
        let (control_plane, _queue, _router, store) = harness();
        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();

        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let assignment = control_plane
            .poll_task(WorkerId::new())
            .await
            .unwrap()
            .unwrap();

        assert!(!assignment.lease_token.is_empty());

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.task(task_id).unwrap().status(), TaskStatus::Running);
    }

    #[tokio::test]
    async fn poll_task_errors_when_task_missing_from_execution() {
        let (control_plane, queue, _router, _store) = harness();
        let (definition, _task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();

        control_plane
            .submit_execution(execution, definition.clone())
            .await
            .unwrap();

        // Drain the naturally-enqueued item, then inject one referencing a
        // workflow task id that does not exist on the execution.
        control_plane.poll_task(WorkerId::new()).await.unwrap();

        queue
            .enqueue(QueueItem {
                execution_id,
                workflow_task_id: miranda_core::id::WorkflowTaskId::new(),
                definition: Arc::new(definition),
            })
            .await
            .unwrap();

        let err = match control_plane.poll_task(WorkerId::new()).await {
            Err(err) => err,
            Ok(_) => panic!("expected poll_task to fail"),
        };
        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn poll_task_errors_on_task_in_unexpected_status() {
        let (control_plane, queue, _router, _store) = harness();
        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();
        let definition = Arc::new(definition);

        control_plane
            .submit_execution(execution, (*definition).clone())
            .await
            .unwrap();

        // First poll moves the task to Running.
        control_plane.poll_task(WorkerId::new()).await.unwrap();

        // Re-inject the same task while it is already Running.
        queue
            .enqueue(QueueItem {
                execution_id,
                workflow_task_id: task_id,
                definition,
            })
            .await
            .unwrap();

        let err = match control_plane.poll_task(WorkerId::new()).await {
            Err(err) => err,
            Ok(_) => panic!("expected poll_task to fail"),
        };
        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn report_result_ok_completes_task_and_releases_lease() {
        let (control_plane, _queue, _router, store) = harness();
        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();
        let worker_id = WorkerId::new();

        save_definition(&store, workflow_version_id, &definition).await;
        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let assignment = control_plane.poll_task(worker_id).await.unwrap().unwrap();

        control_plane
            .report_result(worker_id, assignment.lease_token.clone(), Ok(()))
            .await
            .unwrap();

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(
            stored.task(task_id).unwrap().status(),
            TaskStatus::Completed
        );

        // Lease was released, so a repeated report against it must fail.
        let err = control_plane
            .report_result(worker_id, assignment.lease_token, Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn report_result_rejects_unknown_lease_token() {
        let (control_plane, _queue, _router, _store) = harness();

        let err = control_plane
            .report_result(WorkerId::new(), "not-a-real-token".to_string(), Ok(()))
            .await
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn report_result_rejects_mismatched_worker() {
        let (control_plane, _queue, _router, _store) = harness();
        let (definition, _task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let assignment = control_plane
            .poll_task(WorkerId::new())
            .await
            .unwrap()
            .unwrap();

        let err = control_plane
            .report_result(WorkerId::new(), assignment.lease_token, Ok(()))
            .await
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn report_result_err_requeues_task_when_retries_remain() {
        let (control_plane, queue, _router, store) = fast_retry_control_plane(3);
        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();
        let worker_id = WorkerId::new();

        save_definition(&store, workflow_version_id, &definition).await;
        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let assignment = control_plane.poll_task(worker_id).await.unwrap().unwrap();

        control_plane
            .report_result(
                worker_id,
                assignment.lease_token,
                Err(miranda_worker::WorkerError::ExecutionFailed {
                    message: "boom".to_string(),
                }),
            )
            .await
            .unwrap();

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.task(task_id).unwrap().status(), TaskStatus::Failed);
        assert_eq!(stored.status(), ExecutionStatus::Running);

        let requeued = queue.dequeue().await.unwrap();
        assert!(requeued.is_some(), "task should be requeued for retry");
    }

    #[tokio::test]
    async fn retried_task_can_be_polled_and_completed_again() {
        let (control_plane, _queue, _router, store) = fast_retry_control_plane(3);
        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();
        let worker_id = WorkerId::new();

        save_definition(&store, workflow_version_id, &definition).await;
        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let first_assignment = control_plane.poll_task(worker_id).await.unwrap().unwrap();

        control_plane
            .report_result(
                worker_id,
                first_assignment.lease_token,
                Err(miranda_worker::WorkerError::ExecutionFailed {
                    message: "boom".to_string(),
                }),
            )
            .await
            .unwrap();

        let second_assignment = control_plane
            .poll_task(worker_id)
            .await
            .unwrap()
            .expect("retried task should be pollable again");

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.task(task_id).unwrap().status(), TaskStatus::Running);

        control_plane
            .report_result(worker_id, second_assignment.lease_token, Ok(()))
            .await
            .unwrap();

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(
            stored.task(task_id).unwrap().status(),
            TaskStatus::Completed
        );
    }

    #[tokio::test]
    async fn report_result_err_fails_execution_once_retries_are_exhausted() {
        let (control_plane, _queue, _router, store) = fast_retry_control_plane(1);
        let (definition, _task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();
        let worker_id = WorkerId::new();

        save_definition(&store, workflow_version_id, &definition).await;
        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let assignment = control_plane.poll_task(worker_id).await.unwrap().unwrap();

        control_plane
            .report_result(
                worker_id,
                assignment.lease_token,
                Err(miranda_worker::WorkerError::ExecutionFailed {
                    message: "boom".to_string(),
                }),
            )
            .await
            .unwrap();

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.status(), ExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn heartbeat_silently_ignores_unknown_lease_tokens() {
        let (control_plane, _queue, _router, _store) = harness();

        control_plane
            .heartbeat(WorkerId::new(), &["does-not-exist".to_string()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn heartbeat_renews_active_leases() {
        let (control_plane, _queue, _router, _store) = harness();
        let (definition, _task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let worker_id = WorkerId::new();

        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        let assignment = control_plane.poll_task(worker_id).await.unwrap().unwrap();

        control_plane
            .heartbeat(worker_id, &[assignment.lease_token])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn register_worker_makes_it_selectable_by_capability() {
        let (control_plane, _queue, router, _store) = harness();
        let worker_id = WorkerId::new();

        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .unwrap();

        let selected = router.select_worker("send_email").await;
        assert_eq!(selected, Some(worker_id));
    }

    #[tokio::test]
    async fn deregister_worker_removes_it_from_routing() {
        let (control_plane, _queue, router, _store) = harness();
        let worker_id = WorkerId::new();

        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .unwrap();
        control_plane.deregister_worker(worker_id).await.unwrap();

        let selected = router.select_worker("send_email").await;
        assert_eq!(selected, None);
    }

    #[tokio::test]
    async fn with_lease_ttl_overrides_default() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let store = MemoryStore::new();
        let dispatch = Dispatcher::new(queue.clone());

        let control_plane = ControlPlane::new(queue, router, store, dispatch)
            .with_lease_ttl(Duration::from_secs(5));

        assert_eq!(control_plane.lease_ttl, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn with_worker_staleness_threshold_overrides_default() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let store = MemoryStore::new();
        let dispatch = Dispatcher::new(queue.clone());

        let control_plane = ControlPlane::new(queue, router, store, dispatch)
            .with_worker_staleness_threshold(Duration::from_secs(5));

        assert_eq!(
            control_plane.worker_staleness_threshold,
            Duration::from_secs(5)
        );
    }

    #[tokio::test]
    async fn reap_expired_leases_returns_zero_when_no_leases_exist() {
        let (control_plane, _queue, _router, _store) = harness();

        let recovered = control_plane.reap_expired_leases().await.unwrap();

        assert_eq!(recovered, 0);
    }

    #[tokio::test]
    async fn reap_expired_leases_fails_and_requeues_abandoned_running_tasks() {
        let queue = InMemoryTaskQueue::new();
        let store = MemoryStore::new();
        let dispatch = Dispatcher::new(queue.clone());
        let control_plane = ControlPlane::new(
            queue.clone(),
            InMemoryRouter::new(),
            store.clone(),
            dispatch,
        )
        .with_lease_ttl(Duration::ZERO);

        let (definition, task_id) = single_task_definition();
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        let execution_id = execution.id();

        save_definition(&store, workflow_version_id, &definition).await;
        control_plane
            .submit_execution(execution, definition)
            .await
            .unwrap();

        // Zero lease TTL means the lease issued here is immediately expired.
        control_plane.poll_task(WorkerId::new()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;

        let recovered = control_plane.reap_expired_leases().await.unwrap();

        assert_eq!(recovered, 1);

        let (stored, _version) = store.get_execution(execution_id).await.unwrap();
        assert_eq!(stored.task(task_id).unwrap().status(), TaskStatus::Failed);

        let requeued = queue.dequeue().await.unwrap();
        assert!(
            requeued.is_some(),
            "the recovered task should be requeued for a retry"
        );
    }

    #[tokio::test]
    async fn reap_stale_workers_returns_zero_when_no_workers_are_stale() {
        let (control_plane, _queue, _router, _store) = harness();

        let reaped = control_plane.reap_stale_workers().await.unwrap();

        assert_eq!(reaped, 0);
    }

    #[tokio::test]
    async fn reap_stale_workers_removes_workers_past_the_staleness_threshold() {
        let queue = InMemoryTaskQueue::new();
        let router = InMemoryRouter::new();
        let store = MemoryStore::new();
        let dispatch = Dispatcher::new(queue.clone());

        let control_plane = ControlPlane::new(queue, router.clone(), store, dispatch)
            .with_worker_staleness_threshold(Duration::ZERO);
        let worker_id = WorkerId::new();

        control_plane
            .register_worker(worker_id, vec!["send_email".to_string()])
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;

        let reaped = control_plane.reap_stale_workers().await.unwrap();

        assert_eq!(reaped, 1);
        assert_eq!(router.select_worker("send_email").await, None);
    }
}
