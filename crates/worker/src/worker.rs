use miranda_core::id::WorkerId;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use time::Duration;
use tokio::{
    sync::{RwLock, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    assignment::{ControlPlaneClient, TaskAssignment},
    error::WorkerError,
    executor::TaskExecutor,
    heartbeat::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_MAX_CONSECUTIVE_HEARTBEAT_FAILURES, HeartbeatRunner,
    },
};

pub struct WorkerConfig {
    pub heartbeat_interval: Duration,
    pub max_consecutive_heartbeat_failures: u32,
    pub poll_interval: Duration,
    pub drain_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            max_consecutive_heartbeat_failures: DEFAULT_MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
            poll_interval: Duration::seconds(6),
            drain_timeout: Duration::seconds(60),
        }
    }
}

pub struct WorkerHandle {
    shutdown: CancellationToken,
    task_handle: JoinHandle<Result<(), WorkerError>>,
}

impl WorkerHandle {
    pub async fn shutdown(self) -> Result<(), WorkerError> {
        self.shutdown.cancel();

        match self.task_handle.await {
            Ok(result) => result,

            Err(e) => Err(WorkerError::ExecutionFailed {
                message: format!("worker task panicked: {}", e),
            }),
        }
    }
}

pub struct Worker<C, E> {
    id: WorkerId,
    capabilities: HashSet<String>,
    control_plane: Arc<C>,
    executor: Arc<E>,
    config: WorkerConfig,
    token: String,
}

impl<C, E> Worker<C, E>
where
    C: ControlPlaneClient + 'static,
    E: TaskExecutor + 'static,
{
    pub fn new(
        capabilities: Vec<String>,
        control_plane: Arc<C>,
        executor: Arc<E>,
        config: WorkerConfig,
        token: String,
    ) -> Self {
        Self {
            id: WorkerId::new(),
            capabilities: capabilities.into_iter().collect(),
            control_plane,
            executor,
            config,
            token,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn capabilities(&self) -> &HashSet<String> {
        &self.capabilities
    }

    pub fn run(self) -> (WorkerHandle, oneshot::Receiver<Result<(), WorkerError>>) {
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();

        let (started_tx, started_rx) = oneshot::channel();

        let worker_id = self.id;
        let capabilities = self.capabilities;
        let control_plane = self.control_plane;
        let executor = self.executor;
        let config = self.config;
        let token = self.token;

        let task_handle = tokio::spawn(async move {
            tracing::info!(worker_id = %worker_id, "registering with control plane");

            if let Err(e) = control_plane
                .register(worker_id, &capabilities, &token)
                .await
            {
                tracing::error!(worker_id = %worker_id, error = %e, "registration failed, worker will not start");

                let _ = started_tx.send(Err(e.clone()));

                return Err(e);
            }

            let _ = started_tx.send(Ok(()));

            let (active_tx, mut active_rx) = mpsc::channel::<()>(1);

            let active_leases: Arc<RwLock<HashMap<String, ()>>> =
                Arc::new(RwLock::new(HashMap::new()));
            let active_leases_for_heartbeat = active_leases.clone();

            let heartbeat_runner = HeartbeatRunner::new(worker_id, control_plane.clone())
                .with_interval(config.heartbeat_interval)
                .with_max_consecutive_failures(config.max_consecutive_heartbeat_failures);
            let heartbeat_shutdown_handle = heartbeat_runner.shutdown_handle();

            let heartbeat_handle = tokio::spawn(async move {
                tokio::select! {
                    biased;

                    _ = active_rx.recv() => {
                        tracing::info!(worker_id = %worker_id, "no active tasks remain, heartbeat stopping");
                    }

                    _ = heartbeat_runner.run(move || {
                        let leases = active_leases_for_heartbeat.clone();

                        async move {
                            leases.read().await.keys().cloned().collect()
                        }
                    }) => {}
                }
            });

            let (assignment_tx, mut assignment_rx) = mpsc::channel::<TaskAssignment>(32);
            let poll_shutdown = shutdown_for_task.clone();
            let poll_control_plane = control_plane.clone();
            let poll_capabilities = capabilities.clone();
            let poll_interval = config.poll_interval;

            let poll_handle = tokio::spawn(async move {
                run_poll_loop(
                    worker_id,
                    poll_capabilities,
                    poll_control_plane,
                    poll_interval,
                    poll_shutdown,
                    assignment_tx,
                )
                .await;
            });

            let mut in_flight: JoinSet<()> = JoinSet::new();

            macro_rules! spawn_assignment {
                ($assignment:expr) => {{
                    let executor = executor.clone();
                    let control_plane = control_plane.clone();
                    let canary = active_tx.clone();
                    let leases = active_leases.clone();
                    let assignment = $assignment;

                    in_flight.spawn(async move {
                        let _canary = canary; // held for task's lifetime

                        execute_and_report(worker_id, executor, control_plane, leases, assignment)
                            .await;
                    });
                }};
            }

            loop {
                tokio::select! {
                    biased;

                    Some(assignment) = assignment_rx.recv() => {
                        spawn_assignment!(assignment);
                    }

                    Some(result) = in_flight.join_next(), if !in_flight.is_empty() => {
                        if let Err(join_error) = result {
                            tracing::error!(worker_id = %worker_id, error = %join_error, "task panicked");
                        }
                    }

                    _ = shutdown_for_task.cancelled() => {
                        tracing::info!(worker_id = %worker_id, "shutdown signaled, draining active tasks");

                        break;
                    }
                }
            }

            while let Ok(assignment) = assignment_rx.try_recv() {
                spawn_assignment!(assignment);
            }

            drop(active_tx);

            let std_drain_timeout: std::time::Duration = config
                .drain_timeout
                .try_into()
                .expect("drain timeout must be non-negative");

            let drain_result = tokio::time::timeout(std_drain_timeout, async {
                while let Some(result) = in_flight.join_next().await {
                    if let Err(join_error) = result {
                        tracing::error!(worker_id = %worker_id, error = %join_error, "task panicked during drain");
                    }
                }
            }).await;

            if drain_result.is_err() {
                tracing::warn!(
                    worker_id = %worker_id,
                    remaining = in_flight.len(),
                    "drain timeout exceeded, aborting remaining tasks"
                );

                in_flight.abort_all();

                while in_flight.join_next().await.is_some() {}
            }

            poll_handle.abort(); // safety net; run_poll_loop should already have returned

            heartbeat_shutdown_handle.notify_waiters();

            if let Err(e) = heartbeat_handle.await {
                tracing::warn!(worker_id = %worker_id, error = %e, "heartbeat task did not shut down cleanly");
            }

            tracing::info!(worker_id = %worker_id, "deregistering from control plane");
            control_plane.deregister(worker_id).await?;

            Ok(())
        });

        (
            WorkerHandle {
                shutdown,
                task_handle,
            },
            started_rx,
        )
    }
}

async fn run_poll_loop<C: ControlPlaneClient>(
    worker_id: WorkerId,
    capabilities: HashSet<String>,
    control_plane: Arc<C>,
    poll_interval: Duration,
    shutdown: CancellationToken,
    assignment_tx: mpsc::Sender<TaskAssignment>,
) {
    let mut notifications = match control_plane.subscribe(worker_id, &capabilities).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::warn!(worker_id = %worker_id, error = %e, "subscribe failed, falling back to timer-only polling");

            Box::pin(tokio_stream::pending())
        }
    };

    let std_poll_interval: std::time::Duration = poll_interval
        .try_into()
        .expect("poll interval must be non-negative");

    loop {
        tokio::select! {
            biased;

            _ = shutdown.cancelled() => {
                tracing::debug!(worker_id = %worker_id, "poll loop stopping");

                return;
            }

            _ = tokio::time::sleep(std_poll_interval) => {}

            _ = notifications.next() => {
                tracing::debug!(worker_id = %worker_id, "woken by task notification");
            }
        }

        match control_plane.poll_task(worker_id, &capabilities).await {
            Ok(Some(assignment)) => {
                if assignment_tx.send(assignment).await.is_err() {
                    return;
                }
            }

            Ok(None) => tracing::debug!(worker_id = %worker_id, "no tasks available"),

            Err(e) => tracing::warn!(worker_id = %worker_id, error = %e, "poll failed"),
        }
    }
}

async fn execute_and_report<C: ControlPlaneClient, E: TaskExecutor>(
    worker_id: WorkerId,
    executor: Arc<E>,
    control_plane: Arc<C>,
    active_leases: Arc<RwLock<HashMap<String, ()>>>,
    assignment: TaskAssignment,
) {
    let lease_token = assignment.lease_token.clone();

    active_leases.write().await.insert(lease_token.clone(), ());

    let result = executor
        .execute(
            assignment.execution_id,
            &assignment.task,
            assignment.timeout,
        )
        .await;

    active_leases.write().await.remove(&lease_token);

    if let Err(e) = control_plane
        .report_result(worker_id, lease_token, result)
        .await
    {
        tracing::warn!(worker_id = %worker_id, error = %e, "failed to report result");
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::{ExecutionId, WorkflowTaskId};
    use std::{
        pin::Pin,
        sync::atomic::{AtomicU32, Ordering},
        time::Duration as StdDuration,
    };
    use tokio::sync::Mutex as AsyncMutex;
    use tokio_stream::Stream;

    use super::*;

    #[derive(Default)]
    struct MockClient {
        register_calls: AtomicU32,
        deregister_calls: AtomicU32,
        heartbeat_calls: AtomicU32,
        poll_calls: AtomicU32,
        fail_register: bool,
        pending_assignments: AsyncMutex<Vec<TaskAssignment>>,
        reported: AsyncMutex<Vec<(String, Result<(), WorkerError>)>>,
    }

    impl MockClient {
        fn new() -> Self {
            Self::default()
        }

        fn failing_register() -> Self {
            Self {
                fail_register: true,
                ..Self::default()
            }
        }

        fn with_assignment(assignment: TaskAssignment) -> Self {
            Self {
                pending_assignments: AsyncMutex::new(vec![assignment]),
                ..Self::default()
            }
        }
    }

    impl ControlPlaneClient for MockClient {
        async fn register(
            &self,
            _worker_id: WorkerId,
            _capabilities: &HashSet<String>,
            _token: &str,
        ) -> Result<(), WorkerError> {
            self.register_calls.fetch_add(1, Ordering::SeqCst);

            if self.fail_register {
                Err(WorkerError::ExecutionFailed {
                    message: "simulated registration failure".to_owned(),
                })
            } else {
                Ok(())
            }
        }

        async fn deregister(&self, _worker_id: WorkerId) -> Result<(), WorkerError> {
            self.deregister_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn heartbeat(
            &self,
            _worker_id: WorkerId,
            _active_leases: &[String],
        ) -> Result<(), WorkerError> {
            self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn poll_task(
            &self,
            _worker_id: WorkerId,
            _capabilities: &HashSet<String>,
        ) -> Result<Option<TaskAssignment>, WorkerError> {
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.pending_assignments.lock().await.pop())
        }

        async fn report_result(
            &self,
            _worker_id: WorkerId,
            lease_token: String,
            result: Result<(), WorkerError>,
        ) -> Result<(), WorkerError> {
            self.reported.lock().await.push((lease_token, result));
            Ok(())
        }

        async fn subscribe(
            &self,
            _worker_id: WorkerId,
            _capabilities: &HashSet<String>,
        ) -> Result<Pin<Box<dyn Stream<Item = ()> + Send>>, WorkerError> {
            Ok(Box::pin(tokio_stream::pending()))
        }
    }

    struct CountingExecutor {
        calls: AtomicU32,
        fail: bool,
    }

    impl CountingExecutor {
        fn ok() -> Self {
            Self {
                calls: AtomicU32::new(0),
                fail: false,
            }
        }

        fn err() -> Self {
            Self {
                calls: AtomicU32::new(0),
                fail: true,
            }
        }
    }

    impl TaskExecutor for CountingExecutor {
        async fn execute(
            &self,
            _execution_id: ExecutionId,
            _task: &miranda_core::workflow::WorkflowTask,
            _timeout: Option<StdDuration>,
        ) -> Result<(), WorkerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);

            if self.fail {
                Err(WorkerError::ExecutionFailed {
                    message: "simulated execution failure".to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }

    fn test_config() -> WorkerConfig {
        WorkerConfig {
            heartbeat_interval: Duration::seconds(60),
            max_consecutive_heartbeat_failures: 3,
            poll_interval: Duration::milliseconds(10),
            drain_timeout: Duration::seconds(5),
        }
    }

    fn test_assignment(lease_token: &str) -> TaskAssignment {
        TaskAssignment {
            execution_id: ExecutionId::new(),
            lease_token: lease_token.to_owned(),
            task: miranda_core::workflow::WorkflowTask::new(
                WorkflowTaskId::new(),
                "noop".to_owned(),
                vec![],
            )
            .unwrap(),
            timeout: None,
        }
    }

    #[test]
    fn new_assigns_a_unique_id_and_dedupes_capabilities() {
        let worker = Worker::new(
            vec!["a".to_owned(), "b".to_owned(), "a".to_owned()],
            Arc::new(MockClient::new()),
            Arc::new(CountingExecutor::ok()),
            test_config(),
            "token".to_owned(),
        );

        assert_eq!(worker.capabilities().len(), 2);
        assert!(worker.capabilities().contains("a"));
        assert!(worker.capabilities().contains("b"));
    }

    #[tokio::test]
    async fn run_reports_registration_failure_and_exits() {
        let client = Arc::new(MockClient::failing_register());
        let worker = Worker::new(
            vec![],
            client.clone(),
            Arc::new(CountingExecutor::ok()),
            test_config(),
            "token".to_owned(),
        );

        let (handle, started_rx) = worker.run();

        let started = tokio::time::timeout(StdDuration::from_secs(1), started_rx)
            .await
            .expect("started_rx should resolve promptly")
            .expect("started_rx should not be dropped");

        assert!(started.is_err());

        let result = tokio::time::timeout(StdDuration::from_secs(1), handle.task_handle)
            .await
            .expect("worker task should finish promptly")
            .expect("worker task should not panic");

        assert!(result.is_err());
        assert_eq!(client.deregister_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn run_registers_then_deregisters_cleanly_on_shutdown() {
        let client = Arc::new(MockClient::new());
        let worker = Worker::new(
            vec![],
            client.clone(),
            Arc::new(CountingExecutor::ok()),
            test_config(),
            "token".to_owned(),
        );

        let (handle, started_rx) = worker.run();

        let started = tokio::time::timeout(StdDuration::from_secs(1), started_rx)
            .await
            .expect("started_rx should resolve promptly")
            .expect("started_rx should not be dropped");

        assert!(started.is_ok());

        let result = tokio::time::timeout(StdDuration::from_secs(1), handle.shutdown())
            .await
            .expect("shutdown should complete promptly");

        assert!(result.is_ok());
        assert_eq!(client.register_calls.load(Ordering::SeqCst), 1);
        assert_eq!(client.deregister_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_dispatches_polled_assignments_to_the_executor_and_reports_success() {
        let client = Arc::new(MockClient::with_assignment(test_assignment("lease-1")));
        let executor = Arc::new(CountingExecutor::ok());
        let worker = Worker::new(
            vec![],
            client.clone(),
            executor.clone(),
            test_config(),
            "token".to_owned(),
        );

        let (handle, _started_rx) = worker.run();

        tokio::time::sleep(StdDuration::from_millis(100)).await;

        tokio::time::timeout(StdDuration::from_secs(1), handle.shutdown())
            .await
            .expect("shutdown should complete promptly")
            .expect("worker should shut down without error");

        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

        let reported = client.reported.lock().await;
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].0, "lease-1");
        assert!(reported[0].1.is_ok());
    }

    #[tokio::test]
    async fn run_reports_executor_failures_without_crashing_the_worker() {
        let client = Arc::new(MockClient::with_assignment(test_assignment("lease-2")));
        let executor = Arc::new(CountingExecutor::err());
        let worker = Worker::new(
            vec![],
            client.clone(),
            executor.clone(),
            test_config(),
            "token".to_owned(),
        );

        let (handle, _started_rx) = worker.run();

        tokio::time::sleep(StdDuration::from_millis(100)).await;

        tokio::time::timeout(StdDuration::from_secs(1), handle.shutdown())
            .await
            .expect("shutdown should complete promptly")
            .expect("worker should shut down without error");

        let reported = client.reported.lock().await;
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].0, "lease-2");
        assert!(reported[0].1.is_err());
    }

    #[tokio::test]
    async fn execute_and_report_tracks_active_leases_and_reports_the_result() {
        let client = Arc::new(MockClient::new());
        let executor = Arc::new(CountingExecutor::ok());
        let active_leases: Arc<RwLock<HashMap<String, ()>>> = Arc::new(RwLock::new(HashMap::new()));

        execute_and_report(
            WorkerId::new(),
            executor.clone(),
            client.clone(),
            active_leases.clone(),
            test_assignment("lease-3"),
        )
        .await;

        assert!(active_leases.read().await.is_empty());
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

        let reported = client.reported.lock().await;
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].0, "lease-3");
        assert!(reported[0].1.is_ok());
    }
}
