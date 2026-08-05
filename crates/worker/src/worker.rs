use miranda_core::id::WorkerId;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{RwLock, mpsc},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
            poll_interval: Duration::from_secs(6),
            drain_timeout: Duration::from_secs(60),
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
    ) -> Self {
        Self {
            id: WorkerId::new(),
            capabilities: capabilities.into_iter().collect(),
            control_plane,
            executor,
            config,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn capabilities(&self) -> &HashSet<String> {
        &self.capabilities
    }

    pub fn run(self) -> WorkerHandle {
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();

        let worker_id = self.id;
        let capabilities = self.capabilities;
        let control_plane = self.control_plane;
        let executor = self.executor;
        let config = self.config;

        let task_handle = tokio::spawn(async move {
            info!(worker_id = %worker_id, "registering with control plane");
            control_plane.register(worker_id, &capabilities).await?;

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
                        info!(worker_id = %worker_id, "no active tasks remain, heartbeat stopping");
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
                            error!(worker_id = %worker_id, error = %join_error, "task panicked");
                        }
                    }

                    _ = shutdown_for_task.cancelled() => {
                        info!(worker_id = %worker_id, "shutdown signaled, draining active tasks");

                        break;
                    }
                }
            }

            while let Ok(assignment) = assignment_rx.try_recv() {
                spawn_assignment!(assignment);
            }

            drop(active_tx);

            let drain_result = tokio::time::timeout(config.drain_timeout, async {
                while let Some(result) = in_flight.join_next().await {
                    if let Err(join_error) = result {
                        error!(worker_id = %worker_id, error = %join_error, "task panicked during drain");
                    }
                }
            }).await;

            if drain_result.is_err() {
                warn!(
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
                warn!(worker_id = %worker_id, error = %e, "heartbeat task did not shut down cleanly");
            }

            info!(worker_id = %worker_id, "deregistering from control plane");
            control_plane.deregister(worker_id).await?;

            Ok(())
        });

        WorkerHandle {
            shutdown,
            task_handle,
        }
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
    loop {
        tokio::select! {
            biased;

            _ = shutdown.cancelled() => {
                debug!(worker_id = %worker_id, "poll loop stopping");

                return;
            }

            _ = tokio::time::sleep(poll_interval) => {
                match control_plane.poll_task(worker_id, &capabilities).await {
                    Ok(Some(assignment)) => {
                        if assignment_tx.send(assignment).await.is_err() {
                            return;
                        }
                    }

                    Ok(None) => debug!(worker_id = %worker_id, "no tasks available"),

                    Err(e) => warn!(worker_id = %worker_id, error = %e, "poll failed"),
                }
            }
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

    let result = executor.execute(&assignment.task).await;

    active_leases.write().await.remove(&lease_token);

    if let Err(e) = control_plane
        .report_result(worker_id, lease_token, result)
        .await
    {
        warn!(worker_id = %worker_id, error = %e, "failed to report result");
    }
}
