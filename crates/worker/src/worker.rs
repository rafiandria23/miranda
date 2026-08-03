use miranda_core::id::WorkerId;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Notify, RwLock, mpsc};
use tracing::{debug, info, warn};

use crate::{ControlPlaneClient, HeartbeatRunner, TaskExecutor, WorkerError};

pub struct WorkerConfig {
    pub heartbeat_interval: Duration,
    pub poll_interval: Duration,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(30),
            poll_interval: Duration::from_secs(6),
            graceful_shutdown_timeout: Duration::from_secs(60),
        }
    }
}

pub struct WorkerHandle {
    shutdown_tx: mpsc::Sender<()>,
    heartbeat_shutdown: Arc<Notify>,

    // Store the join handle so we can await completion
    task_handle: tokio::task::JoinHandle<Result<(), WorkerError>>,
}

impl WorkerHandle {
    pub async fn shutdown(self) -> Result<(), WorkerError> {
        let _ = self.shutdown_tx.send(()).await;

        self.heartbeat_shutdown.notify_waiters();

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
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        let heartbeat_shutdown = Arc::new(Notify::new());

        let worker_id = self.id;
        let capabilities = self.capabilities.clone();
        let control_plane = self.control_plane.clone();
        let executor = self.executor.clone();
        let config = self.config;

        let task_handle = tokio::spawn(async move {
            // 1. Register with control plane
            info!(worker_id = %worker_id, "registering with control plane");

            control_plane.register(worker_id, &capabilities).await?;

            // 2. Spawn heartbeat task
            let active_leases: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
            let active_leases_for_heartbeat = active_leases.clone();

            let heartbeat_runner =
                HeartbeatRunner::new(worker_id, config.heartbeat_interval, control_plane.clone());
            let heartbeat_handle = heartbeat_runner.shutdown_handle();

            tokio::spawn(async move {
                heartbeat_runner
                    .run(move || {
                        let leases = active_leases_for_heartbeat.blocking_read().clone();

                        leases
                    })
                    .await;
            });

            // 3. Poll loop
            let mut shutdown_requested = false;

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(config.poll_interval) => {
                        if shutdown_requested {
                            // Graceful shutdown: stop polling, finish active tasks

                            let leases = active_leases.read().await;

                            if leases.is_empty() {
                                info!(worker_id = %worker_id, "all tasks complete, shutting down");

                                break;
                            }

                            debug!(worker_id = %worker_id, active_tasks = leases.len(), "waiting for tasks to complete");

                            continue;
                        }

                        match control_plane.poll_task(worker_id, &capabilities).await {
                            Ok(Some(assignment)) => {
                                debug!(worker_id = %worker_id, task_type = assignment.task.task_type(), "task received");

                                let lease_token = assignment.lease_token.clone();

                                active_leases.write().await.push(lease_token.clone());

                                let result = executor.execute(&assignment.task).await;

                                active_leases.write().await.retain(|t| t != &lease_token);

                                if let Err(e) = control_plane.report_result(worker_id, lease_token, result).await {
                                    warn!(worker_id = %worker_id, error = %e, "failed to report result");
                                }
                            }

                            Ok(None) => {
                                debug!(worker_id = %worker_id, "no tasks available");
                            }

                            Err(e) => {
                                warn!(worker_id = %worker_id, error = %e, "poll failed");
                            }
                        }
                    }

                    _ = shutdown_rx.recv() => {
                        if !shutdown_requested {
                            info!(worker_id = %worker_id, "shutdown requested, finishing active tasks");

                            shutdown_requested = true;
                        }
                    }
                }
            }

            // 4. Shutdown heartbeat
            heartbeat_handle.notify_waiters();

            // 5. Deregister
            info!(worker_id = %worker_id, "deregistering from control plane");

            control_plane.deregister(worker_id).await?;

            Ok(())
        });

        WorkerHandle {
            shutdown_tx,
            heartbeat_shutdown,
            task_handle,
        }
    }
}
