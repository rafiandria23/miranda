use miranda_core::id::WorkerId;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::ControlPlaneClient;

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_CONSECUTIVE_HEARTBEAT_FAILURES: u32 = 3;

pub struct HeartbeatRunner<C> {
    worker_id: WorkerId,
    interval: Duration,
    max_consecutive_failures: u32,
    client: Arc<C>,
    shutdown: Arc<Notify>,
}

impl<C> HeartbeatRunner<C>
where
    C: ControlPlaneClient,
{
    pub fn new(worker_id: WorkerId, client: Arc<C>) -> Self {
        Self {
            worker_id,
            interval: DEFAULT_HEARTBEAT_INTERVAL,
            max_consecutive_failures: DEFAULT_MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
            client,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_max_consecutive_failures(mut self, max_consecutive_failures: u32) -> Self {
        self.max_consecutive_failures = max_consecutive_failures;
        self
    }

    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    pub async fn run<F, Fut>(&self, get_active_leases: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Vec<String>> + Send,
    {
        let mut interval_timer = tokio::time::interval(self.interval);
        let mut consecutive_failures = 0u32;

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    let leases = get_active_leases().await;

                    debug!(worker_id = %self.worker_id, lease_count = leases.len(), "sending heartbeat");

                    match self.client.heartbeat(self.worker_id, &leases).await {
                        Ok(()) => {
                            if consecutive_failures > 0 {
                                info!(worker_id = %self.worker_id, "heartbeat recovered");
                            }

                            consecutive_failures = 0;
                        }

                        Err(e) => {
                            consecutive_failures += 1;

                            warn!(
                                worker_id = %self.worker_id,
                                error = %e,
                                consecutive_failures,
                                "heartbeat failed"
                            );

                            if consecutive_failures >= self.max_consecutive_failures {
                                error!(
                                    worker_id = %self.worker_id,
                                    max_failures = self.max_consecutive_failures,
                                    "heartbeat max failures reached, shutting down"
                                );

                                break;
                            }
                        }
                    }
                }

                _ = self.shutdown.notified() => {
                    info!(worker_id = %self.worker_id, "heartbeat runner shutting down");

                    break;
                }
            }
        }
    }
}
