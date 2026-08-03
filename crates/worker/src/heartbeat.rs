use miranda_core::id::WorkerId;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::ControlPlaneClient;

pub struct HeartbeatRunner<C> {
    worker_id: WorkerId,
    interval: Duration,
    client: Arc<C>,
    shutdown: Arc<Notify>,
}

impl<C> HeartbeatRunner<C>
where
    C: ControlPlaneClient,
{
    pub fn new(worker_id: WorkerId, interval: Duration, client: Arc<C>) -> Self {
        Self {
            worker_id,
            interval,
            client,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    pub async fn run<F>(&self, get_active_leases: F)
    where
        F: Fn() -> Vec<String> + Send + Sync + 'static,
    {
        let mut interval_timer = tokio::time::interval(self.interval);
        let mut consecutive_failures = 0u32;
        const MAX_FAILURES: u32 = 3;

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    let leases = get_active_leases();

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

                            if consecutive_failures >= MAX_FAILURES {
                                error!(
                                    worker_id = %self.worker_id,
                                    max_failures = MAX_FAILURES,
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
