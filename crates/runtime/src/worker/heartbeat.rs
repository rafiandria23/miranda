use miranda_core::id::WorkerId;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::Notify;

pub struct HeartbeatRunner {
    worker_id: WorkerId,
    interval: Duration,
    shutdown_notify: Arc<Notify>,
}

impl HeartbeatRunner {
    pub fn new(worker_id: WorkerId, interval: Duration) -> Self {
        Self {
            worker_id,
            interval,
            shutdown_notify: Arc::new(Notify::new()),
        }
    }

    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown_notify.clone()
    }

    pub async fn run<F, Fut>(&self, on_heartbeat: F)
    where
        F: Fn(WorkerId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let mut interval_timer = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    on_heartbeat(self.worker_id).await;
                }

                _ = self.shutdown_notify.notified() => {
                    tracing::info!(worker_id = %self.worker_id, "heartbeat runner shutting down");

                    break;
                }
            }
        }
    }
}
