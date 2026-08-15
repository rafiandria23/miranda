use miranda_core::id::WorkerId;
use std::{future::Future, sync::Arc};
use time::Duration;
use tokio::sync::Notify;

use crate::ControlPlaneClient;

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::seconds(30);
pub const DEFAULT_MAX_CONSECUTIVE_HEARTBEAT_FAILURES: u32 = 6;

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
        let std_interval: std::time::Duration = self
            .interval
            .try_into()
            .expect("heartbeat interval must be non-negative");

        let mut interval_timer = tokio::time::interval(std_interval);
        let mut consecutive_failures = 0u32;

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    let leases = get_active_leases().await;

                    tracing::debug!(worker_id = %self.worker_id, lease_count = leases.len(), "sending heartbeat");

                    match self.client.heartbeat(self.worker_id, &leases).await {
                        Ok(()) => {
                            if consecutive_failures > 0 {
                                tracing::info!(worker_id = %self.worker_id, "heartbeat recovered");
                            }

                            consecutive_failures = 0;
                        }

                        Err(e) => {
                            consecutive_failures += 1;

                            tracing::warn!(
                                worker_id = %self.worker_id,
                                error = %e,
                                consecutive_failures,
                                "heartbeat failed"
                            );

                            if consecutive_failures >= self.max_consecutive_failures {
                                tracing::error!(
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
                    tracing::info!(worker_id = %self.worker_id, "heartbeat runner shutting down");

                    break;
                }
            }
        }
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        pin::Pin,
        sync::atomic::{AtomicU32, Ordering},
        time::Duration as StdDuration,
    };
    use tokio_stream::Stream;

    use crate::{TaskAssignment, WorkerError};

    use super::*;

    struct MockClient {
        heartbeat_calls: AtomicU32,
        succeed_after: u32,
    }

    impl MockClient {
        fn always_ok() -> Self {
            Self {
                heartbeat_calls: AtomicU32::new(0),
                succeed_after: 0,
            }
        }

        fn always_err() -> Self {
            Self {
                heartbeat_calls: AtomicU32::new(0),
                succeed_after: u32::MAX,
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
            Ok(())
        }

        async fn deregister(&self, _worker_id: WorkerId) -> Result<(), WorkerError> {
            Ok(())
        }

        async fn heartbeat(
            &self,
            _worker_id: WorkerId,
            _active_leases: &[String],
        ) -> Result<(), WorkerError> {
            let call = self.heartbeat_calls.fetch_add(1, Ordering::SeqCst) + 1;

            if call > self.succeed_after {
                Ok(())
            } else {
                Err(WorkerError::ExecutionFailed {
                    message: "simulated heartbeat failure".to_owned(),
                })
            }
        }

        async fn poll_task(
            &self,
            _worker_id: WorkerId,
            _capabilities: &HashSet<String>,
        ) -> Result<Option<TaskAssignment>, WorkerError> {
            Ok(None)
        }

        async fn report_result(
            &self,
            _worker_id: WorkerId,
            _lease_token: String,
            _result: Result<(), WorkerError>,
        ) -> Result<(), WorkerError> {
            Ok(())
        }

        async fn subscribe(
            &self,
            _worker_id: WorkerId,
            _capabilities: &HashSet<String>,
        ) -> Result<Pin<Box<dyn Stream<Item = ()> + Send>>, WorkerError> {
            Ok(Box::pin(tokio_stream::empty()))
        }
    }

    fn short_interval() -> Duration {
        Duration::milliseconds(10)
    }

    #[tokio::test]
    async fn shutdown_notification_stops_the_loop_promptly() {
        let client = Arc::new(MockClient::always_ok());
        let runner = HeartbeatRunner::new(WorkerId::new(), client.clone())
            .with_interval(Duration::seconds(60));

        let shutdown = runner.shutdown_handle();
        shutdown.notify_one();

        let outcome = tokio::time::timeout(
            StdDuration::from_millis(500),
            runner.run(|| async { Vec::new() }),
        )
        .await;

        assert!(outcome.is_ok(), "runner should exit promptly on shutdown");
        assert_eq!(client.heartbeat_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn run_sends_heartbeats_with_the_active_leases() {
        let client = Arc::new(MockClient::always_ok());
        let runner =
            HeartbeatRunner::new(WorkerId::new(), client.clone()).with_interval(short_interval());

        let shutdown = runner.shutdown_handle();

        let run_fut = runner.run(|| async { vec!["lease-1".to_owned()] });
        tokio::pin!(run_fut);

        tokio::select! {
            _ = &mut run_fut => {}
            _ = tokio::time::sleep(StdDuration::from_millis(60)) => {
                shutdown.notify_one();
                run_fut.await;
            }
        }

        assert!(client.heartbeat_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn run_stops_after_max_consecutive_failures() {
        let client = Arc::new(MockClient::always_err());
        let runner = HeartbeatRunner::new(WorkerId::new(), client.clone())
            .with_interval(short_interval())
            .with_max_consecutive_failures(3);

        tokio::time::timeout(
            StdDuration::from_secs(2),
            runner.run(|| async { Vec::new() }),
        )
        .await
        .expect("runner should stop on its own after max failures");

        assert_eq!(client.heartbeat_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn with_max_consecutive_failures_of_one_stops_after_a_single_failure() {
        let client = Arc::new(MockClient::always_err());
        let runner = HeartbeatRunner::new(WorkerId::new(), client.clone())
            .with_interval(short_interval())
            .with_max_consecutive_failures(1);

        tokio::time::timeout(
            StdDuration::from_secs(1),
            runner.run(|| async { Vec::new() }),
        )
        .await
        .expect("runner should stop after the first failure");

        assert_eq!(client.heartbeat_calls.load(Ordering::SeqCst), 1);
    }
}
