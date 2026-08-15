use miranda_storage::leadership_store::LeadershipStore;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use time::Duration;
use uuid::Uuid;

pub const DEFAULT_LEADERSHIP_TTL: Duration = Duration::seconds(18);
pub const DEFAULT_LEADERSHIP_RENEW_INTERVAL: Duration = Duration::seconds(6);

pub struct LeadershipRunner<S> {
    holder_id: String,
    store: S,
    ttl: Duration,
    renew_interval: Duration,
    is_leader: Arc<AtomicBool>,
}

impl<S: LeadershipStore> LeadershipRunner<S> {
    pub fn new(store: S) -> Self {
        Self {
            holder_id: Uuid::new_v4().to_string(),
            store,
            ttl: DEFAULT_LEADERSHIP_TTL,
            renew_interval: DEFAULT_LEADERSHIP_RENEW_INTERVAL,
            is_leader: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn with_renew_interval(mut self, renew_interval: Duration) -> Self {
        self.renew_interval = renew_interval;
        self
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    pub fn holder_id(&self) -> &str {
        &self.holder_id
    }

    pub async fn run(&self) {
        let std_interval: std::time::Duration = self
            .renew_interval
            .try_into()
            .expect("leadership renew interval must be non-negative");

        let mut ticker = tokio::time::interval(std_interval);

        loop {
            ticker.tick().await;

            let currently_leader = self.is_leader.load(Ordering::Relaxed);

            let result = if currently_leader {
                self.store.renew(&self.holder_id, self.ttl).await
            } else {
                self.store.try_acquire(&self.holder_id, self.ttl).await
            };

            match result {
                Ok(true) => {
                    if !currently_leader {
                        tracing::info!(holder_id = %self.holder_id, "acquired leadership");
                    }

                    self.is_leader.store(true, Ordering::Relaxed);
                }

                Ok(false) => {
                    if currently_leader {
                        tracing::warn!(holder_id = %self.holder_id, "lost leadership (renew failed)");
                    }

                    self.is_leader.store(false, Ordering::Relaxed);
                }

                Err(e) => {
                    tracing::warn!(holder_id = %self.holder_id, error = %e, "leadership check failed");
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
    use miranda_storage::InMemoryStore;

    use super::*;

    #[test]
    fn new_generates_a_unique_holder_id_and_starts_as_non_leader() {
        let runner = LeadershipRunner::new(InMemoryStore::new());
        let other = LeadershipRunner::new(InMemoryStore::new());

        assert!(!runner.is_leader());
        assert!(!runner.holder_id().is_empty());
        assert_ne!(runner.holder_id(), other.holder_id());
    }

    #[test]
    fn with_ttl_and_with_renew_interval_override_the_defaults() {
        let runner = LeadershipRunner::new(InMemoryStore::new())
            .with_ttl(Duration::seconds(1))
            .with_renew_interval(Duration::milliseconds(10));

        assert_eq!(runner.ttl, Duration::seconds(1));
        assert_eq!(runner.renew_interval, Duration::milliseconds(10));
    }

    #[tokio::test(start_paused = true)]
    async fn run_acquires_leadership_when_none_is_held() {
        let store = InMemoryStore::new();
        let runner = Arc::new(
            LeadershipRunner::new(store)
                .with_ttl(Duration::seconds(10))
                .with_renew_interval(Duration::milliseconds(50)),
        );

        let handle = tokio::spawn({
            let runner = runner.clone();
            async move { runner.run().await }
        });

        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        assert!(runner.is_leader());

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn run_does_not_acquire_leadership_while_another_holder_is_active() {
        let store = InMemoryStore::new();

        store
            .try_acquire("other-holder", Duration::seconds(10))
            .await
            .expect("other holder acquires leadership");

        let runner = Arc::new(
            LeadershipRunner::new(store)
                .with_ttl(Duration::seconds(10))
                .with_renew_interval(Duration::milliseconds(50)),
        );

        let handle = tokio::spawn({
            let runner = runner.clone();
            async move { runner.run().await }
        });

        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        assert!(!runner.is_leader());

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn run_loses_leadership_when_the_lease_expires_before_the_next_renewal() {
        let store = InMemoryStore::new();

        let runner = Arc::new(
            LeadershipRunner::new(store)
                .with_ttl(Duration::milliseconds(10))
                .with_renew_interval(Duration::milliseconds(50)),
        );

        let handle = tokio::spawn({
            let runner = runner.clone();
            async move { runner.run().await }
        });

        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        assert!(runner.is_leader());

        std::thread::sleep(std::time::Duration::from_millis(15));

        tokio::time::advance(std::time::Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert!(!runner.is_leader());

        handle.abort();
    }
}
