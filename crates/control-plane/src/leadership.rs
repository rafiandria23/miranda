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
