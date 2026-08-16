use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

use crate::id::WorkerId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRegistration {
    id: WorkerId,
    capabilities: Vec<String>,
    last_heartbeat: OffsetDateTime,
}

impl WorkerRegistration {
    pub fn new(id: WorkerId, capabilities: Vec<String>, last_heartbeat: OffsetDateTime) -> Self {
        Self {
            id,
            capabilities,
            last_heartbeat,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    pub fn last_heartbeat(&self) -> OffsetDateTime {
        self.last_heartbeat
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }

    pub fn with_heartbeat(mut self, last_heartbeat: OffsetDateTime) -> Self {
        self.last_heartbeat = last_heartbeat;
        self
    }

    pub fn is_stale(&self, threshold: Duration) -> bool {
        OffsetDateTime::now_utc() - self.last_heartbeat > threshold
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registration(
        capabilities: Vec<String>,
        last_heartbeat: OffsetDateTime,
    ) -> WorkerRegistration {
        WorkerRegistration::new(WorkerId::new(), capabilities, last_heartbeat)
    }

    #[test]
    fn new_preserves_all_fields() {
        let id = WorkerId::new();
        let heartbeat = OffsetDateTime::now_utc();
        let registration = WorkerRegistration::new(id, vec!["shell".to_string()], heartbeat);

        assert_eq!(registration.id(), id);
        assert_eq!(registration.capabilities(), &["shell".to_string()]);
        assert_eq!(registration.last_heartbeat(), heartbeat);
    }

    #[test]
    fn has_capability_returns_true_when_present() {
        let registration = make_registration(
            vec!["shell".to_string(), "http".to_string()],
            OffsetDateTime::now_utc(),
        );

        assert!(registration.has_capability("http"));
    }

    #[test]
    fn has_capability_returns_false_when_absent() {
        let registration = make_registration(vec!["shell".to_string()], OffsetDateTime::now_utc());

        assert!(!registration.has_capability("http"));
    }

    #[test]
    fn with_heartbeat_replaces_last_heartbeat() {
        let original = OffsetDateTime::now_utc();
        let registration = make_registration(vec![], original);

        let updated_heartbeat = original + Duration::seconds(30);
        let updated = registration.with_heartbeat(updated_heartbeat);

        assert_eq!(updated.last_heartbeat(), updated_heartbeat);
    }

    #[test]
    fn is_stale_returns_true_when_beyond_threshold() {
        let stale_heartbeat = OffsetDateTime::now_utc() - Duration::minutes(10);
        let registration = make_registration(vec![], stale_heartbeat);

        assert!(registration.is_stale(Duration::minutes(5)));
    }

    #[test]
    fn is_stale_returns_false_when_within_threshold() {
        let recent_heartbeat = OffsetDateTime::now_utc();
        let registration = make_registration(vec![], recent_heartbeat);

        assert!(!registration.is_stale(Duration::minutes(5)));
    }

    #[test]
    fn serde_round_trip() {
        let registration = make_registration(vec!["shell".to_string()], OffsetDateTime::now_utc());
        let json = serde_json::to_string(&registration).unwrap();
        let deserialized: WorkerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(registration, deserialized);
    }
}
