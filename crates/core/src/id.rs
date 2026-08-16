use serde::{Deserialize, Serialize};
use std::{fmt, str};
use uuid::Uuid;

// Macro to generate strongly-typed, type-safe domain identifiers
macro_rules! define_id {
    (
        $(#[$meta:meta])*
        $name:ident
) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[repr(transparent)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            // Generates a new unique identifier (UUIDv7 for time-ordering)
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            // Creates an ID from an existing raw Uuid
            pub fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            // Expose the underlying raw Uuid
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            // Unwraps into the inner Uuid
            pub fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s).map(Self)
            }
        }

        impl From<Uuid> for $name {
            fn from(uuid: Uuid) -> Self {
                Self(uuid)
            }
        }

        impl From<$name> for Uuid {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

// =========================================================================
// Blueprint & Definition Identifiers
// =========================================================================

define_id!(
    // Unique identifier for a top-level Workflow blueprint.
    WorkflowId
);

define_id!(
    // Unique identifier for an immutable version revision of a Workflow.
    WorkflowVersionId
);

define_id!(
    // Unique identifier for a static task node blueprint within a Workflow Definition graph.
    WorkflowTaskId
);

// =========================================================================
// Dynamic Instance & Execution Identifiers
// =========================================================================

define_id!(
    // Unique identifier for a single workflow execution instance.
    ExecutionId
);

define_id!(
    // Unique identifier for a specific dynamic task instance inside an execution.
    TaskId
);

define_id!(
    // Unique identifier for an execution attempt on a specific task.
    AttemptId
);

define_id!(
    // Unique identifier for a lifecycle event emitted by an execution instance.
    EventId
);

define_id!(
    // Unique identifier for a task's entry in the dispatch queue.
    TaskQueueEntryId
);

define_id!(
    // Unique identifier for a worker process executing workflow tasks.
    WorkerId
);

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn new_generates_unique_ids() {
        let a = WorkflowId::new();
        let b = WorkflowId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn default_generates_a_valid_id() {
        let id = WorkflowId::default();
        assert_eq!(id.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn from_uuid_and_as_uuid_round_trip() {
        let uuid = Uuid::now_v7();
        let id = WorkflowId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), &uuid);
    }

    #[test]
    fn into_uuid_unwraps_inner_value() {
        let uuid = Uuid::now_v7();
        let id = WorkflowId::from_uuid(uuid);
        assert_eq!(id.into_uuid(), uuid);
    }

    #[test]
    fn display_formats_as_uuid_string() {
        let uuid = Uuid::now_v7();
        let id = WorkflowId::from_uuid(uuid);
        assert_eq!(id.to_string(), uuid.to_string());
    }

    #[test]
    fn from_str_parses_valid_uuid() {
        let uuid = Uuid::now_v7();
        let id = WorkflowId::from_str(&uuid.to_string()).unwrap();
        assert_eq!(id.into_uuid(), uuid);
    }

    #[test]
    fn from_str_rejects_invalid_uuid() {
        assert!(WorkflowId::from_str("not-a-uuid").is_err());
    }

    #[test]
    fn from_uuid_trait_conversion() {
        let uuid = Uuid::now_v7();
        let id: WorkflowId = uuid.into();
        assert_eq!(id.into_uuid(), uuid);
    }

    #[test]
    fn into_uuid_trait_conversion() {
        let id = WorkflowId::new();
        let uuid_from_id: Uuid = id.into();
        assert_eq!(uuid_from_id, *id.as_uuid());
    }

    #[test]
    fn ordering_matches_underlying_uuid_ordering() {
        let a = Uuid::now_v7();
        let b = Uuid::from_u128(a.as_u128() + 1);
        let id_a = WorkflowId::from_uuid(a);
        let id_b = WorkflowId::from_uuid(b);
        assert!(id_a < id_b);
    }

    #[test]
    fn serde_round_trip() {
        let id = WorkflowId::new();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: WorkflowId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn distinct_id_types_generate_correctly() {
        assert_ne!(WorkflowVersionId::new(), WorkflowVersionId::new());
        assert_ne!(WorkflowTaskId::new(), WorkflowTaskId::new());
        assert_ne!(ExecutionId::new(), ExecutionId::new());
        assert_ne!(TaskId::new(), TaskId::new());
        assert_ne!(AttemptId::new(), AttemptId::new());
        assert_ne!(EventId::new(), EventId::new());
        assert_ne!(TaskQueueEntryId::new(), TaskQueueEntryId::new());
        assert_ne!(WorkerId::new(), WorkerId::new());
    }
}
