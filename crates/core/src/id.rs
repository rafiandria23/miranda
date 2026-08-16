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
