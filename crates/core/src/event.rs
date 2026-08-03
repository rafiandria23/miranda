use serde::{Deserialize, Serialize};

use crate::id::{AttemptId, EventId, ExecutionId, TaskId, WorkflowTaskId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    id: EventId,
    execution_id: ExecutionId,
    payload: EventPayload,
}

impl Event {
    pub fn new(execution_id: ExecutionId, payload: EventPayload) -> Self {
        Self {
            id: EventId::new(),
            execution_id,
            payload,
        }
    }

    pub fn with_id(id: EventId, execution_id: ExecutionId, payload: EventPayload) -> Self {
        Self {
            id,
            execution_id,
            payload,
        }
    }

    pub fn id(&self) -> EventId {
        self.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn payload(&self) -> &EventPayload {
        &self.payload
    }

    pub fn into_payload(self) -> EventPayload {
        self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum EventPayload {
    // Execution lifecycle
    ExecutionCreated,
    ExecutionStarted,
    ExecutionCompleted,
    ExecutionFailed,
    ExecutionCancelled,
    ExecutionTerminated,

    // Task lifecycle
    TaskCreated {
        workflow_task_id: WorkflowTaskId,
    },
    TaskStarted {
        workflow_task_id: WorkflowTaskId,
    },
    TaskCompleted {
        workflow_task_id: WorkflowTaskId,
    },
    TaskFailed {
        workflow_task_id: WorkflowTaskId,
    },
    TaskCancelled {
        workflow_task_id: WorkflowTaskId,
    },
    TaskRetried {
        workflow_task_id: WorkflowTaskId,
    },

    // Attempt lifecycle
    AttemptCreated {
        task_id: TaskId,
        attempt_id: AttemptId,
        attempt_number: u32,
    },
    AttemptStarted {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    AttemptSucceeded {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    AttemptFailed {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    AttemptCancelled {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_assigns_a_fresh_id_and_stores_execution_id_and_payload() {
        let execution_id = ExecutionId::new();
        let event = Event::new(execution_id, EventPayload::ExecutionStarted);

        assert_eq!(event.execution_id(), execution_id);
        assert_eq!(event.payload(), &EventPayload::ExecutionStarted);
        assert_ne!(event.id(), EventId::new());
    }

    #[test]
    fn new_assigns_unique_ids_across_calls() {
        let execution_id = ExecutionId::new();

        let first = Event::new(execution_id, EventPayload::ExecutionStarted);
        let second = Event::new(execution_id, EventPayload::ExecutionStarted);

        assert_ne!(first.id(), second.id());
    }

    #[test]
    fn with_id_preserves_the_given_id() {
        let id = EventId::new();
        let execution_id = ExecutionId::new();
        let event = Event::with_id(id, execution_id, EventPayload::ExecutionCompleted);

        assert_eq!(event.id(), id);
        assert_eq!(event.execution_id(), execution_id);
        assert_eq!(event.payload(), &EventPayload::ExecutionCompleted);
    }

    #[test]
    fn into_payload_consumes_the_event_and_returns_the_payload() {
        let workflow_task_id = WorkflowTaskId::new();
        let event = Event::new(
            ExecutionId::new(),
            EventPayload::TaskStarted { workflow_task_id },
        );

        assert_eq!(
            event.into_payload(),
            EventPayload::TaskStarted { workflow_task_id }
        );
    }

    #[test]
    fn unit_variants_with_equal_data_are_equal() {
        assert_eq!(
            EventPayload::ExecutionStarted,
            EventPayload::ExecutionStarted
        );
        assert_ne!(
            EventPayload::ExecutionStarted,
            EventPayload::ExecutionFailed
        );
    }

    #[test]
    fn struct_variants_compare_by_field_values() {
        let workflow_task_id = WorkflowTaskId::new();
        let other_workflow_task_id = WorkflowTaskId::new();

        assert_eq!(
            EventPayload::TaskCompleted { workflow_task_id },
            EventPayload::TaskCompleted { workflow_task_id }
        );
        assert_ne!(
            EventPayload::TaskCompleted { workflow_task_id },
            EventPayload::TaskCompleted {
                workflow_task_id: other_workflow_task_id
            }
        );
        assert_ne!(
            EventPayload::TaskCompleted { workflow_task_id },
            EventPayload::TaskFailed { workflow_task_id }
        );
    }

    #[test]
    fn event_round_trips_through_json() {
        let event = Event::new(
            ExecutionId::new(),
            EventPayload::TaskRetried {
                workflow_task_id: WorkflowTaskId::new(),
            },
        );

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn unit_variant_serializes_with_tag_and_no_data_field() {
        let json = serde_json::to_value(EventPayload::ExecutionStarted).unwrap();

        assert_eq!(json, serde_json::json!({ "type": "ExecutionStarted" }));
    }

    #[test]
    fn struct_variant_serializes_with_tag_and_data_field() {
        let attempt_id = AttemptId::new();
        let task_id = TaskId::new();
        let payload = EventPayload::AttemptStarted {
            task_id,
            attempt_id,
        };

        let json = serde_json::to_value(&payload).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "type": "AttemptStarted",
                "data": {
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                }
            })
        );
    }

    #[test]
    fn attempt_created_round_trips_through_json() {
        let payload = EventPayload::AttemptCreated {
            task_id: TaskId::new(),
            attempt_id: AttemptId::new(),
            attempt_number: 3,
        };

        let json = serde_json::to_string(&payload).unwrap();
        let deserialized: EventPayload = serde_json::from_str(&json).unwrap();

        assert_eq!(payload, deserialized);
    }
}
