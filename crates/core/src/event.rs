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
