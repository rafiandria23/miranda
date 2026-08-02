use serde::{Deserialize, Serialize};

use crate::id::{AttemptId, TaskId};

// =========================================================================
// Execution Event Payloads
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCreated;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStarted;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCompleted;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionFailed;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCancelled;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionTerminated;

// =========================================================================
// Task Event Payloads
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCreated {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStarted {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCompleted {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFailed {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelled {
    pub task_id: TaskId,
}

// =========================================================================
// Attempt Event Payloads
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptCreated {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptStarted {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptSucceeded {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFailed {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptCancelled {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

// =========================================================================
// Unified Event Payload Enum
// =========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum EventPayload {
    // Execution
    ExecutionCreated(ExecutionCreated),
    ExecutionStarted(ExecutionStarted),
    ExecutionCompleted(ExecutionCompleted),
    ExecutionFailed(ExecutionFailed),
    ExecutionCancelled(ExecutionCancelled),
    ExecutionTerminated(ExecutionTerminated),

    // Task
    TaskCreated(TaskCreated),
    TaskStarted(TaskStarted),
    TaskCompleted(TaskCompleted),
    TaskFailed(TaskFailed),
    TaskCancelled(TaskCancelled),

    // Attempt
    AttemptCreated(AttemptCreated),
    AttemptStarted(AttemptStarted),
    AttemptSucceeded(AttemptSucceeded),
    AttemptFailed(AttemptFailed),
    AttemptCancelled(AttemptCancelled),
}
