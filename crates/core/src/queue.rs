use serde::{Deserialize, Serialize};

use crate::id::{ExecutionId, TaskQueueEntryId, WorkflowTaskId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedTask {
    id: TaskQueueEntryId,
    execution_id: ExecutionId,
    workflow_task_id: WorkflowTaskId,
}

impl QueuedTask {
    pub fn new(execution_id: ExecutionId, workflow_task_id: WorkflowTaskId) -> Self {
        Self {
            id: TaskQueueEntryId::new(),
            execution_id,
            workflow_task_id,
        }
    }

    pub fn from_parts(
        id: TaskQueueEntryId,
        execution_id: ExecutionId,
        workflow_task_id: WorkflowTaskId,
    ) -> Self {
        Self {
            id,
            execution_id,
            workflow_task_id,
        }
    }

    pub fn id(&self) -> TaskQueueEntryId {
        self.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn workflow_task_id(&self) -> WorkflowTaskId {
        self.workflow_task_id
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_generates_unique_id_and_preserves_fields() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let task = QueuedTask::new(execution_id, workflow_task_id);

        assert_eq!(task.execution_id(), execution_id);
        assert_eq!(task.workflow_task_id(), workflow_task_id);
    }

    #[test]
    fn new_generates_distinct_ids_for_separate_tasks() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let a = QueuedTask::new(execution_id, workflow_task_id);
        let b = QueuedTask::new(execution_id, workflow_task_id);

        assert_ne!(a.id(), b.id());
    }

    #[test]
    fn from_parts_preserves_all_fields() {
        let id = TaskQueueEntryId::new();
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let task = QueuedTask::from_parts(id, execution_id, workflow_task_id);

        assert_eq!(task.id(), id);
        assert_eq!(task.execution_id(), execution_id);
        assert_eq!(task.workflow_task_id(), workflow_task_id);
    }

    #[test]
    fn equality_is_based_on_all_fields() {
        let id = TaskQueueEntryId::new();
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let a = QueuedTask::from_parts(id, execution_id, workflow_task_id);
        let b = QueuedTask::from_parts(id, execution_id, workflow_task_id);
        let c = QueuedTask::from_parts(TaskQueueEntryId::new(), execution_id, workflow_task_id);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn serde_round_trip() {
        let task = QueuedTask::new(ExecutionId::new(), WorkflowTaskId::new());
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: QueuedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(task, deserialized);
    }
}
