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
