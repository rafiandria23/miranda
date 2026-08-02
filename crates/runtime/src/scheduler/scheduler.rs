use miranda_core::{Execution, WorkflowDefinition, id::WorkflowTaskId};

pub fn next_ready(execution: &Execution, definition: &WorkflowDefinition) -> Vec<WorkflowTaskId> {
    execution.ready_tasks(definition)
}
