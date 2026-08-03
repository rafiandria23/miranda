use miranda_core::{execution::Execution, id::WorkflowTaskId, workflow::WorkflowDefinition};

pub fn next_ready(execution: &Execution, definition: &WorkflowDefinition) -> Vec<WorkflowTaskId> {
    execution.ready_tasks(definition)
}
