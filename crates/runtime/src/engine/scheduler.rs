use miranda_core::{definition::WorkflowDefinition, id::WorkflowTaskId, instance::Execution};

pub fn next_ready(execution: &Execution, definition: &WorkflowDefinition) -> Vec<WorkflowTaskId> {
    execution.ready_tasks(definition)
}
