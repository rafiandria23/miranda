use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkflowTaskId(Uuid);

impl WorkflowTaskId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow_task_id() {
        let workflow_task_id = WorkflowTaskId::new();

        assert_ne!(workflow_task_id, WorkflowTaskId::new());
    }
}
