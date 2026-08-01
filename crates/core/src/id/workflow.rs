use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkflowId(Uuid);

impl WorkflowId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow_id() {
        let workflow_id = WorkflowId::new();

        assert_ne!(workflow_id, WorkflowId::new());
    }
}
