use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkflowVersionId(Uuid);

impl WorkflowVersionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow_version_id() {
        let workflow_version_id = WorkflowVersionId::new();

        assert_ne!(workflow_version_id, WorkflowVersionId::new());
    }
}
