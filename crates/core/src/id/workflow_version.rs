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
    fn generate_unique_ids() {
        let first = WorkflowVersionId::new();
        let second = WorkflowVersionId::new();
        let third = WorkflowVersionId::new();

        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(third, first);
    }
}
