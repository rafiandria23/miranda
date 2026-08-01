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
    fn generate_unique_ids() {
        let first = WorkflowId::new();
        let second = WorkflowId::new();
        let third = WorkflowId::new();

        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(third, first);
    }
}
