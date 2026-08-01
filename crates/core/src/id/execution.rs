use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionId(Uuid);

impl ExecutionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_execution_id() {
        let execution_id = ExecutionId::new();

        assert_ne!(execution_id, ExecutionId::new());
    }
}
