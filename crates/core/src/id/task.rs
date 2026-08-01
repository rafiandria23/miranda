use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_unique_ids() {
        let first = TaskId::new();
        let second = TaskId::new();
        let third = TaskId::new();

        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(third, first);
    }
}
