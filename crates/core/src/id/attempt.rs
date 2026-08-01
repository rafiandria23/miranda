use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttemptId(Uuid);

impl AttemptId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_unique_ids() {
        let first = AttemptId::new();
        let second = AttemptId::new();
        let third = AttemptId::new();

        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(third, first);
    }
}
