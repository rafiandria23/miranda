use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_unique_ids() {
        let first = WorkerId::new();
        let second = WorkerId::new();
        let third = WorkerId::new();

        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(third, first);
    }
}
