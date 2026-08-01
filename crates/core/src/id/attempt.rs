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
    fn creates_attempt_id() {
        let attempt_id = AttemptId::new();

        assert_ne!(attempt_id, AttemptId::new());
    }
}
