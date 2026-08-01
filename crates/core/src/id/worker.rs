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
    fn creates_worker_id() {
        let worker_id = WorkerId::new();

        assert_ne!(worker_id, WorkerId::new());
    }
}
