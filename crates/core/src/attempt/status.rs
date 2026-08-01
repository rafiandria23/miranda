#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}
