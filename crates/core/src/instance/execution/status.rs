use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Terminated,
}

impl ExecutionStatus {
    // Returns true if the execution has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Terminated
        )
    }

    // Returns true if the execution is currently running.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}
