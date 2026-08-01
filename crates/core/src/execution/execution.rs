use crate::id::{ExecutionId, WorkflowVersionId};

use super::{ExecutionError, ExecutionStatus};

pub struct Execution {
    id: ExecutionId,
    workflow_version_id: WorkflowVersionId,
    status: ExecutionStatus,
}

impl Execution {
    pub fn new(workflow_version_id: WorkflowVersionId) -> Self {
        Self {
            id: ExecutionId::new(),
            workflow_version_id,
            status: ExecutionStatus::Pending,
        }
    }

    pub fn id(&self) -> ExecutionId {
        self.id
    }

    pub fn workflow_version_id(&self) -> WorkflowVersionId {
        self.workflow_version_id
    }

    pub fn status(&self) -> ExecutionStatus {
        self.status
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        match self.status {
            ExecutionStatus::Pending => {
                self.status = ExecutionStatus::Running;

                Ok(())
            }

            status => Err(ExecutionError::InvalidTransition {
                from: status,
                to: ExecutionStatus::Running,
            }),
        }
    }

    fn transition_to(&mut self, target: ExecutionStatus) -> Result<(), ExecutionError> {
        let valid = match (self.status, target) {
            (ExecutionStatus::Pending, ExecutionStatus::Running) => true,

            (ExecutionStatus::Running, ExecutionStatus::Completed) => true,
            (ExecutionStatus::Running, ExecutionStatus::Failed) => true,
            (ExecutionStatus::Running, ExecutionStatus::Cancelled) => true,
            (ExecutionStatus::Running, ExecutionStatus::Terminated) => true,

            _ => false,
        };

        if !valid {
            return Err(ExecutionError::InvalidTransition {
                from: self.status,
                to: target,
            });
        }

        self.status = target;

        Ok(())
    }

    pub fn complete(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Completed)
    }

    pub fn fail(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Failed)
    }

    pub fn cancel(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Cancelled)
    }

    pub fn terminate(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Terminated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_execution() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        assert_eq!(execution.status(), ExecutionStatus::Pending);
    }

    #[test]
    fn pending_can_start() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Running);
    }

    #[test]
    fn running_can_complete() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.complete().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Completed);
    }

    #[test]
    fn running_can_fail() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.fail().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Failed);
    }

    #[test]
    fn running_can_cancel() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.cancel().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Cancelled);
    }

    #[test]
    fn running_can_terminate() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.terminate().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Terminated);
    }

    #[test]
    fn pending_cannot_complete() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        assert!(execution.complete().is_err());
    }

    #[test]
    fn pending_cannot_fail() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        assert!(execution.fail().is_err());
    }

    #[test]
    fn completed_cannot_restart() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.complete().unwrap();

        assert!(execution.start().is_err());
    }

    #[test]
    fn failed_cannot_restart() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.fail().unwrap();

        assert!(execution.start().is_err());
    }

    #[test]
    fn cancelled_cannot_restart() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.cancel().unwrap();

        assert!(execution.start().is_err());
    }

    #[test]
    fn terminated_cannot_restart() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.terminate().unwrap();

        assert!(execution.start().is_err());
    }
}
