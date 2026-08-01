use crate::{
    Task,
    id::{ExecutionId, WorkflowTaskId, WorkflowVersionId},
};

use super::{ExecutionError, ExecutionStatus};

pub struct Execution {
    id: ExecutionId,
    workflow_version_id: WorkflowVersionId,
    status: ExecutionStatus,
    tasks: Vec<Task>,
}

impl Execution {
    pub fn new(workflow_version_id: WorkflowVersionId) -> Self {
        Self {
            id: ExecutionId::new(),
            workflow_version_id,
            status: ExecutionStatus::Pending,
            tasks: Vec::new(),
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

    pub fn tasks(&self) -> &[Task] {
        &self.tasks
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

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Running)
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

    pub fn add_task(&mut self, workflow_task_id: WorkflowTaskId) -> Result<&Task, ExecutionError> {
        let task = Task::new(self.id, workflow_task_id);

        self.tasks.push(task);

        Ok(self.tasks.last().expect("task was just added"))
    }
}

#[cfg(test)]
mod tests {
    use crate::TaskStatus;

    use super::*;

    #[test]
    fn creates_execution() {
        let workflow_version_id = WorkflowVersionId::new();

        let execution = Execution::new(workflow_version_id);

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
    fn pending_cannot_cancel() {
        let workflow_version_id = WorkflowVersionId::new();

        let mut execution = Execution::new(workflow_version_id);

        assert!(execution.cancel().is_err());
    }

    #[test]
    fn pending_cannot_terminate() {
        let workflow_version_id = WorkflowVersionId::new();

        let mut execution = Execution::new(workflow_version_id);

        assert!(execution.terminate().is_err());
    }

    #[test]
    fn completed_cannot_transition() {
        let workflow_version_id = WorkflowVersionId::new();

        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.complete().unwrap();

        assert!(execution.start().is_err());
        assert!(execution.complete().is_err());
        assert!(execution.fail().is_err());
        assert!(execution.cancel().is_err());
        assert!(execution.terminate().is_err());
    }

    #[test]
    fn failed_cannot_transition() {
        let workflow_version_id = WorkflowVersionId::new();

        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.fail().unwrap();

        assert!(execution.start().is_err());
        assert!(execution.complete().is_err());
        assert!(execution.fail().is_err());
        assert!(execution.cancel().is_err());
        assert!(execution.terminate().is_err());
    }

    #[test]
    fn cancelled_cannot_transition() {
        let workflow_version_id = WorkflowVersionId::new();

        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.cancel().unwrap();

        assert!(execution.start().is_err());
        assert!(execution.complete().is_err());
        assert!(execution.fail().is_err());
        assert!(execution.cancel().is_err());
        assert!(execution.terminate().is_err());
    }

    #[test]
    fn terminated_cannot_transition() {
        let workflow_version_id = WorkflowVersionId::new();

        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        execution.terminate().unwrap();

        assert!(execution.start().is_err());
        assert!(execution.complete().is_err());
        assert!(execution.fail().is_err());
        assert!(execution.cancel().is_err());
        assert!(execution.terminate().is_err());
    }

    #[test]
    fn adds_task() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);

        let execution_id = execution.id();

        let task = execution.add_task(workflow_task_id).unwrap();

        assert_eq!(task.execution_id(), execution_id);
        assert_eq!(task.workflow_task_id(), workflow_task_id);
        assert_eq!(task.status(), TaskStatus::Pending);
        assert_eq!(execution.tasks().len(), 1);
    }

    #[test]
    fn adds_multiple_tasks() {
        let workflow_version_id = WorkflowVersionId::new();
        let first_workflow_task_id = WorkflowTaskId::new();
        let second_workflow_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);

        execution.add_task(first_workflow_task_id).unwrap();
        execution.add_task(second_workflow_task_id).unwrap();

        assert_eq!(execution.tasks().len(), 2);
        assert_eq!(
            execution.tasks()[0].workflow_task_id(),
            first_workflow_task_id
        );
        assert_eq!(
            execution.tasks()[1].workflow_task_id(),
            second_workflow_task_id
        );
    }
}
