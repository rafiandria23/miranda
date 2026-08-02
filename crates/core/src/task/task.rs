use crate::{
    Attempt,
    id::{ExecutionId, TaskId, WorkflowTaskId},
};

use super::{TaskError, TaskStatus};

#[derive(Debug)]
pub struct Task {
    id: TaskId,
    execution_id: ExecutionId,
    workflow_task_id: WorkflowTaskId,
    status: TaskStatus,
    attempts: Vec<Attempt>,
}

impl Task {
    pub fn new(execution_id: ExecutionId, workflow_task_id: WorkflowTaskId) -> Self {
        Self {
            id: TaskId::new(),
            execution_id,
            workflow_task_id,
            status: TaskStatus::Pending,
            attempts: Vec::new(),
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn workflow_task_id(&self) -> WorkflowTaskId {
        self.workflow_task_id
    }

    pub fn status(&self) -> TaskStatus {
        self.status
    }

    pub fn attempts(&self) -> &[Attempt] {
        &self.attempts
    }

    fn transition_to(&mut self, target: TaskStatus) -> Result<(), TaskError> {
        let valid = match (self.status, target) {
            (TaskStatus::Pending, TaskStatus::Running) => true,
            (TaskStatus::Pending, TaskStatus::Cancelled) => true,

            (TaskStatus::Running, TaskStatus::Completed) => true,
            (TaskStatus::Running, TaskStatus::Failed) => true,
            (TaskStatus::Running, TaskStatus::Cancelled) => true,

            (TaskStatus::Failed, TaskStatus::Running) => true,

            _ => false,
        };

        if !valid {
            return Err(TaskError::InvalidTransition {
                from: self.status,
                to: target,
            });
        }

        self.status = target;

        Ok(())
    }

    pub fn start(&mut self) -> Result<(), TaskError> {
        self.transition_to(TaskStatus::Running)?;
        self.add_attempt()?;

        Ok(())
    }

    pub fn complete(&mut self) -> Result<(), TaskError> {
        self.transition_to(TaskStatus::Completed)
    }

    pub fn fail(&mut self) -> Result<(), TaskError> {
        self.transition_to(TaskStatus::Failed)
    }

    pub fn cancel(&mut self) -> Result<(), TaskError> {
        self.transition_to(TaskStatus::Cancelled)
    }

    pub fn add_attempt(&mut self) -> Result<&Attempt, TaskError> {
        let number = self.attempts.len() as u32 + 1;
        let attempt = Attempt::new(self.id, number)?;

        self.attempts.push(attempt);

        Ok(self.attempts.last().expect("attempt was just added"))
    }
}

#[cfg(test)]
mod tests {
    use crate::AttemptStatus;

    use super::*;

    #[test]
    fn creates_task() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let task = Task::new(execution_id, workflow_task_id);

        assert_eq!(task.execution_id(), execution_id);
        assert_eq!(task.workflow_task_id(), workflow_task_id);
        assert_eq!(task.status(), TaskStatus::Pending);
        assert!(task.attempts().is_empty());
        assert_ne!(task.id(), TaskId::new());
    }

    #[test]
    fn pending_can_start() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();

        assert_eq!(task.status(), TaskStatus::Running);
    }

    #[test]
    fn running_can_complete() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.complete().unwrap();

        assert_eq!(task.status(), TaskStatus::Completed);
    }

    #[test]
    fn running_can_fail() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.fail().unwrap();

        assert_eq!(task.status(), TaskStatus::Failed);
    }

    #[test]
    fn running_can_cancel() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.cancel().unwrap();

        assert_eq!(task.status(), TaskStatus::Cancelled);
    }

    #[test]
    fn pending_cannot_complete() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        let err = task.complete().unwrap_err();

        assert!(matches!(
            err,
            TaskError::InvalidTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Completed,
            }
        ));
    }

    #[test]
    fn pending_cannot_fail() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        assert!(task.fail().is_err());
    }

    #[test]
    fn pending_can_cancel() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.cancel().unwrap();

        assert_eq!(task.status(), TaskStatus::Cancelled);
    }

    #[test]
    fn completed_cannot_transition() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.complete().unwrap();

        assert!(task.start().is_err());
        assert!(task.complete().is_err());
        assert!(task.fail().is_err());
        assert!(task.cancel().is_err());
    }

    #[test]
    fn failed_permits_only_restart() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.fail().unwrap();

        assert!(task.complete().is_err());
        assert!(task.fail().is_err());
        assert!(task.cancel().is_err());

        task.start().unwrap();
        assert_eq!(task.status(), TaskStatus::Running);
    }

    #[test]
    fn cancelled_cannot_transition() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.cancel().unwrap();

        assert!(task.start().is_err());
        assert!(task.complete().is_err());
        assert!(task.fail().is_err());
        assert!(task.cancel().is_err());
    }

    #[test]
    fn adds_first_attempt() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        let task_id = task.id();

        let attempt = task.add_attempt().unwrap();

        assert_eq!(attempt.task_id(), task_id);
        assert_eq!(attempt.number(), 1);
        assert_eq!(attempt.status(), AttemptStatus::Pending);
        assert_eq!(task.attempts().len(), 1);
    }

    #[test]
    fn adds_incrementing_attempts() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.add_attempt().unwrap();
        task.add_attempt().unwrap();

        assert_eq!(task.attempts().len(), 2);
        assert_eq!(task.attempts()[0].number(), 1);
        assert_eq!(task.attempts()[1].number(), 2);
    }

    // -------------------------------------------------------------------------
    // Retry
    // -------------------------------------------------------------------------

    #[test]
    fn failed_task_can_retry() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.fail().unwrap();
        task.start().unwrap();

        assert_eq!(task.status(), TaskStatus::Running);
        assert_eq!(task.attempts().len(), 2);
        assert_eq!(task.attempts()[0].number(), 1);
        assert_eq!(task.attempts()[1].number(), 2);
    }

    #[test]
    fn start_records_an_attempt() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();

        assert_eq!(task.attempts().len(), 1);
        assert_eq!(task.attempts()[0].number(), 1);
        assert_eq!(task.attempts()[0].status(), AttemptStatus::Pending);
    }

    #[test]
    fn completed_cannot_retry() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        task.start().unwrap();
        task.complete().unwrap();

        assert!(task.start().is_err());
    }
}
