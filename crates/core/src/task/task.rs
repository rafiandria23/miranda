use crate::id::{ExecutionId, TaskId};

use super::{TaskError, TaskStatus};

pub struct Task {
    id: TaskId,
    execution_id: ExecutionId,
    status: TaskStatus,
}

impl Task {
    pub fn new(execution_id: ExecutionId) -> Self {
        Self {
            id: TaskId::new(),
            execution_id,
            status: TaskStatus::Pending,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn status(&self) -> TaskStatus {
        self.status
    }

    fn transition_to(&mut self, target: TaskStatus) -> Result<(), TaskError> {
        let valid = match (self.status, target) {
            (TaskStatus::Pending, TaskStatus::Running) => true,

            (TaskStatus::Running, TaskStatus::Completed) => true,
            (TaskStatus::Running, TaskStatus::Failed) => true,
            (TaskStatus::Running, TaskStatus::Cancelled) => true,

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
        self.transition_to(TaskStatus::Running)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_task() {
        let execution_id = ExecutionId::new();

        let task = Task::new(execution_id);

        assert_eq!(task.status(), TaskStatus::Pending);
    }

    #[test]
    fn pending_can_start() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();

        assert_eq!(task.status(), TaskStatus::Running);
    }

    #[test]
    fn running_can_complete() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();
        task.complete().unwrap();

        assert_eq!(task.status(), TaskStatus::Completed);
    }

    #[test]
    fn running_can_fail() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();
        task.fail().unwrap();

        assert_eq!(task.status(), TaskStatus::Failed);
    }

    #[test]
    fn running_can_cancel() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();
        task.cancel().unwrap();

        assert_eq!(task.status(), TaskStatus::Cancelled);
    }

    #[test]
    fn pending_cannot_complete() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        assert!(task.complete().is_err());
    }

    #[test]
    fn pending_cannot_fail() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        assert!(task.fail().is_err());
    }

    #[test]
    fn completed_cannot_restart() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();
        task.complete().unwrap();

        assert!(task.start().is_err());
    }

    #[test]
    fn failed_cannot_restart() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();
        task.fail().unwrap();

        assert!(task.start().is_err());
    }

    #[test]
    fn cancelled_cannot_restart() {
        let execution_id = ExecutionId::new();

        let mut task = Task::new(execution_id);

        task.start().unwrap();
        task.cancel().unwrap();

        assert!(task.start().is_err());
    }
}
