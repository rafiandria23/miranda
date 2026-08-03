use serde::{Deserialize, Serialize};

use crate::{
    error::ExecutionError,
    execution::status::AttemptStatus,
    id::{AttemptId, TaskId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    id: AttemptId,
    task_id: TaskId,
    number: u32,
    status: AttemptStatus,
}

impl Attempt {
    pub fn new(task_id: TaskId, number: u32) -> Result<Self, ExecutionError> {
        let attempt = Self {
            id: AttemptId::new(),
            task_id,
            number,
            status: AttemptStatus::Pending,
        };

        attempt.validate()?;

        Ok(attempt)
    }

    pub fn id(&self) -> AttemptId {
        self.id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn number(&self) -> u32 {
        self.number
    }

    pub fn status(&self) -> AttemptStatus {
        self.status
    }

    pub fn validate(&self) -> Result<(), ExecutionError> {
        self.validate_number()?;
        Ok(())
    }

    fn validate_number(&self) -> Result<(), ExecutionError> {
        if self.number == 0 {
            return Err(ExecutionError::InvalidAttemptNumber);
        }
        Ok(())
    }

    fn transition_to(&mut self, target: AttemptStatus) -> Result<(), ExecutionError> {
        let valid = match (self.status, target) {
            (AttemptStatus::Pending, AttemptStatus::Running) => true,
            (AttemptStatus::Running, AttemptStatus::Succeeded) => true,
            (AttemptStatus::Running, AttemptStatus::Failed) => true,
            (AttemptStatus::Running, AttemptStatus::Cancelled) => true,
            _ => false,
        };

        if !valid {
            return Err(ExecutionError::InvalidAttemptTransition {
                from: self.status,
                to: target,
            });
        }

        self.status = target;
        Ok(())
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(AttemptStatus::Running)
    }

    pub fn succeed(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(AttemptStatus::Succeeded)
    }

    pub fn fail(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(AttemptStatus::Failed)
    }

    pub fn cancel(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(AttemptStatus::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_attempt() {
        let task_id = TaskId::new();
        let attempt = Attempt::new(task_id, 3).unwrap();

        assert_eq!(attempt.task_id(), task_id);
        assert_eq!(attempt.number(), 3);
        assert_eq!(attempt.status(), AttemptStatus::Pending);
        assert_ne!(attempt.id(), AttemptId::new());
    }

    #[test]
    fn rejects_zero_number() {
        let task_id = TaskId::new();
        let attempt = Attempt::new(task_id, 0);

        assert!(matches!(attempt, Err(ExecutionError::InvalidAttemptNumber)));
    }

    #[test]
    fn validates_attempt() {
        let task_id = TaskId::new();
        let attempt = Attempt::new(task_id, 3).unwrap();

        assert!(attempt.validate().is_ok());
    }

    #[test]
    fn pending_can_start() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        assert_eq!(attempt.status(), AttemptStatus::Running);
    }

    #[test]
    fn running_can_succeed() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        attempt.succeed().unwrap();
        assert_eq!(attempt.status(), AttemptStatus::Succeeded);
    }

    #[test]
    fn running_can_fail() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        attempt.fail().unwrap();
        assert_eq!(attempt.status(), AttemptStatus::Failed);
    }

    #[test]
    fn running_can_cancel() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        attempt.cancel().unwrap();
        assert_eq!(attempt.status(), AttemptStatus::Cancelled);
    }

    #[test]
    fn pending_cannot_succeed() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        let err = attempt.succeed().unwrap_err();

        assert!(matches!(
            err,
            ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Pending,
                to: AttemptStatus::Succeeded,
            }
        ));
    }

    #[test]
    fn terminal_states_cannot_transition() {
        let task_id = TaskId::new();

        let mut attempt = Attempt::new(task_id, 1).unwrap();
        attempt.start().unwrap();
        attempt.succeed().unwrap();

        assert!(attempt.start().is_err());
        assert!(attempt.succeed().is_err());
        assert!(attempt.fail().is_err());
        assert!(attempt.cancel().is_err());
    }
}
