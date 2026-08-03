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
    fn new_stores_task_id_and_number_and_starts_pending() {
        let task_id = TaskId::new();
        let attempt = Attempt::new(task_id, 3).unwrap();

        assert_eq!(attempt.task_id(), task_id);
        assert_eq!(attempt.number(), 3);
        assert_eq!(attempt.status(), AttemptStatus::Pending);
    }

    #[test]
    fn new_assigns_unique_ids_across_calls() {
        let task_id = TaskId::new();

        let first = Attempt::new(task_id, 1).unwrap();
        let second = Attempt::new(task_id, 1).unwrap();

        assert_ne!(first.id(), second.id());
    }

    #[test]
    fn new_rejects_zero_number() {
        let attempt = Attempt::new(TaskId::new(), 0);

        assert_eq!(attempt, Err(ExecutionError::InvalidAttemptNumber));
    }

    #[test]
    fn validate_succeeds_for_nonzero_number() {
        let attempt = Attempt::new(TaskId::new(), 1).unwrap();

        assert!(attempt.validate().is_ok());
    }

    #[test]
    fn start_transitions_pending_to_running() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();

        attempt.start().unwrap();

        assert_eq!(attempt.status(), AttemptStatus::Running);
    }

    #[test]
    fn succeed_transitions_running_to_succeeded() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        attempt.succeed().unwrap();

        assert_eq!(attempt.status(), AttemptStatus::Succeeded);
    }

    #[test]
    fn fail_transitions_running_to_failed() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        attempt.fail().unwrap();

        assert_eq!(attempt.status(), AttemptStatus::Failed);
    }

    #[test]
    fn cancel_transitions_running_to_cancelled() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        attempt.cancel().unwrap();

        assert_eq!(attempt.status(), AttemptStatus::Cancelled);
    }

    #[test]
    fn pending_cannot_succeed() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();

        let err = attempt.succeed().unwrap_err();

        assert_eq!(
            err,
            ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Pending,
                to: AttemptStatus::Succeeded,
            }
        );
    }

    #[test]
    fn pending_cannot_fail() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();

        let err = attempt.fail().unwrap_err();

        assert_eq!(
            err,
            ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Pending,
                to: AttemptStatus::Failed,
            }
        );
    }

    #[test]
    fn pending_cannot_cancel() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();

        let err = attempt.cancel().unwrap_err();

        assert_eq!(
            err,
            ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Pending,
                to: AttemptStatus::Cancelled,
            }
        );
    }

    #[test]
    fn running_cannot_restart() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        let err = attempt.start().unwrap_err();

        assert_eq!(
            err,
            ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Running,
                to: AttemptStatus::Running,
            }
        );
    }

    #[test]
    fn succeeded_is_terminal() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();
        attempt.succeed().unwrap();

        assert!(attempt.start().is_err());
        assert!(attempt.succeed().is_err());
        assert!(attempt.fail().is_err());
        assert!(attempt.cancel().is_err());
    }

    #[test]
    fn failed_is_terminal() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();
        attempt.fail().unwrap();

        assert!(attempt.start().is_err());
        assert!(attempt.succeed().is_err());
        assert!(attempt.fail().is_err());
        assert!(attempt.cancel().is_err());
    }

    #[test]
    fn cancelled_is_terminal() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();
        attempt.cancel().unwrap();

        assert!(attempt.start().is_err());
        assert!(attempt.succeed().is_err());
        assert!(attempt.fail().is_err());
        assert!(attempt.cancel().is_err());
    }

    #[test]
    fn attempt_round_trips_through_json() {
        let mut attempt = Attempt::new(TaskId::new(), 2).unwrap();
        attempt.start().unwrap();

        let json = serde_json::to_string(&attempt).unwrap();
        let deserialized: Attempt = serde_json::from_str(&json).unwrap();

        assert_eq!(attempt, deserialized);
    }
}
