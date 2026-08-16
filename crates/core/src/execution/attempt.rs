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
        let valid = matches!(
            (self.status, target),
            (AttemptStatus::Pending, AttemptStatus::Running)
                | (AttemptStatus::Running, AttemptStatus::Succeeded)
                | (AttemptStatus::Running, AttemptStatus::Failed)
                | (AttemptStatus::Running, AttemptStatus::Cancelled)
        );

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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_assigns_a_fresh_id_and_stores_task_id_number_and_pending_status() {
        let task_id = TaskId::new();

        let attempt = Attempt::new(task_id, 1).unwrap();

        assert_eq!(attempt.task_id(), task_id);
        assert_eq!(attempt.number(), 1);
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
        let task_id = TaskId::new();

        let attempt = Attempt::new(task_id, 0);

        assert_eq!(attempt, Err(ExecutionError::InvalidAttemptNumber));
    }

    #[test]
    fn validate_succeeds_for_nonzero_number() {
        let task_id = TaskId::new();
        let attempt = Attempt::new(task_id, 1).unwrap();

        assert!(attempt.validate().is_ok());
    }

    #[test]
    fn start_transitions_pending_to_running() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();

        assert!(attempt.start().is_ok());
        assert_eq!(attempt.status(), AttemptStatus::Running);
    }

    #[test]
    fn succeed_transitions_running_to_succeeded() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        assert!(attempt.succeed().is_ok());
        assert_eq!(attempt.status(), AttemptStatus::Succeeded);
    }

    #[test]
    fn fail_transitions_running_to_failed() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        assert!(attempt.fail().is_ok());
        assert_eq!(attempt.status(), AttemptStatus::Failed);
    }

    #[test]
    fn cancel_transitions_running_to_cancelled() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        assert!(attempt.cancel().is_ok());
        assert_eq!(attempt.status(), AttemptStatus::Cancelled);
    }

    #[test]
    fn start_rejects_transition_from_non_pending_status() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();
        attempt.start().unwrap();

        let result = attempt.start();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Running,
                to: AttemptStatus::Running,
            })
        );
    }

    #[test]
    fn succeed_rejects_transition_from_pending_status() {
        let mut attempt = Attempt::new(TaskId::new(), 1).unwrap();

        let result = attempt.succeed();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Pending,
                to: AttemptStatus::Succeeded,
            })
        );
    }

    #[test]
    fn transitions_out_of_terminal_statuses_are_rejected() {
        let mut succeeded = Attempt::new(TaskId::new(), 1).unwrap();
        succeeded.start().unwrap();
        succeeded.succeed().unwrap();

        assert_eq!(
            succeeded.fail(),
            Err(ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Succeeded,
                to: AttemptStatus::Failed,
            })
        );

        let mut failed = Attempt::new(TaskId::new(), 1).unwrap();
        failed.start().unwrap();
        failed.fail().unwrap();

        assert_eq!(
            failed.cancel(),
            Err(ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Failed,
                to: AttemptStatus::Cancelled,
            })
        );

        let mut cancelled = Attempt::new(TaskId::new(), 1).unwrap();
        cancelled.start().unwrap();
        cancelled.cancel().unwrap();

        assert_eq!(
            cancelled.start(),
            Err(ExecutionError::InvalidAttemptTransition {
                from: AttemptStatus::Cancelled,
                to: AttemptStatus::Running,
            })
        );
    }

    #[test]
    fn attempt_round_trips_through_json() {
        let attempt = Attempt::new(TaskId::new(), 2).unwrap();

        let json = serde_json::to_string(&attempt).unwrap();
        let deserialized: Attempt = serde_json::from_str(&json).unwrap();

        assert_eq!(attempt, deserialized);
    }
}
