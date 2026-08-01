use crate::id::{AttemptId, TaskId};

use super::{AttemptError, AttemptStatus};

pub struct Attempt {
    id: AttemptId,
    task_id: TaskId,
    number: u32,
    status: AttemptStatus,
}

impl Attempt {
    pub fn new(task_id: TaskId, number: u32) -> Result<Self, AttemptError> {
        if number == 0 {
            return Err(AttemptError::InvalidNumber);
        }

        Ok(Self {
            id: AttemptId::new(),
            task_id,
            number,
            status: AttemptStatus::Pending,
        })
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

    fn transition_to(&mut self, target: AttemptStatus) -> Result<(), AttemptError> {
        let valid = match (self.status, target) {
            (AttemptStatus::Pending, AttemptStatus::Running) => true,

            (AttemptStatus::Running, AttemptStatus::Succeeded) => true,
            (AttemptStatus::Running, AttemptStatus::Failed) => true,
            (AttemptStatus::Running, AttemptStatus::Cancelled) => true,

            _ => false,
        };

        if !valid {
            return Err(AttemptError::InvalidTransition {
                from: self.status,
                to: target,
            });
        }

        self.status = target;

        Ok(())
    }

    pub fn start(&mut self) -> Result<(), AttemptError> {
        self.transition_to(AttemptStatus::Running)
    }

    pub fn succeed(&mut self) -> Result<(), AttemptError> {
        self.transition_to(AttemptStatus::Succeeded)
    }

    pub fn fail(&mut self) -> Result<(), AttemptError> {
        self.transition_to(AttemptStatus::Failed)
    }

    pub fn cancel(&mut self) -> Result<(), AttemptError> {
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

        assert_eq!(attempt.number(), 3);
        assert_eq!(attempt.status(), AttemptStatus::Pending);
    }

    #[test]
    fn rejects_zero_number() {
        let task_id = TaskId::new();
        let attempt = Attempt::new(task_id, 0);

        assert!(attempt.is_err());
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

        assert!(attempt.succeed().is_err());
    }

    #[test]
    fn pending_cannot_fail() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        assert!(attempt.fail().is_err());
    }

    #[test]
    fn succeeded_cannot_restart() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        attempt.succeed().unwrap();

        assert!(attempt.start().is_err());
    }

    #[test]
    fn failed_cannot_restart() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        attempt.fail().unwrap();

        assert!(attempt.start().is_err());
    }

    #[test]
    fn cancelled_cannot_restart() {
        let task_id = TaskId::new();
        let mut attempt = Attempt::new(task_id, 3).unwrap();

        attempt.start().unwrap();
        attempt.cancel().unwrap();

        assert!(attempt.start().is_err());
    }
}
