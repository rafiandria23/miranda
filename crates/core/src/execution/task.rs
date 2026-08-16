use serde::{Deserialize, Serialize};

use crate::{
    error::ExecutionError,
    execution::{attempt::Attempt, status::TaskStatus},
    id::{ExecutionId, TaskId, WorkflowTaskId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn latest_attempt(&self) -> Option<&Attempt> {
        self.attempts.last()
    }

    pub fn latest_attempt_mut(&mut self) -> Option<&mut Attempt> {
        self.attempts.last_mut()
    }

    fn transition_to(&mut self, target: TaskStatus) -> Result<(), ExecutionError> {
        let valid = match (self.status, target) {
            (TaskStatus::Pending, TaskStatus::Running) => true,
            (TaskStatus::Pending, TaskStatus::Cancelled) => true,

            (TaskStatus::Running, TaskStatus::Completed) => true,
            (TaskStatus::Running, TaskStatus::Failed) => true,
            (TaskStatus::Running, TaskStatus::Cancelled) => true,

            // Retry logic
            (TaskStatus::Failed, TaskStatus::Running) => true,

            _ => false,
        };

        if !valid {
            return Err(ExecutionError::InvalidTaskTransition {
                from: self.status,
                to: target,
            });
        }

        self.status = target;

        Ok(())
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(TaskStatus::Running)?;

        self.add_attempt()?;

        if let Some(attempt) = self.latest_attempt_mut() {
            attempt.start()?;
        }

        Ok(())
    }

    pub fn complete(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(TaskStatus::Completed)?;

        if let Some(attempt) = self.latest_attempt_mut() {
            attempt.succeed()?;
        }

        Ok(())
    }

    pub fn fail(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(TaskStatus::Failed)?;

        if let Some(attempt) = self.latest_attempt_mut() {
            attempt.fail()?;
        }

        Ok(())
    }

    pub fn cancel(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(TaskStatus::Cancelled)?;

        if let Some(attempt) = self.latest_attempt_mut() {
            attempt.cancel()?;
        }

        Ok(())
    }

    pub fn add_attempt(&mut self) -> Result<&Attempt, ExecutionError> {
        let number = self.attempts.len() as u32 + 1;
        let attempt = Attempt::new(self.id, number)?;

        self.attempts.push(attempt);

        Ok(self.attempts.last().expect("attempt was just appended"))
    }

    pub fn recover_from_abandonment(&mut self) -> Result<(), ExecutionError> {
        if self.status != TaskStatus::Running {
            return Err(ExecutionError::InvalidTaskTransition {
                from: self.status,
                to: TaskStatus::Failed,
            });
        }

        if let Some(attempt) = self.latest_attempt_mut() {
            attempt.fail()?;
        }

        self.transition_to(TaskStatus::Failed)?;

        Ok(())
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::status::AttemptStatus;

    #[test]
    fn new_starts_pending_with_no_attempts() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let task = Task::new(execution_id, workflow_task_id);

        assert_eq!(task.execution_id(), execution_id);
        assert_eq!(task.workflow_task_id(), workflow_task_id);
        assert_eq!(task.status(), TaskStatus::Pending);
        assert!(task.attempts().is_empty());
        assert!(task.latest_attempt().is_none());
    }

    #[test]
    fn new_assigns_unique_ids_across_calls() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let first = Task::new(execution_id, workflow_task_id);
        let second = Task::new(execution_id, workflow_task_id);

        assert_ne!(first.id(), second.id());
    }

    #[test]
    fn start_transitions_to_running_and_adds_a_running_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        assert!(task.start().is_ok());

        assert_eq!(task.status(), TaskStatus::Running);
        assert_eq!(task.attempts().len(), 1);
        assert_eq!(task.latest_attempt().unwrap().number(), 1);
        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Running
        );
    }

    #[test]
    fn start_rejects_transition_from_completed() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();
        task.complete().unwrap();

        let result = task.start();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidTaskTransition {
                from: TaskStatus::Completed,
                to: TaskStatus::Running,
            })
        );
    }

    #[test]
    fn complete_transitions_to_completed_and_succeeds_latest_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();

        assert!(task.complete().is_ok());

        assert_eq!(task.status(), TaskStatus::Completed);
        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Succeeded
        );
    }

    #[test]
    fn complete_rejects_transition_from_pending() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        let result = task.complete();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Completed,
            })
        );
    }

    #[test]
    fn fail_transitions_to_failed_and_fails_latest_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();

        assert!(task.fail().is_ok());

        assert_eq!(task.status(), TaskStatus::Failed);
        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Failed
        );
    }

    #[test]
    fn fail_rejects_transition_from_pending() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        let result = task.fail();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Failed,
            })
        );
    }

    #[test]
    fn cancel_from_pending_transitions_without_an_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        assert!(task.cancel().is_ok());

        assert_eq!(task.status(), TaskStatus::Cancelled);
        assert!(task.attempts().is_empty());
    }

    #[test]
    fn cancel_from_running_cancels_latest_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();

        assert!(task.cancel().is_ok());

        assert_eq!(task.status(), TaskStatus::Cancelled);
        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Cancelled
        );
    }

    #[test]
    fn cancel_from_terminal_state_fails() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();
        task.complete().unwrap();

        let result = task.cancel();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidTaskTransition {
                from: TaskStatus::Completed,
                to: TaskStatus::Cancelled,
            })
        );
    }

    #[test]
    fn failed_task_can_restart_and_accumulates_attempts() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();
        task.fail().unwrap();

        task.start().unwrap();

        assert_eq!(task.status(), TaskStatus::Running);
        assert_eq!(task.attempts().len(), 2);
        assert_eq!(task.latest_attempt().unwrap().number(), 2);
        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Running
        );
    }

    #[test]
    fn add_attempt_appends_incrementing_attempt_numbers() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        task.add_attempt().unwrap();
        task.add_attempt().unwrap();

        assert_eq!(task.attempts().len(), 2);
        assert_eq!(task.attempts()[0].number(), 1);
        assert_eq!(task.attempts()[1].number(), 2);
    }

    #[test]
    fn latest_attempt_mut_allows_mutating_the_last_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();

        task.latest_attempt_mut().unwrap().succeed().unwrap();

        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Succeeded
        );
    }

    #[test]
    fn latest_attempt_mut_returns_none_when_there_are_no_attempts() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        assert!(task.latest_attempt_mut().is_none());
    }

    #[test]
    fn recover_from_abandonment_fails_a_running_task_and_its_latest_attempt() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();

        assert!(task.recover_from_abandonment().is_ok());

        assert_eq!(task.status(), TaskStatus::Failed);
        assert_eq!(
            task.latest_attempt().unwrap().status(),
            AttemptStatus::Failed
        );
    }

    #[test]
    fn recover_from_abandonment_fails_when_task_is_not_running() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());

        let result = task.recover_from_abandonment();

        assert_eq!(
            result,
            Err(ExecutionError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Failed,
            })
        );
    }

    #[test]
    fn task_round_trips_through_json() {
        let mut task = Task::new(ExecutionId::new(), WorkflowTaskId::new());
        task.start().unwrap();

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: Task = serde_json::from_str(&json).unwrap();

        assert_eq!(task, deserialized);
    }
}
