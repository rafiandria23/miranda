use serde::{Deserialize, Serialize};

use crate::error::ExecutionError;
use crate::id::{ExecutionId, TaskId, WorkflowTaskId};
use crate::instance::attempt::Attempt;

use super::TaskStatus;

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

    // Resets a task currently in `Running` back to `Pending` state.
    //
    // This is used during crash recovery when an execution is hydrated after
    // a worker process crash while tasks were still in-flight.
    pub fn reset_to_pending(&mut self) -> Result<(), ExecutionError> {
        if self.status != TaskStatus::Running {
            return Err(ExecutionError::InvalidTaskTransition {
                from: self.status,
                to: TaskStatus::Pending,
            });
        }

        self.status = TaskStatus::Pending;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(task.attempts().len(), 1);
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
    fn pending_cannot_complete() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);

        let err = task.complete().unwrap_err();

        assert!(matches!(
            err,
            ExecutionError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Completed,
            }
        ));
    }

    #[test]
    fn running_can_reset_to_pending() {
        let execution_id = ExecutionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut task = Task::new(execution_id, workflow_task_id);
        task.start().unwrap();

        assert_eq!(task.status(), TaskStatus::Running);
        task.reset_to_pending().unwrap();
        assert_eq!(task.status(), TaskStatus::Pending);
    }
}
