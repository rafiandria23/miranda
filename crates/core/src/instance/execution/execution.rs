use serde::{Deserialize, Serialize};

use crate::definition::{WorkflowDefinition, WorkflowTask};
use crate::error::ExecutionError;
use crate::id::{ExecutionId, WorkflowTaskId, WorkflowVersionId};
use crate::instance::task::{Task, TaskStatus};

use super::ExecutionStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn from_definition(
        workflow_version_id: WorkflowVersionId,
        definition: &WorkflowDefinition,
    ) -> Result<Self, ExecutionError> {
        let mut execution = Self::new(workflow_version_id);

        for workflow_task in definition.tasks() {
            execution.add_task(workflow_task.id())?;
        }

        Ok(execution)
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

    pub fn tasks_mut(&mut self) -> &mut [Task] {
        &mut self.tasks
    }

    fn transition_to(&mut self, target: ExecutionStatus) -> Result<(), ExecutionError> {
        let valid = match (self.status, target) {
            (ExecutionStatus::Pending, ExecutionStatus::Running) => true,
            (ExecutionStatus::Pending, ExecutionStatus::Cancelled) => true,

            (ExecutionStatus::Running, ExecutionStatus::Completed) => true,
            (ExecutionStatus::Running, ExecutionStatus::Failed) => true,
            (ExecutionStatus::Running, ExecutionStatus::Cancelled) => true,
            (ExecutionStatus::Running, ExecutionStatus::Terminated) => true,

            _ => false,
        };

        if !valid {
            return Err(ExecutionError::InvalidExecutionTransition {
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
        if self
            .tasks
            .iter()
            .any(|task| task.status() != TaskStatus::Completed)
        {
            return Err(ExecutionError::IncompleteTasks);
        }

        self.transition_to(ExecutionStatus::Completed)
    }

    pub fn fail(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Failed)
    }

    pub fn cancel(&mut self) -> Result<(), ExecutionError> {
        for task in &mut self.tasks {
            if matches!(task.status(), TaskStatus::Pending | TaskStatus::Running) {
                task.cancel()?;
            }
        }

        self.transition_to(ExecutionStatus::Cancelled)
    }

    pub fn terminate(&mut self) -> Result<(), ExecutionError> {
        self.transition_to(ExecutionStatus::Terminated)
    }

    pub fn add_task(&mut self, workflow_task_id: WorkflowTaskId) -> Result<&Task, ExecutionError> {
        let task = Task::new(self.id, workflow_task_id);

        self.tasks.push(task);

        Ok(self.tasks.last().expect("task was just appended"))
    }

    pub fn task(&self, workflow_task_id: WorkflowTaskId) -> Option<&Task> {
        self.tasks
            .iter()
            .find(|task| task.workflow_task_id() == workflow_task_id)
    }

    pub fn task_mut(&mut self, workflow_task_id: WorkflowTaskId) -> Option<&mut Task> {
        self.tasks
            .iter_mut()
            .find(|task| task.workflow_task_id() == workflow_task_id)
    }

    fn are_task_dependencies_completed(&self, workflow_task: &WorkflowTask) -> bool {
        workflow_task.dependencies().iter().all(|dependency_id| {
            self.task(*dependency_id)
                .is_some_and(|dependency_task| dependency_task.status() == TaskStatus::Completed)
        })
    }

    fn is_task_ready(&self, workflow_task: &WorkflowTask) -> bool {
        let Some(task) = self.task(workflow_task.id()) else {
            return false;
        };

        task.status() == TaskStatus::Pending && self.are_task_dependencies_completed(workflow_task)
    }

    pub fn ready_tasks(&self, definition: &WorkflowDefinition) -> Vec<WorkflowTaskId> {
        definition
            .tasks()
            .iter()
            .filter(|workflow_task| self.is_task_ready(workflow_task))
            .map(WorkflowTask::id)
            .collect()
    }

    pub fn start_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
        definition: &WorkflowDefinition,
    ) -> Result<&mut Task, ExecutionError> {
        if self.status != ExecutionStatus::Running {
            return Err(ExecutionError::ExecutionNotRunning);
        }

        let workflow_task = definition
            .task(workflow_task_id)
            .ok_or(ExecutionError::UnknownWorkflowTask(workflow_task_id))?;

        if !self.is_task_ready(workflow_task) {
            return Err(ExecutionError::TaskNotReady(workflow_task_id));
        }

        let task = self
            .task_mut(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.start()?;

        Ok(task)
    }

    pub fn complete_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        let task = self
            .task_mut(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.complete()?;
        Ok(task)
    }

    pub fn fail_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        let task = self
            .task_mut(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.fail()?;
        Ok(task)
    }

    pub fn cancel_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        let task = self
            .task_mut(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.cancel()?;
        Ok(task)
    }

    pub fn retry_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
        definition: &WorkflowDefinition,
    ) -> Result<&mut Task, ExecutionError> {
        if self.status != ExecutionStatus::Running {
            return Err(ExecutionError::ExecutionNotRunning);
        }

        let workflow_task = definition
            .task(workflow_task_id)
            .ok_or(ExecutionError::UnknownWorkflowTask(workflow_task_id))?;

        let task_status = self
            .task(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?
            .status();

        if task_status != TaskStatus::Failed {
            return Err(ExecutionError::TaskNotRetryable(workflow_task_id));
        }

        if !self.are_task_dependencies_completed(workflow_task) {
            return Err(ExecutionError::TaskNotReady(workflow_task_id));
        }

        let task = self
            .task_mut(workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.start()?;

        Ok(task)
    }

    // Returns `true` if the execution has reached a terminal status.
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled
                | ExecutionStatus::Terminated
        )
    }

    // Resets any tasks left in `Running` state back to `Pending` so they can be
    // rescheduled after worker node failover or orchestrator crash recovery.
    pub fn recover_abandoned_tasks(
        &mut self,
        definition: &WorkflowDefinition,
    ) -> Result<usize, ExecutionError> {
        // Phase 1: Identify tasks in Running state whose dependencies are fulfilled
        let tasks_to_recover: Vec<WorkflowTaskId> = self
            .tasks
            .iter()
            .filter(|task| task.status() == TaskStatus::Running)
            .filter_map(|task| {
                definition
                    .task(task.workflow_task_id())
                    .filter(|workflow_task| self.are_task_dependencies_completed(workflow_task))
                    .map(|workflow_task| workflow_task.id())
            })
            .collect();

        let recovered_count = tasks_to_recover.len();

        // Phase 2: Mutate the collected tasks back to Pending
        for workflow_task_id in tasks_to_recover {
            if let Some(task) = self.task_mut(workflow_task_id) {
                task.reset_to_pending()?;
            }
        }

        Ok(recovered_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::{WorkflowDefinition, WorkflowTask};
    use crate::instance::task::TaskStatus;

    #[test]
    fn creates_execution() {
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::new(workflow_version_id);

        assert_eq!(execution.status(), ExecutionStatus::Pending);
        assert!(execution.tasks().is_empty());
    }

    #[test]
    fn creates_execution_from_definition() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();
        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Pending);
        assert_eq!(execution.tasks().len(), 1);
        assert_eq!(execution.tasks()[0].workflow_task_id(), workflow_task_id);
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Pending);
    }

    #[test]
    fn pending_can_start() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.start().unwrap();
        assert_eq!(execution.status(), ExecutionStatus::Running);
    }

    #[test]
    fn cancel_cascades_to_pending_and_running_tasks() {
        let workflow_version_id = WorkflowVersionId::new();
        let pending_task_id = WorkflowTaskId::new();
        let running_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);
        execution.add_task(pending_task_id).unwrap();
        execution.add_task(running_task_id).unwrap();

        execution.start().unwrap();

        let definition = WorkflowDefinition::new(vec![
            WorkflowTask::new(pending_task_id, "validate".to_owned(), vec![]).unwrap(),
            WorkflowTask::new(running_task_id, "send_email".to_owned(), vec![]).unwrap(),
        ])
        .unwrap();

        execution.start_task(running_task_id, &definition).unwrap();
        execution.cancel().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Cancelled);
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Cancelled);
        assert_eq!(execution.tasks()[1].status(), TaskStatus::Cancelled);
    }

    #[test]
    fn cannot_complete_with_incomplete_tasks() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();
        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        execution.start().unwrap();
        let error = execution.complete().unwrap_err();

        assert!(matches!(error, ExecutionError::IncompleteTasks));
        assert_eq!(execution.status(), ExecutionStatus::Running);
    }

    #[test]
    fn task_becomes_ready_when_dependency_completes() {
        let workflow_version_id = WorkflowVersionId::new();
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let dependency = WorkflowTask::new(dependency_id, "validate".to_owned(), vec![]).unwrap();
        let task =
            WorkflowTask::new(task_id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![dependency, task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![dependency_id]);

        execution.start_task(dependency_id, &definition).unwrap();
        execution.complete_task(dependency_id).unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![task_id]);
    }

    #[test]
    fn failed_task_can_be_retried() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.fail_task(workflow_task_id).unwrap();
        execution.retry_task(workflow_task_id, &definition).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Running);
        assert_eq!(execution.tasks()[0].attempts().len(), 2);
    }

    #[test]
    fn recovers_abandoned_running_tasks() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "process_chunk".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        // Task starts running on worker
        execution.start_task(workflow_task_id, &definition).unwrap();
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Running);

        // Process crashes, recovery service runs:
        let recovered = execution.recover_abandoned_tasks(&definition).unwrap();

        assert_eq!(recovered, 1);
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Pending);
        // Task is immediately available for ready_tasks scheduling again
        assert_eq!(execution.ready_tasks(&definition), vec![workflow_task_id]);
    }
}
