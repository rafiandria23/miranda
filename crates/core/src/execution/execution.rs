use crate::{
    Task, TaskStatus, WorkflowDefinition, WorkflowTask,
    id::{ExecutionId, WorkflowTaskId, WorkflowVersionId},
};

use super::{ExecutionError, ExecutionStatus};

#[derive(Debug)]
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

        Ok(self.tasks.last().expect("task was just added"))
    }

    fn task(&self, workflow_task_id: WorkflowTaskId) -> Option<&Task> {
        self.tasks
            .iter()
            .find(|task| task.workflow_task_id() == workflow_task_id)
    }

    fn is_task_ready(&self, workflow_task: &WorkflowTask) -> bool {
        let Some(task) = self.task(workflow_task.id()) else {
            return false;
        };

        task.status() == TaskStatus::Pending
            && workflow_task.dependencies().iter().all(|dependency_id| {
                self.task(*dependency_id)
                    .is_some_and(|task| task.status() == TaskStatus::Completed)
            })
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
            .tasks
            .iter_mut()
            .find(|task| task.workflow_task_id() == workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.start()?;

        Ok(task)
    }

    pub fn complete_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.workflow_task_id() == workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.complete()?;

        Ok(task)
    }

    pub fn fail_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.workflow_task_id() == workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.fail()?;

        Ok(task)
    }

    pub fn cancel_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.workflow_task_id() == workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        task.cancel()?;

        Ok(task)
    }

    pub fn retry_task(
        &mut self,
        workflow_task_id: WorkflowTaskId,
    ) -> Result<&mut Task, ExecutionError> {
        if self.status != ExecutionStatus::Running {
            return Err(ExecutionError::ExecutionNotRunning);
        }

        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.workflow_task_id() == workflow_task_id)
            .ok_or(ExecutionError::UnknownTask(workflow_task_id))?;

        if task.status() != TaskStatus::Failed {
            return Err(ExecutionError::TaskNotRetryable(workflow_task_id));
        }

        task.start()?;

        Ok(task)
    }
}

#[cfg(test)]
mod tests {
    use crate::{TaskStatus, WorkflowDefinition, WorkflowTask};

    use super::*;

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

    // -------------------------------------------------------------------------
    // Execution lifecycle
    // -------------------------------------------------------------------------

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
    fn pending_can_cancel() {
        let workflow_version_id = WorkflowVersionId::new();
        let mut execution = Execution::new(workflow_version_id);

        execution.cancel().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Cancelled);
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

    // -------------------------------------------------------------------------
    // Runtime task creation
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Ready task discovery
    // -------------------------------------------------------------------------

    #[test]
    fn no_tasks_are_ready_when_definition_is_empty() {
        let workflow_version_id = WorkflowVersionId::new();
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        assert!(execution.ready_tasks(&definition).is_empty());
    }

    #[test]
    fn task_without_dependencies_is_ready() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![workflow_task_id]);
    }

    #[test]
    fn task_with_missing_runtime_dependency_is_not_ready() {
        let workflow_version_id = WorkflowVersionId::new();
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let dependency = WorkflowTask::new(dependency_id, "validate".to_owned(), vec![]).unwrap();

        let task =
            WorkflowTask::new(task_id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![dependency, task]).unwrap();

        let mut execution = Execution::new(workflow_version_id);
        execution.add_task(task_id).unwrap();

        assert!(execution.ready_tasks(&definition).is_empty());
    }

    #[test]
    fn task_with_incomplete_dependency_is_not_ready() {
        let workflow_version_id = WorkflowVersionId::new();
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let dependency = WorkflowTask::new(dependency_id, "validate".to_owned(), vec![]).unwrap();

        let task =
            WorkflowTask::new(task_id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![dependency, task]).unwrap();

        let execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![dependency_id]);
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
    fn task_with_multiple_dependencies_requires_all_dependencies_to_complete() {
        let workflow_version_id = WorkflowVersionId::new();
        let first_dependency_id = WorkflowTaskId::new();
        let second_dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let first_dependency =
            WorkflowTask::new(first_dependency_id, "validate".to_owned(), vec![]).unwrap();

        let second_dependency =
            WorkflowTask::new(second_dependency_id, "authorize".to_owned(), vec![]).unwrap();

        let task = WorkflowTask::new(
            task_id,
            "send_email".to_owned(),
            vec![first_dependency_id, second_dependency_id],
        )
        .unwrap();

        let definition =
            WorkflowDefinition::new(vec![first_dependency, second_dependency, task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        assert_eq!(
            execution.ready_tasks(&definition),
            vec![first_dependency_id, second_dependency_id]
        );

        execution
            .start_task(first_dependency_id, &definition)
            .unwrap();
        execution.complete_task(first_dependency_id).unwrap();

        assert_eq!(
            execution.ready_tasks(&definition),
            vec![second_dependency_id]
        );

        execution
            .start_task(second_dependency_id, &definition)
            .unwrap();
        execution.complete_task(second_dependency_id).unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![task_id]);
    }

    #[test]
    fn running_task_is_not_ready() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();

        assert!(execution.ready_tasks(&definition).is_empty());
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Running);
    }

    #[test]
    fn completed_task_is_not_ready() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.complete_task(workflow_task_id).unwrap();

        assert!(execution.ready_tasks(&definition).is_empty());
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Completed);
    }

    // -------------------------------------------------------------------------
    // Task start
    // -------------------------------------------------------------------------

    #[test]
    fn cannot_start_task_that_is_not_ready() {
        let workflow_version_id = WorkflowVersionId::new();
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let dependency = WorkflowTask::new(dependency_id, "validate".to_owned(), vec![]).unwrap();

        let task =
            WorkflowTask::new(task_id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![dependency, task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        let error = execution.start_task(task_id, &definition).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotReady(_)));
    }

    #[test]
    fn cannot_start_task_twice() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();

        let error = execution
            .start_task(workflow_task_id, &definition)
            .unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotReady(_)));
    }

    #[test]
    fn cannot_start_task_when_execution_is_not_running() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        let error = execution
            .start_task(workflow_task_id, &definition)
            .unwrap_err();

        assert!(matches!(error, ExecutionError::ExecutionNotRunning));
    }

    #[test]
    fn cannot_start_task_with_no_matching_runtime_task() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::new(workflow_version_id);
        execution.start().unwrap();

        let error = execution
            .start_task(workflow_task_id, &definition)
            .unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotReady(_)));
    }

    #[test]
    fn cannot_start_task_unknown_to_workflow_definition() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();
        let unknown_workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        let error = execution
            .start_task(unknown_workflow_task_id, &definition)
            .unwrap_err();

        assert!(matches!(
            error,
            ExecutionError::UnknownWorkflowTask(id) if id == unknown_workflow_task_id
        ));
    }

    // -------------------------------------------------------------------------
    // Task completion
    // -------------------------------------------------------------------------

    #[test]
    fn running_task_can_complete() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.complete_task(workflow_task_id).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Completed);
        assert!(execution.ready_tasks(&definition).is_empty());
    }

    #[test]
    fn pending_task_cannot_complete() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        let error = execution.complete_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskTransition(_)));
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Pending);
    }

    #[test]
    fn cannot_complete_task_twice() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.complete_task(workflow_task_id).unwrap();

        let error = execution.complete_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskTransition(_)));
    }

    #[test]
    fn cannot_complete_unknown_runtime_task() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);

        let error = execution.complete_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::UnknownTask(_)));
    }

    // -------------------------------------------------------------------------
    // Task failure
    // -------------------------------------------------------------------------

    #[test]
    fn running_task_can_fail() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.fail_task(workflow_task_id).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Failed);
        assert!(execution.ready_tasks(&definition).is_empty());
    }

    #[test]
    fn pending_task_cannot_fail() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        let error = execution.fail_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskTransition(_)));
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Pending);
    }

    #[test]
    fn cannot_fail_task_twice() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.fail_task(workflow_task_id).unwrap();

        let error = execution.fail_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskTransition(_)));
    }

    #[test]
    fn cannot_fail_unknown_runtime_task() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);

        let error = execution.fail_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::UnknownTask(_)));
    }

    // -------------------------------------------------------------------------
    // Task cancellation
    // -------------------------------------------------------------------------

    #[test]
    fn running_task_can_cancel() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.cancel_task(workflow_task_id).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Cancelled);
        assert!(execution.ready_tasks(&definition).is_empty());
    }

    #[test]
    fn pending_task_can_cancel() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        execution.cancel_task(workflow_task_id).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Cancelled);
    }

    #[test]
    fn cannot_cancel_task_twice() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.cancel_task(workflow_task_id).unwrap();

        let error = execution.cancel_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskTransition(_)));
    }

    #[test]
    fn cannot_cancel_unknown_runtime_task() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);

        let error = execution.cancel_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::UnknownTask(_)));
    }

    // -------------------------------------------------------------------------
    // Task retry
    // -------------------------------------------------------------------------

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

        execution.retry_task(workflow_task_id).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Running);
        assert_eq!(execution.tasks()[0].attempts().len(), 2);
        assert_eq!(execution.tasks()[0].attempts()[0].number(), 1);
        assert_eq!(execution.tasks()[0].attempts()[1].number(), 2);
    }

    #[test]
    fn retried_task_is_ready_for_completion_again() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.fail_task(workflow_task_id).unwrap();
        execution.retry_task(workflow_task_id).unwrap();
        execution.complete_task(workflow_task_id).unwrap();

        assert_eq!(execution.tasks()[0].status(), TaskStatus::Completed);
        assert!(execution.complete().is_ok());
    }

    #[test]
    fn pending_task_cannot_be_retried() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        let error = execution.retry_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotRetryable(_)));
        assert_eq!(execution.tasks()[0].status(), TaskStatus::Pending);
    }

    #[test]
    fn running_task_cannot_be_retried() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();

        let error = execution.retry_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotRetryable(_)));
    }

    #[test]
    fn completed_task_cannot_be_retried() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.complete_task(workflow_task_id).unwrap();

        let error = execution.retry_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotRetryable(_)));
    }

    #[test]
    fn cancelled_task_cannot_be_retried() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(workflow_task_id, &definition).unwrap();
        execution.cancel_task(workflow_task_id).unwrap();

        let error = execution.retry_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::TaskNotRetryable(_)));
    }

    #[test]
    fn cannot_retry_unknown_runtime_task() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let mut execution = Execution::new(workflow_version_id);
        execution.start().unwrap();

        let error = execution.retry_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::UnknownTask(_)));
    }

    #[test]
    fn cannot_retry_task_when_execution_is_not_running() {
        let workflow_version_id = WorkflowVersionId::new();
        let workflow_task_id = WorkflowTaskId::new();

        let workflow_task =
            WorkflowTask::new(workflow_task_id, "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![workflow_task]).unwrap();

        let mut execution = Execution::from_definition(workflow_version_id, &definition).unwrap();

        let error = execution.retry_task(workflow_task_id).unwrap_err();

        assert!(matches!(error, ExecutionError::ExecutionNotRunning));
    }
}
