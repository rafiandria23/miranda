pub mod attempt;
pub mod status;
pub mod task;

pub use attempt::Attempt;
pub use status::{AttemptStatus, ExecutionStatus, TaskStatus};
pub use task::Task;

use serde::{Deserialize, Serialize};

use crate::{
    error::ExecutionError,
    event::{Event, EventPayload},
    id::{ExecutionId, WorkflowTaskId, WorkflowVersionId},
    workflow::{WorkflowDefinition, WorkflowTask},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextAction {
    RunTasks(Vec<WorkflowTaskId>),
    Finished(ExecutionStatus),
    Deadlocked,
}

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

    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled
                | ExecutionStatus::Terminated
        )
    }

    pub fn recover_abandoned_tasks(
        &mut self,
        definition: &WorkflowDefinition,
    ) -> Result<Vec<WorkflowTaskId>, ExecutionError> {
        let tasks_to_recover: Vec<WorkflowTaskId> = self
            .tasks
            .iter()
            .filter(|t| t.status() == TaskStatus::Running)
            .filter_map(|t| {
                definition
                    .task(t.workflow_task_id())
                    .filter(|wf_t| self.are_task_dependencies_completed(wf_t))
                    .map(|wf_t| wf_t.id())
            })
            .collect();

        for workflow_task_id in &tasks_to_recover {
            if let Some(task) = self.task_mut(*workflow_task_id) {
                task.recover_from_abandonment()?;
            }
        }

        Ok(tasks_to_recover)
    }

    pub fn apply(
        &mut self,
        event: Event,
        definition: &WorkflowDefinition,
    ) -> Result<Event, ExecutionError> {
        if event.execution_id() != self.id {
            return Err(ExecutionError::InvalidIdFormat(
                "event execution_id mismatch".to_string(),
            ));
        }

        match event.payload() {
            // Execution lifecycle
            EventPayload::ExecutionStarted => self.start()?,
            EventPayload::ExecutionCompleted => self.complete()?,
            EventPayload::ExecutionFailed { .. } => self.fail()?,
            EventPayload::ExecutionCancelled => self.cancel()?,
            EventPayload::ExecutionTerminated => self.terminate()?,

            // Task lifecycle
            EventPayload::TaskStarted { workflow_task_id } => {
                self.start_task(*workflow_task_id, definition)?;
            }
            EventPayload::TaskCompleted { workflow_task_id } => {
                self.complete_task(*workflow_task_id)?;
            }
            EventPayload::TaskFailed {
                workflow_task_id, ..
            } => {
                self.fail_task(*workflow_task_id)?;
            }
            EventPayload::TaskCancelled { workflow_task_id } => {
                self.cancel_task(*workflow_task_id)?;
            }
            EventPayload::TaskRetried { workflow_task_id } => {
                self.retry_task(*workflow_task_id, definition)?;
            }

            // No-ops for initialization/internal events
            EventPayload::ExecutionCreated => {}
            EventPayload::TaskCreated { .. } => {}
            EventPayload::AttemptCreated { .. } => {}
            EventPayload::AttemptStarted { .. } => {}
            EventPayload::AttemptSucceeded { .. } => {}
            EventPayload::AttemptFailed { .. } => {}
            EventPayload::AttemptCancelled { .. } => {}
        }

        Ok(event)
    }

    pub fn next_action(&self, definition: &WorkflowDefinition) -> NextAction {
        if self.is_finished() {
            return NextAction::Finished(self.status);
        }

        let ready = self.ready_tasks(definition);

        if !ready.is_empty() {
            return NextAction::RunTasks(ready);
        }

        if self
            .tasks
            .iter()
            .all(|t| t.status() == TaskStatus::Completed)
        {
            return NextAction::Finished(self.status);
        }

        NextAction::Deadlocked
    }
}

#[cfg(test)]
mod tests {
    use crate::id::WorkflowTaskId;

    use super::*;

    fn single_task_definition() -> (WorkflowDefinition, WorkflowTaskId) {
        let task_id = WorkflowTaskId::new();
        let task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![]).unwrap();
        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        (definition, task_id)
    }

    fn chained_task_definition() -> (WorkflowDefinition, WorkflowTaskId, WorkflowTaskId) {
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let dependency = WorkflowTask::new(dependency_id, "validate".to_owned(), vec![]).unwrap();
        let task =
            WorkflowTask::new(task_id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![dependency, task]).unwrap();

        (definition, dependency_id, task_id)
    }

    #[test]
    fn new_starts_pending_with_no_tasks() {
        let workflow_version_id = WorkflowVersionId::new();
        let execution = Execution::new(workflow_version_id);

        assert_eq!(execution.workflow_version_id(), workflow_version_id);
        assert_eq!(execution.status(), ExecutionStatus::Pending);
        assert!(execution.tasks().is_empty());
    }

    #[test]
    fn from_definition_creates_a_task_per_workflow_task() {
        let (definition, task_id) = single_task_definition();

        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        assert_eq!(execution.tasks().len(), 1);
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Pending
        );
    }

    #[test]
    fn start_transitions_pending_to_running() {
        let mut execution = Execution::new(WorkflowVersionId::new());

        execution.start().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Running);
    }

    #[test]
    fn start_cannot_be_called_twice() {
        let mut execution = Execution::new(WorkflowVersionId::new());
        execution.start().unwrap();

        let err = execution.start().unwrap_err();

        assert_eq!(
            err,
            ExecutionError::InvalidExecutionTransition {
                from: ExecutionStatus::Running,
                to: ExecutionStatus::Running,
            }
        );
    }

    #[test]
    fn complete_fails_when_tasks_are_incomplete() {
        let (definition, _task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        let err = execution.complete().unwrap_err();

        assert_eq!(err, ExecutionError::IncompleteTasks);
        assert_eq!(execution.status(), ExecutionStatus::Running);
    }

    #[test]
    fn complete_succeeds_once_all_tasks_are_completed() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(task_id, &definition).unwrap();
        execution.complete_task(task_id).unwrap();
        execution.complete().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Completed);
    }

    #[test]
    fn fail_transitions_running_to_failed() {
        let mut execution = Execution::new(WorkflowVersionId::new());
        execution.start().unwrap();

        execution.fail().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Failed);
    }

    #[test]
    fn cancel_cancels_pending_and_running_tasks_and_the_execution() {
        let (definition, dependency_id, task_id) = chained_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(dependency_id, &definition).unwrap();
        execution.cancel().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Cancelled);
        assert_eq!(
            execution.task(dependency_id).unwrap().status(),
            TaskStatus::Cancelled
        );
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Cancelled
        );
    }

    #[test]
    fn terminate_transitions_running_to_terminated() {
        let mut execution = Execution::new(WorkflowVersionId::new());
        execution.start().unwrap();

        execution.terminate().unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Terminated);
    }

    #[test]
    fn add_task_appends_a_pending_task_for_the_workflow_task() {
        let mut execution = Execution::new(WorkflowVersionId::new());
        let workflow_task_id = WorkflowTaskId::new();

        execution.add_task(workflow_task_id).unwrap();

        let task = execution.task(workflow_task_id).unwrap();
        assert_eq!(task.execution_id(), execution.id());
        assert_eq!(task.status(), TaskStatus::Pending);
    }

    #[test]
    fn task_returns_none_for_unknown_workflow_task() {
        let execution = Execution::new(WorkflowVersionId::new());

        assert!(execution.task(WorkflowTaskId::new()).is_none());
    }

    #[test]
    fn ready_tasks_only_returns_tasks_whose_dependencies_are_completed() {
        let (definition, dependency_id, task_id) = chained_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![dependency_id]);

        execution.start().unwrap();
        execution.start_task(dependency_id, &definition).unwrap();
        execution.complete_task(dependency_id).unwrap();

        assert_eq!(execution.ready_tasks(&definition), vec![task_id]);
    }

    #[test]
    fn start_task_fails_when_execution_not_running() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let err = execution.start_task(task_id, &definition).unwrap_err();

        assert_eq!(err, ExecutionError::ExecutionNotRunning);
    }

    #[test]
    fn start_task_fails_for_unknown_workflow_task() {
        let (definition, _task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        let unknown_id = WorkflowTaskId::new();
        let err = execution.start_task(unknown_id, &definition).unwrap_err();

        assert_eq!(err, ExecutionError::UnknownWorkflowTask(unknown_id));
    }

    #[test]
    fn start_task_fails_when_dependencies_are_not_completed() {
        let (definition, _dependency_id, task_id) = chained_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        let err = execution.start_task(task_id, &definition).unwrap_err();

        assert_eq!(err, ExecutionError::TaskNotReady(task_id));
    }

    #[test]
    fn complete_task_fails_for_unknown_task() {
        let mut execution = Execution::new(WorkflowVersionId::new());

        let unknown_id = WorkflowTaskId::new();
        let err = execution.complete_task(unknown_id).unwrap_err();

        assert_eq!(err, ExecutionError::UnknownTask(unknown_id));
    }

    #[test]
    fn fail_task_transitions_running_task_to_failed() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();
        execution.start_task(task_id, &definition).unwrap();

        execution.fail_task(task_id).unwrap();

        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Failed
        );
    }

    #[test]
    fn cancel_task_fails_for_unknown_task() {
        let mut execution = Execution::new(WorkflowVersionId::new());

        let unknown_id = WorkflowTaskId::new();
        let err = execution.cancel_task(unknown_id).unwrap_err();

        assert_eq!(err, ExecutionError::UnknownTask(unknown_id));
    }

    #[test]
    fn retry_task_requires_execution_running() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let err = execution.retry_task(task_id, &definition).unwrap_err();

        assert_eq!(err, ExecutionError::ExecutionNotRunning);
    }

    #[test]
    fn retry_task_requires_task_to_be_failed() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        let err = execution.retry_task(task_id, &definition).unwrap_err();

        assert_eq!(err, ExecutionError::TaskNotRetryable(task_id));
    }

    #[test]
    fn retry_task_requires_dependencies_to_be_completed() {
        let (definition, _dependency_id, task_id) = chained_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        // Force `task_id` into `Failed` without completing its dependency, by
        // driving the task directly rather than through `execution.start_task`,
        // which would otherwise refuse this transition via `is_task_ready`.
        let task = execution.task_mut(task_id).unwrap();
        task.start().unwrap();
        task.fail().unwrap();

        let err = execution.retry_task(task_id, &definition).unwrap_err();

        assert_eq!(err, ExecutionError::TaskNotReady(task_id));
    }

    #[test]
    fn retry_task_restarts_a_failed_task() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(task_id, &definition).unwrap();
        execution.fail_task(task_id).unwrap();
        execution.retry_task(task_id, &definition).unwrap();

        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Running
        );
        assert_eq!(execution.task(task_id).unwrap().attempts().len(), 2);
    }

    #[test]
    fn is_finished_is_true_only_for_terminal_statuses() {
        let mut execution = Execution::new(WorkflowVersionId::new());
        assert!(!execution.is_finished());

        execution.start().unwrap();
        assert!(!execution.is_finished());

        execution.terminate().unwrap();
        assert!(execution.is_finished());
    }

    #[test]
    fn recover_abandoned_tasks_fails_running_tasks_with_completed_dependencies() {
        let (definition, dependency_id, task_id) = chained_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        execution.start_task(dependency_id, &definition).unwrap();
        execution.complete_task(dependency_id).unwrap();
        execution.start_task(task_id, &definition).unwrap();

        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Running
        );

        let recovered = execution.recover_abandoned_tasks(&definition).unwrap();

        assert_eq!(recovered, vec![task_id]);
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Failed
        );
    }

    #[test]
    fn recover_abandoned_tasks_leaves_tasks_whose_dependencies_are_incomplete() {
        let (definition, _dependency_id, task_id) = chained_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();

        // Force `task_id` into Running without completing its dependency, by
        // directly manipulating the underlying task since `start_task` would
        // otherwise refuse this transition via `is_task_ready`.
        execution.task_mut(task_id).unwrap().start().unwrap();

        let recovered = execution.recover_abandoned_tasks(&definition).unwrap();

        assert!(recovered.is_empty());
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Running
        );
    }

    #[test]
    fn apply_rejects_event_for_a_different_execution() {
        let (definition, _task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let foreign_event = Event::new(ExecutionId::new(), EventPayload::ExecutionStarted);
        let err = execution.apply(foreign_event, &definition).unwrap_err();

        assert!(matches!(err, ExecutionError::InvalidIdFormat(_)));
        assert_eq!(execution.status(), ExecutionStatus::Pending);
    }

    #[test]
    fn apply_execution_started_starts_the_execution() {
        let (definition, _task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let event = Event::new(execution.id(), EventPayload::ExecutionStarted);
        execution.apply(event, &definition).unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Running);
    }

    #[test]
    fn apply_task_started_then_completed_drives_task_lifecycle() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        execution
            .apply(
                Event::new(execution.id(), EventPayload::ExecutionStarted),
                &definition,
            )
            .unwrap();
        execution
            .apply(
                Event::new(
                    execution.id(),
                    EventPayload::TaskStarted {
                        workflow_task_id: task_id,
                    },
                ),
                &definition,
            )
            .unwrap();
        execution
            .apply(
                Event::new(
                    execution.id(),
                    EventPayload::TaskCompleted {
                        workflow_task_id: task_id,
                    },
                ),
                &definition,
            )
            .unwrap();

        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Completed
        );
    }

    #[test]
    fn apply_no_op_events_do_not_change_execution_state() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        let event = Event::new(
            execution.id(),
            EventPayload::TaskCreated {
                workflow_task_id: task_id,
            },
        );
        execution.apply(event, &definition).unwrap();

        assert_eq!(execution.status(), ExecutionStatus::Pending);
        assert_eq!(
            execution.task(task_id).unwrap().status(),
            TaskStatus::Pending
        );
    }

    #[test]
    fn apply_invalid_transition_fails() {
        let definition = WorkflowDefinition::new(vec![]).unwrap();
        let mut execution = Execution::new(WorkflowVersionId::new());

        let event = Event::new(execution.id(), EventPayload::ExecutionCompleted);
        let err = execution.apply(event, &definition).unwrap_err();

        assert!(matches!(
            err,
            ExecutionError::InvalidExecutionTransition { .. }
        ));
    }

    #[test]
    fn execution_round_trips_through_json() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();
        execution.start_task(task_id, &definition).unwrap();

        let json = serde_json::to_string(&execution).unwrap();
        let deserialized: Execution = serde_json::from_str(&json).unwrap();

        assert_eq!(execution, deserialized);
    }

    #[test]
    fn next_action_returns_finished_when_execution_is_finished() {
        let mut execution = Execution::new(WorkflowVersionId::new());
        let definition = WorkflowDefinition::new(vec![]).unwrap();
        execution.start().unwrap();
        execution.terminate().unwrap();

        assert_eq!(
            execution.next_action(&definition),
            NextAction::Finished(ExecutionStatus::Terminated)
        );
    }

    #[test]
    fn next_action_returns_run_tasks_when_tasks_are_ready() {
        let (definition, task_id) = single_task_definition();
        let execution = Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();

        assert_eq!(
            execution.next_action(&definition),
            NextAction::RunTasks(vec![task_id])
        );
    }

    #[test]
    fn next_action_returns_deadlocked_when_no_ready_tasks_and_not_finished() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();
        execution.start_task(task_id, &definition).unwrap();

        assert_eq!(execution.next_action(&definition), NextAction::Deadlocked);
    }

    #[test]
    fn next_action_returns_finished_when_all_tasks_completed_but_execution_not_yet_completed() {
        let (definition, task_id) = single_task_definition();
        let mut execution =
            Execution::from_definition(WorkflowVersionId::new(), &definition).unwrap();
        execution.start().unwrap();
        execution.start_task(task_id, &definition).unwrap();
        execution.complete_task(task_id).unwrap();

        assert_eq!(
            execution.next_action(&definition),
            NextAction::Finished(ExecutionStatus::Running)
        );
    }
}
