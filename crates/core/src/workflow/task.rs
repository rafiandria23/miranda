use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::{error::ExecutionError, id::WorkflowTaskId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTask {
    id: WorkflowTaskId,
    task_type: String,
    dependencies: Vec<WorkflowTaskId>,
}

impl WorkflowTask {
    pub fn new(
        id: WorkflowTaskId,
        task_type: String,
        dependencies: Vec<WorkflowTaskId>,
    ) -> Result<Self, ExecutionError> {
        let task = Self {
            id,
            task_type,
            dependencies,
        };

        task.validate()?;
        Ok(task)
    }

    pub fn id(&self) -> WorkflowTaskId {
        self.id
    }

    pub fn task_type(&self) -> &str {
        &self.task_type
    }

    pub fn dependencies(&self) -> &[WorkflowTaskId] {
        &self.dependencies
    }

    pub fn validate(&self) -> Result<(), ExecutionError> {
        self.validate_task_type()?;
        self.validate_dependencies()?;
        Ok(())
    }

    fn validate_task_type(&self) -> Result<(), ExecutionError> {
        if self.task_type.trim().is_empty() {
            return Err(ExecutionError::InvalidTaskType);
        }
        Ok(())
    }

    fn validate_dependencies(&self) -> Result<(), ExecutionError> {
        let mut dependencies = HashSet::new();

        for dependency in &self.dependencies {
            if *dependency == self.id {
                return Err(ExecutionError::SelfDependency(self.id));
            }

            if !dependencies.insert(*dependency) {
                return Err(ExecutionError::DuplicateDependency(*dependency));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stores_id_task_type_and_dependencies() {
        let id = WorkflowTaskId::new();
        let dependency_id = WorkflowTaskId::new();

        let task = WorkflowTask::new(id, "send_email".to_owned(), vec![dependency_id]).unwrap();

        assert_eq!(task.id(), id);
        assert_eq!(task.task_type(), "send_email");
        assert_eq!(task.dependencies(), &[dependency_id]);
    }

    #[test]
    fn new_rejects_empty_task_type() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "".to_owned(), vec![]);

        assert_eq!(task, Err(ExecutionError::InvalidTaskType));
    }

    #[test]
    fn new_rejects_whitespace_task_type() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "   ".to_owned(), vec![]);

        assert_eq!(task, Err(ExecutionError::InvalidTaskType));
    }

    #[test]
    fn new_rejects_self_dependency() {
        let id = WorkflowTaskId::new();

        let task = WorkflowTask::new(id, "send_email".to_owned(), vec![id]);

        assert_eq!(task, Err(ExecutionError::SelfDependency(id)));
    }

    #[test]
    fn new_rejects_duplicate_dependency() {
        let id = WorkflowTaskId::new();
        let dependency_id = WorkflowTaskId::new();

        let task = WorkflowTask::new(
            id,
            "send_email".to_owned(),
            vec![dependency_id, dependency_id],
        );

        assert_eq!(
            task,
            Err(ExecutionError::DuplicateDependency(dependency_id))
        );
    }

    #[test]
    fn new_allows_no_dependencies() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]);

        assert!(task.is_ok());
    }

    #[test]
    fn new_allows_multiple_distinct_dependencies() {
        let id = WorkflowTaskId::new();
        let first_dependency = WorkflowTaskId::new();
        let second_dependency = WorkflowTaskId::new();

        let task = WorkflowTask::new(
            id,
            "send_email".to_owned(),
            vec![first_dependency, second_dependency],
        )
        .unwrap();

        assert_eq!(task.dependencies(), &[first_dependency, second_dependency]);
    }

    #[test]
    fn validate_succeeds_for_well_formed_task() {
        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        assert!(task.validate().is_ok());
    }

    #[test]
    fn workflow_task_round_trips_through_json() {
        let task = WorkflowTask::new(
            WorkflowTaskId::new(),
            "send_email".to_owned(),
            vec![WorkflowTaskId::new()],
        )
        .unwrap();

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: WorkflowTask = serde_json::from_str(&json).unwrap();

        assert_eq!(task, deserialized);
    }
}
