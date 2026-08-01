use std::collections::HashSet;

use crate::id::WorkflowTaskId;

use super::WorkflowTaskError;

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
    ) -> Result<Self, WorkflowTaskError> {
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

    pub fn validate(&self) -> Result<(), WorkflowTaskError> {
        self.validate_task_type()?;
        self.validate_dependencies()?;

        Ok(())
    }

    fn validate_task_type(&self) -> Result<(), WorkflowTaskError> {
        if self.task_type.trim().is_empty() {
            return Err(WorkflowTaskError::InvalidTaskType);
        }

        Ok(())
    }

    fn validate_dependencies(&self) -> Result<(), WorkflowTaskError> {
        let mut dependencies = HashSet::new();

        for dependency in &self.dependencies {
            if *dependency == self.id {
                return Err(WorkflowTaskError::SelfDependency);
            }

            if !dependencies.insert(*dependency) {
                return Err(WorkflowTaskError::DuplicateDependency);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow_task() {
        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        assert_eq!(task.task_type(), "send_email");
    }

    #[test]
    fn rejects_empty_task_type() {
        let task = WorkflowTask::new(WorkflowTaskId::new(), "".to_owned(), vec![]);

        assert!(task.is_err());
    }

    #[test]
    fn rejects_duplicate_dependency() {
        let dependency_id = WorkflowTaskId::new();

        let task = WorkflowTask::new(
            WorkflowTaskId::new(),
            "send_email".to_owned(),
            vec![dependency_id, dependency_id],
        );

        assert!(task.is_err());
    }

    #[test]
    fn rejects_self_dependency() {
        let task_id = WorkflowTaskId::new();

        let task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![task_id]);

        assert!(task.is_err());
    }
}
