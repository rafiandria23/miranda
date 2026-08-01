use std::collections::{HashMap, HashSet};

use crate::id::WorkflowTaskId;

use super::{WorkflowDefinitionError, WorkflowTask};

pub struct WorkflowDefinition {
    tasks: Vec<WorkflowTask>,
}

impl WorkflowDefinition {
    pub fn new(tasks: Vec<WorkflowTask>) -> Result<Self, WorkflowDefinitionError> {
        let definition = Self { tasks };

        definition.validate()?;

        Ok(definition)
    }

    pub fn tasks(&self) -> &[WorkflowTask] {
        &self.tasks
    }

    pub fn validate(&self) -> Result<(), WorkflowDefinitionError> {
        self.validate_task_ids()?;
        self.validate_task_dependencies()?;
        self.validate_task_dependency_cycles()?;

        Ok(())
    }

    fn validate_task_ids(&self) -> Result<(), WorkflowDefinitionError> {
        let mut task_ids = HashSet::new();

        for task in &self.tasks {
            if !task_ids.insert(task.id()) {
                return Err(WorkflowDefinitionError::DuplicateTaskId);
            }
        }

        Ok(())
    }

    fn validate_task_dependencies(&self) -> Result<(), WorkflowDefinitionError> {
        let task_ids: HashSet<_> = self.tasks.iter().map(|task| task.id()).collect();

        for task in &self.tasks {
            for dependency in task.dependencies() {
                if !task_ids.contains(dependency) {
                    return Err(WorkflowDefinitionError::UnknownDependency);
                }
            }
        }

        Ok(())
    }

    fn validate_task_dependency_cycles(&self) -> Result<(), WorkflowDefinitionError> {
        let tasks: HashMap<_, _> = self.tasks.iter().map(|task| (task.id(), task)).collect();

        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();

        for task in &self.tasks {
            if !visited.contains(&task.id()) {
                Self::check_task_dependency_cycle(task.id(), &tasks, &mut visiting, &mut visited)?;
            }
        }

        Ok(())
    }

    // DFS helper for validating task dependency cycles.
    fn check_task_dependency_cycle(
        task_id: WorkflowTaskId,
        tasks: &HashMap<WorkflowTaskId, &WorkflowTask>,
        visiting: &mut HashSet<WorkflowTaskId>,
        visited: &mut HashSet<WorkflowTaskId>,
    ) -> Result<(), WorkflowDefinitionError> {
        if visiting.contains(&task_id) {
            return Err(WorkflowDefinitionError::CyclicDependency);
        }

        if visited.contains(&task_id) {
            return Ok(());
        }

        visiting.insert(task_id);

        let task = tasks
            .get(&task_id)
            .expect("workflow task dependency should have been validated");

        for dependency in task.dependencies() {
            Self::check_task_dependency_cycle(*dependency, tasks, visiting, visited)?;
        }

        visiting.remove(&task_id);
        visited.insert(task_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_workflow_definition() {
        let task =
            WorkflowTask::new(WorkflowTaskId::new(), "send_email".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![task]).unwrap();

        assert_eq!(definition.tasks().len(), 1);
    }

    #[test]
    fn rejects_duplicate_task_id() {
        let task_id = WorkflowTaskId::new();

        let first_task = WorkflowTask::new(task_id, "send_email".to_owned(), vec![]).unwrap();
        let second_task = WorkflowTask::new(task_id, "send_sms".to_owned(), vec![]).unwrap();

        let definition = WorkflowDefinition::new(vec![first_task, second_task]);

        assert!(definition.is_err());
    }

    #[test]
    fn rejects_unknown_task_dependency() {
        let unknown_task_id = WorkflowTaskId::new();

        let task = WorkflowTask::new(
            WorkflowTaskId::new(),
            "send_email".to_owned(),
            vec![unknown_task_id],
        )
        .unwrap();

        let definition = WorkflowDefinition::new(vec![task]);

        assert!(definition.is_err());
    }

    #[test]
    fn rejects_cyclic_task_dependencies() {
        let first_task_id = WorkflowTaskId::new();
        let second_task_id = WorkflowTaskId::new();

        let first_task =
            WorkflowTask::new(first_task_id, "send_email".to_owned(), vec![second_task_id])
                .unwrap();
        let second_task =
            WorkflowTask::new(second_task_id, "send_sms".to_owned(), vec![first_task_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![first_task, second_task]);

        assert!(definition.is_err());
    }

    #[test]
    fn rejects_indirect_cyclic_task_dependencies() {
        let first_task_id = WorkflowTaskId::new();
        let second_task_id = WorkflowTaskId::new();
        let third_task_id = WorkflowTaskId::new();

        let first_task =
            WorkflowTask::new(first_task_id, "send_email".to_owned(), vec![second_task_id])
                .unwrap();
        let second_task =
            WorkflowTask::new(second_task_id, "send_sms".to_owned(), vec![third_task_id]).unwrap();
        let third_task =
            WorkflowTask::new(third_task_id, "send_mms".to_owned(), vec![first_task_id]).unwrap();

        let definition = WorkflowDefinition::new(vec![first_task, second_task, third_task]);

        assert!(definition.is_err());
    }
}
