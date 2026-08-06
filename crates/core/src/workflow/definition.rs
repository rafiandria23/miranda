use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::{error::ExecutionError, id::WorkflowTaskId, workflow::WorkflowTask};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    #[serde(default, with = "crate::serde_util::duration_secs_opt")]
    timeout: Option<Duration>,

    tasks: Vec<WorkflowTask>,
}

impl WorkflowDefinition {
    pub fn new(tasks: Vec<WorkflowTask>) -> Result<Self, ExecutionError> {
        let definition = Self {
            timeout: None,
            tasks,
        };

        definition.validate()?;

        Ok(definition)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn effective_timeout(&self, task: &WorkflowTask) -> Option<Duration> {
        task.timeout().or(self.timeout)
    }

    pub fn tasks(&self) -> &[WorkflowTask] {
        &self.tasks
    }

    pub fn task(&self, task_id: WorkflowTaskId) -> Option<&WorkflowTask> {
        self.tasks.iter().find(|t| t.id() == task_id)
    }

    pub fn validate(&self) -> Result<(), ExecutionError> {
        self.validate_task_ids()?;
        self.validate_task_dependencies()?;
        self.validate_task_dependency_cycles()?;

        Ok(())
    }

    fn validate_task_ids(&self) -> Result<(), ExecutionError> {
        let mut task_ids = HashSet::new();

        for task in &self.tasks {
            if !task_ids.insert(task.id()) {
                return Err(ExecutionError::DuplicateTaskId(task.id()));
            }
        }

        Ok(())
    }

    fn validate_task_dependencies(&self) -> Result<(), ExecutionError> {
        let task_ids: HashSet<_> = self.tasks.iter().map(|t| t.id()).collect();

        for task in &self.tasks {
            for dependency in task.dependencies() {
                if !task_ids.contains(dependency) {
                    return Err(ExecutionError::UnknownDependency);
                }
            }
        }

        Ok(())
    }

    fn validate_task_dependency_cycles(&self) -> Result<(), ExecutionError> {
        let tasks: HashMap<_, _> = self.tasks.iter().map(|t| (t.id(), t)).collect();

        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();

        for task in &self.tasks {
            if !visited.contains(&task.id()) {
                Self::check_task_dependency_cycle(task.id(), &tasks, &mut visiting, &mut visited)?;
            }
        }

        Ok(())
    }

    fn check_task_dependency_cycle(
        task_id: WorkflowTaskId,
        tasks: &HashMap<WorkflowTaskId, &WorkflowTask>,
        visiting: &mut HashSet<WorkflowTaskId>,
        visited: &mut HashSet<WorkflowTaskId>,
    ) -> Result<(), ExecutionError> {
        if visiting.contains(&task_id) {
            return Err(ExecutionError::CyclicDependency);
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

    fn task(id: WorkflowTaskId, dependencies: Vec<WorkflowTaskId>) -> WorkflowTask {
        WorkflowTask::new(id, "send_email".to_owned(), dependencies).unwrap()
    }

    #[test]
    fn new_allows_an_empty_task_list() {
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        assert!(definition.tasks().is_empty());
    }

    #[test]
    fn new_stores_the_given_tasks() {
        let id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![task(id, vec![])]).unwrap();

        assert_eq!(definition.tasks().len(), 1);
        assert_eq!(definition.tasks()[0].id(), id);
    }

    #[test]
    fn new_rejects_duplicate_task_ids() {
        let id = WorkflowTaskId::new();

        let definition = WorkflowDefinition::new(vec![task(id, vec![]), task(id, vec![])]);

        assert_eq!(definition, Err(ExecutionError::DuplicateTaskId(id)));
    }

    #[test]
    fn new_rejects_dependency_on_unknown_task() {
        let id = WorkflowTaskId::new();
        let unknown_id = WorkflowTaskId::new();

        let definition = WorkflowDefinition::new(vec![task(id, vec![unknown_id])]);

        assert_eq!(definition, Err(ExecutionError::UnknownDependency));
    }

    #[test]
    fn new_rejects_direct_dependency_cycle() {
        let first_id = WorkflowTaskId::new();
        let second_id = WorkflowTaskId::new();

        let definition = WorkflowDefinition::new(vec![
            task(first_id, vec![second_id]),
            task(second_id, vec![first_id]),
        ]);

        assert_eq!(definition, Err(ExecutionError::CyclicDependency));
    }

    #[test]
    fn new_rejects_transitive_dependency_cycle() {
        let first_id = WorkflowTaskId::new();
        let second_id = WorkflowTaskId::new();
        let third_id = WorkflowTaskId::new();

        let definition = WorkflowDefinition::new(vec![
            task(first_id, vec![second_id]),
            task(second_id, vec![third_id]),
            task(third_id, vec![first_id]),
        ]);

        assert_eq!(definition, Err(ExecutionError::CyclicDependency));
    }

    #[test]
    fn new_allows_a_valid_dag() {
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let definition = WorkflowDefinition::new(vec![
            task(dependency_id, vec![]),
            task(task_id, vec![dependency_id]),
        ]);

        assert!(definition.is_ok());
    }

    #[test]
    fn task_returns_the_matching_task() {
        let id = WorkflowTaskId::new();
        let definition = WorkflowDefinition::new(vec![task(id, vec![])]).unwrap();

        assert_eq!(definition.task(id).unwrap().id(), id);
    }

    #[test]
    fn task_returns_none_for_unknown_id() {
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        assert!(definition.task(WorkflowTaskId::new()).is_none());
    }

    #[test]
    fn validate_succeeds_for_well_formed_definition() {
        let definition =
            WorkflowDefinition::new(vec![task(WorkflowTaskId::new(), vec![])]).unwrap();

        assert!(definition.validate().is_ok());
    }

    #[test]
    fn workflow_definition_round_trips_through_json() {
        let dependency_id = WorkflowTaskId::new();
        let task_id = WorkflowTaskId::new();

        let definition = WorkflowDefinition::new(vec![
            task(dependency_id, vec![]),
            task(task_id, vec![dependency_id]),
        ])
        .unwrap();

        let json = serde_json::to_string(&definition).unwrap();
        let deserialized: WorkflowDefinition = serde_json::from_str(&json).unwrap();

        assert_eq!(definition, deserialized);
    }

    #[test]
    fn new_defaults_timeout_to_none() {
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        assert_eq!(definition.timeout(), None);
    }

    #[test]
    fn with_timeout_sets_the_timeout() {
        let definition = WorkflowDefinition::new(vec![])
            .unwrap()
            .with_timeout(Duration::from_secs(30));

        assert_eq!(definition.timeout(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn workflow_definition_with_timeout_round_trips_through_json() {
        let definition = WorkflowDefinition::new(vec![])
            .unwrap()
            .with_timeout(Duration::from_secs(30));

        let json = serde_json::to_string(&definition).unwrap();
        let deserialized: WorkflowDefinition = serde_json::from_str(&json).unwrap();

        assert_eq!(definition, deserialized);
        assert_eq!(deserialized.timeout(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn workflow_definition_without_timeout_omits_it_from_json() {
        let definition = WorkflowDefinition::new(vec![]).unwrap();

        let json = serde_json::to_value(&definition).unwrap();

        assert_eq!(json["timeout"], serde_json::Value::Null);
    }

    #[test]
    fn workflow_definition_deserializes_without_timeout_field() {
        let deserialized: WorkflowDefinition = serde_json::from_str(r#"{"tasks":[]}"#).unwrap();

        assert_eq!(deserialized.timeout(), None);
    }

    #[test]
    fn effective_timeout_returns_none_when_neither_set() {
        let workflow_task = task(WorkflowTaskId::new(), vec![]);
        let definition = WorkflowDefinition::new(vec![workflow_task.clone()]).unwrap();

        assert_eq!(definition.effective_timeout(&workflow_task), None);
    }

    #[test]
    fn effective_timeout_falls_back_to_definition_timeout() {
        let workflow_task = task(WorkflowTaskId::new(), vec![]);
        let definition = WorkflowDefinition::new(vec![workflow_task.clone()])
            .unwrap()
            .with_timeout(Duration::from_secs(60));

        assert_eq!(
            definition.effective_timeout(&workflow_task),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn effective_timeout_prefers_task_timeout_over_definition_timeout() {
        let workflow_task =
            task(WorkflowTaskId::new(), vec![]).with_timeout(Duration::from_secs(10));
        let definition = WorkflowDefinition::new(vec![workflow_task.clone()])
            .unwrap()
            .with_timeout(Duration::from_secs(60));

        assert_eq!(
            definition.effective_timeout(&workflow_task),
            Some(Duration::from_secs(10))
        );
    }
}
