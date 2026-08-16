use std::{collections::HashMap, time::Duration};

use crate::{
    id::WorkflowTaskId,
    workflow::{Workflow, WorkflowDefinition, WorkflowTask},
};

use super::{
    dto::{TaskConfigSpec, WorkflowSpec},
    error::SpecError,
};

fn resolve_config(
    config: &TaskConfigSpec,
    task_name: &str,
    name_to_id: &HashMap<String, WorkflowTaskId>,
) -> Result<TaskConfigSpec, SpecError> {
    let mut config = config.clone();

    if let TaskConfigSpec::Shell { inputs, .. } = &mut config {
        for input in inputs.iter_mut() {
            let resolved_id = name_to_id.get(&input.from_task).copied().ok_or_else(|| {
                SpecError::UnknownArtifactSource(task_name.to_owned(), input.from_task.clone())
            })?;

            input.from_task = resolved_id.to_string();
        }
    }

    Ok(config)
}

pub fn lower(spec: WorkflowSpec) -> Result<(Workflow, WorkflowDefinition), SpecError> {
    let name_to_id: HashMap<String, WorkflowTaskId> = spec
        .tasks
        .keys()
        .map(|name| (name.clone(), WorkflowTaskId::new()))
        .collect();

    let mut tasks = Vec::with_capacity(spec.tasks.len());

    for (name, task_spec) in &spec.tasks {
        let id = name_to_id[name];

        let dependencies = task_spec
            .depends_on
            .iter()
            .map(|dep_name| {
                name_to_id
                    .get(dep_name)
                    .copied()
                    .ok_or_else(|| SpecError::UnknownDependency(name.clone(), dep_name.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let task_type = task_type_name(&task_spec.config);

        let resolved_config = resolve_config(&task_spec.config, name, &name_to_id)?;
        let config = serde_json::to_value(&resolved_config)?;

        let mut task = WorkflowTask::new(id, task_type.to_owned(), dependencies)?;
        task = task.with_config(config);

        if let Some(timeout) = task_spec.timeout {
            task = task.with_timeout(Duration::from_secs(timeout));
        }

        tasks.push(task);
    }

    let mut definition = WorkflowDefinition::new(tasks)?;

    if let Some(timeout) = spec.timeout {
        definition = definition.with_timeout(Duration::from_secs(timeout));
    }

    let mut workflow = Workflow::new(spec.name)?;
    workflow.add_version(definition.clone())?;

    Ok((workflow, definition))
}

fn task_type_name(config: &TaskConfigSpec) -> &'static str {
    match config {
        TaskConfigSpec::Shell { .. } => "shell",
        TaskConfigSpec::Http { .. } => "http",
        TaskConfigSpec::Wait { .. } => "wait",
        TaskConfigSpec::Noop { .. } => "noop",
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::spec::dto::{StatusMatcher, TaskSpec};

    use super::*;

    fn noop_task() -> TaskSpec {
        TaskSpec {
            config: TaskConfigSpec::Noop { message: None },
            timeout: None,
            depends_on: Vec::new(),
        }
    }

    #[test]
    fn lower_builds_workflow_and_definition_with_matching_name_and_id() {
        let mut tasks = BTreeMap::new();
        tasks.insert("only".to_owned(), noop_task());

        let spec = WorkflowSpec {
            name: "example".to_owned(),
            timeout: None,
            tasks,
        };

        let (workflow, definition) = lower(spec).unwrap();

        assert_eq!(workflow.name(), "example");
        assert_eq!(definition.tasks().len(), 1);
        assert_eq!(workflow.versions().len(), 1);
        assert_eq!(workflow.versions()[0].definition(), &definition);
    }

    #[test]
    fn lower_resolves_named_dependencies_to_task_ids() {
        let mut tasks = BTreeMap::new();
        tasks.insert("first".to_owned(), noop_task());
        tasks.insert(
            "second".to_owned(),
            TaskSpec {
                config: TaskConfigSpec::Noop { message: None },
                timeout: None,
                depends_on: vec!["first".to_owned()],
            },
        );

        let spec = WorkflowSpec {
            name: "example".to_owned(),
            timeout: None,
            tasks,
        };

        let (_, definition) = lower(spec).unwrap();

        let first_id = definition
            .tasks()
            .iter()
            .find(|t| t.dependencies().is_empty())
            .unwrap()
            .id();
        let second = definition
            .tasks()
            .iter()
            .find(|t| !t.dependencies().is_empty())
            .unwrap();

        assert_eq!(second.dependencies(), &[first_id]);
    }

    #[test]
    fn lower_rejects_dependency_on_unknown_task() {
        let mut tasks = BTreeMap::new();
        tasks.insert(
            "only".to_owned(),
            TaskSpec {
                config: TaskConfigSpec::Noop { message: None },
                timeout: None,
                depends_on: vec!["missing".to_owned()],
            },
        );

        let spec = WorkflowSpec {
            name: "example".to_owned(),
            timeout: None,
            tasks,
        };

        let err = lower(spec).unwrap_err();

        match err {
            SpecError::UnknownDependency(task, dep) => {
                assert_eq!(task, "only");
                assert_eq!(dep, "missing");
            }
            other => panic!("expected UnknownDependency, got {other:?}"),
        }
    }

    #[test]
    fn lower_sets_task_and_definition_timeouts_from_spec() {
        let mut tasks = BTreeMap::new();
        tasks.insert(
            "only".to_owned(),
            TaskSpec {
                config: TaskConfigSpec::Noop { message: None },
                timeout: Some(30),
                depends_on: Vec::new(),
            },
        );

        let spec = WorkflowSpec {
            name: "example".to_owned(),
            timeout: Some(120),
            tasks,
        };

        let (_, definition) = lower(spec).unwrap();

        assert_eq!(definition.timeout(), Some(Duration::from_secs(120)));
        assert_eq!(
            definition.tasks()[0].timeout(),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn lower_defaults_timeouts_to_none_when_unset() {
        let mut tasks = BTreeMap::new();
        tasks.insert("only".to_owned(), noop_task());

        let spec = WorkflowSpec {
            name: "example".to_owned(),
            timeout: None,
            tasks,
        };

        let (_, definition) = lower(spec).unwrap();

        assert_eq!(definition.timeout(), None);
        assert_eq!(definition.tasks()[0].timeout(), None);
    }

    #[test]
    fn lower_serializes_task_config_into_the_workflow_task() {
        let mut tasks = BTreeMap::new();
        tasks.insert(
            "only".to_owned(),
            TaskSpec {
                config: TaskConfigSpec::Shell {
                    command: "echo hi".to_owned(),
                    env: HashMap::new(),
                    cwd: None,
                    shell: None,
                    success_codes: vec![StatusMatcher::Exact(0)],
                    outputs: Vec::new(),
                    inputs: Vec::new(),
                },
                timeout: None,
                depends_on: Vec::new(),
            },
        );

        let spec = WorkflowSpec {
            name: "example".to_owned(),
            timeout: None,
            tasks,
        };

        let (_, definition) = lower(spec).unwrap();
        let task = &definition.tasks()[0];

        assert_eq!(task.task_type(), "shell");
        assert_eq!(task.config()["command"], "echo hi");
    }

    #[test]
    fn task_type_name_maps_each_config_variant() {
        assert_eq!(
            task_type_name(&TaskConfigSpec::Shell {
                command: String::new(),
                env: HashMap::new(),
                cwd: None,
                shell: None,
                success_codes: Vec::new(),
                outputs: Vec::new(),
                inputs: Vec::new(),
            }),
            "shell"
        );
        assert_eq!(
            task_type_name(&TaskConfigSpec::Http {
                method: crate::spec::dto::HttpMethod::Get,
                url: String::new(),
                query: HashMap::new(),
                headers: HashMap::new(),
                body: None,
                success_codes: Vec::new(),
            }),
            "http"
        );
        assert_eq!(
            task_type_name(&TaskConfigSpec::Wait {
                duration: None,
                until: None,
            }),
            "wait"
        );
        assert_eq!(
            task_type_name(&TaskConfigSpec::Noop { message: None }),
            "noop"
        );
    }
}
