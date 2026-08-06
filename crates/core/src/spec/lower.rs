use std::{collections::HashMap, time::Duration};

use crate::{
    id::WorkflowTaskId,
    workflow::{Workflow, WorkflowDefinition, WorkflowTask},
};

use super::{dto::WorkflowSpec, error::SpecError};

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

        let mut task = WorkflowTask::new(id, task_spec.task_type.clone(), dependencies)?;

        if let Some(timeout) = task_spec.timeout {
            task = task.with_timeout(Duration::from_secs(timeout));
        }

        tasks.push(task);
    }

    let mut definition = WorkflowDefinition::new(tasks)?;

    if let Some(timeout) = spec.timeout {
        definition = definition.with_timeout(Duration::from_secs(timeout));
    };

    let mut workflow = Workflow::new(spec.name)?;
    workflow.add_version(definition.clone())?;

    Ok((workflow, definition))
}
