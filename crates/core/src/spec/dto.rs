use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct WorkflowSpec {
    pub name: String,

    #[serde(default)]
    pub timeout: Option<u64>,

    pub tasks: BTreeMap<String, TaskSpec>,
}

#[derive(Debug, Deserialize)]
pub struct TaskSpec {
    #[serde(rename = "type")]
    pub task_type: String,

    #[serde(default)]
    pub timeout: Option<u64>,

    #[serde(default)]
    pub depends_on: Vec<String>,
}
