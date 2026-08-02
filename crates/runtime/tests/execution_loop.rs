use miranda_core::{
    Execution, ExecutionStatus, TaskStatus, WorkflowDefinition, WorkflowTask,
    id::{WorkflowTaskId, WorkflowVersionId},
};
use miranda_runtime::{ExecutorError, NoopExecutor, Orchestrator, TaskExecutor, TaskResult};

fn definition(tasks: Vec<WorkflowTask>) -> WorkflowDefinition {
    WorkflowDefinition::new(tasks).unwrap()
}

fn execution(definition: &WorkflowDefinition) -> Execution {
    Execution::from_definition(WorkflowVersionId::new(), definition).unwrap()
}

// -- 1. linear chain --------------------------------------------------

#[tokio::test]
async fn linear_chain_a_b_c_completes() {
    let a_id = WorkflowTaskId::new();
    let b_id = WorkflowTaskId::new();
    let c_id = WorkflowTaskId::new();

    let def = definition(vec![
        WorkflowTask::new(a_id, "a".into(), vec![]).unwrap(),
        WorkflowTask::new(b_id, "b".into(), vec![a_id]).unwrap(),
        WorkflowTask::new(c_id, "c".into(), vec![b_id]).unwrap(),
    ]);
    let exec = execution(&def);

    let result = Orchestrator::new(NoopExecutor)
        .run(exec, &def)
        .await
        .unwrap();

    assert_eq!(result.status(), ExecutionStatus::Completed);
    assert!(
        result
            .tasks()
            .iter()
            .all(|t| t.status() == TaskStatus::Completed)
    );
}

// -- 2. independent tasks ----------------------------------------------

#[tokio::test]
async fn independent_tasks_both_complete() {
    let a_id = WorkflowTaskId::new();
    let b_id = WorkflowTaskId::new();
    let c_id = WorkflowTaskId::new();

    // A -> B, A -> C (B and C independent of each other)
    let def = definition(vec![
        WorkflowTask::new(a_id, "a".into(), vec![]).unwrap(),
        WorkflowTask::new(b_id, "b".into(), vec![a_id]).unwrap(),
        WorkflowTask::new(c_id, "c".into(), vec![a_id]).unwrap(),
    ]);
    let exec = execution(&def);

    let result = Orchestrator::new(NoopExecutor)
        .run(exec, &def)
        .await
        .unwrap();

    assert_eq!(result.status(), ExecutionStatus::Completed);
    assert!(
        result
            .tasks()
            .iter()
            .all(|t| t.status() == TaskStatus::Completed)
    );
}

// -- 3. multiple dependencies -------------------------------------------

#[tokio::test]
async fn task_with_multiple_dependencies_waits_for_all() {
    let a_id = WorkflowTaskId::new();
    let b_id = WorkflowTaskId::new();
    let c_id = WorkflowTaskId::new();

    let def = definition(vec![
        WorkflowTask::new(a_id, "a".into(), vec![]).unwrap(),
        WorkflowTask::new(b_id, "b".into(), vec![]).unwrap(),
        WorkflowTask::new(c_id, "c".into(), vec![a_id, b_id]).unwrap(),
    ]);
    let exec = execution(&def);

    let result = Orchestrator::new(NoopExecutor)
        .run(exec, &def)
        .await
        .unwrap();

    assert_eq!(result.status(), ExecutionStatus::Completed);
    assert!(
        result
            .tasks()
            .iter()
            .all(|t| t.status() == TaskStatus::Completed)
    );
}

// -- 4. task failure ------------------------------------------------------

struct FailingExecutor {
    fails: Vec<String>,
}

impl TaskExecutor for FailingExecutor {
    async fn execute(&self, task: &miranda_core::WorkflowTask) -> TaskResult {
        if self.fails.contains(&task.task_type().to_string()) {
            TaskResult::Failure(ExecutorError::Failed("boom".into()))
        } else {
            TaskResult::Success
        }
    }
}

#[tokio::test]
async fn task_failure_blocks_downstream_and_fails_execution() {
    let a_id = WorkflowTaskId::new();
    let b_id = WorkflowTaskId::new();

    let def = definition(vec![
        WorkflowTask::new(a_id, "a".into(), vec![]).unwrap(),
        WorkflowTask::new(b_id, "b".into(), vec![a_id]).unwrap(),
    ]);
    let exec = execution(&def);

    let executor = FailingExecutor {
        fails: vec!["a".into()],
    };
    let result = Orchestrator::new(executor).run(exec, &def).await.unwrap();

    assert_eq!(result.status(), ExecutionStatus::Failed);

    let a_task = result
        .tasks()
        .iter()
        .find(|t| t.workflow_task_id() == a_id)
        .unwrap();
    let b_task = result
        .tasks()
        .iter()
        .find(|t| t.workflow_task_id() == b_id)
        .unwrap();

    assert_eq!(a_task.status(), TaskStatus::Failed);
    assert_eq!(b_task.status(), TaskStatus::Pending); // never became ready
}

// -- 5. empty workflow ----------------------------------------------------

#[tokio::test]
async fn empty_workflow_completes_immediately() {
    let def = definition(vec![]);
    let exec = execution(&def);

    let result = Orchestrator::new(NoopExecutor)
        .run(exec, &def)
        .await
        .unwrap();

    assert_eq!(result.status(), ExecutionStatus::Completed);
    assert!(result.tasks().is_empty());
}
