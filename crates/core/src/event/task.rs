use crate::id::TaskId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCreated {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskStarted {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCompleted {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskFailed {
    pub task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCancelled {
    pub task_id: TaskId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_task_created_event() {
        let task_id = TaskId::new();

        let event = TaskCreated { task_id };

        assert_eq!(event.task_id, task_id);
    }

    #[test]
    fn creates_task_started_event() {
        let task_id = TaskId::new();

        let event = TaskStarted { task_id };

        assert_eq!(event.task_id, task_id);
    }

    #[test]
    fn creates_task_completed_event() {
        let task_id = TaskId::new();

        let event = TaskCompleted { task_id };

        assert_eq!(event.task_id, task_id);
    }

    #[test]
    fn creates_task_failed_event() {
        let task_id = TaskId::new();

        let event = TaskFailed { task_id };

        assert_eq!(event.task_id, task_id);
    }

    #[test]
    fn creates_task_cancelled_event() {
        let task_id = TaskId::new();

        let event = TaskCancelled { task_id };

        assert_eq!(event.task_id, task_id);
    }
}
