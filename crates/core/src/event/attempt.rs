use crate::id::{AttemptId, TaskId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptCreated {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptStarted {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptSucceeded {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptFailed {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptCancelled {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_attempt_created_event() {
        let task_id = TaskId::new();

        let attempt_id = AttemptId::new();

        let event = AttemptCreated {
            task_id,
            attempt_id,
        };

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.attempt_id, attempt_id);
    }

    #[test]
    fn creates_attempt_started_event() {
        let task_id = TaskId::new();

        let attempt_id = AttemptId::new();

        let event = AttemptStarted {
            task_id,
            attempt_id,
        };

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.attempt_id, attempt_id);
    }

    #[test]
    fn creates_attempt_succeeded_event() {
        let task_id = TaskId::new();

        let attempt_id = AttemptId::new();

        let event = AttemptSucceeded {
            task_id,
            attempt_id,
        };

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.attempt_id, attempt_id);
    }

    #[test]
    fn creates_attempt_failed_event() {
        let task_id = TaskId::new();

        let attempt_id = AttemptId::new();

        let event = AttemptFailed {
            task_id,
            attempt_id,
        };

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.attempt_id, attempt_id);
    }

    #[test]
    fn creates_attempt_cancelled_event() {
        let task_id = TaskId::new();

        let attempt_id = AttemptId::new();

        let event = AttemptCancelled {
            task_id,
            attempt_id,
        };

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.attempt_id, attempt_id);
    }
}
