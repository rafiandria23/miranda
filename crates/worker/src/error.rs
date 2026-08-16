use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerError {
    #[error("task execution failed: {message}")]
    ExecutionFailed { message: String },

    #[error("task type not supperted: {task_type}")]
    UnsupportedTaskType { task_type: String },

    #[error("task timeout after {duration}ms")]
    Timeout { duration: u64 },
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_message_for_execution_failed() {
        let err = WorkerError::ExecutionFailed {
            message: "boom".to_string(),
        };
        assert_eq!(err.to_string(), "task execution failed: boom");
    }

    #[test]
    fn display_message_for_unsupported_task_type() {
        let err = WorkerError::UnsupportedTaskType {
            task_type: "unknown".to_string(),
        };
        assert_eq!(err.to_string(), "task type not supperted: unknown");
    }

    #[test]
    fn display_message_for_timeout() {
        let err = WorkerError::Timeout { duration: 5000 };
        assert_eq!(err.to_string(), "task timeout after 5000ms");
    }

    #[test]
    fn errors_support_equality_and_clone() {
        let err = WorkerError::Timeout { duration: 1 };
        let cloned = err.clone();
        assert_eq!(err, cloned);
        assert_ne!(err, WorkerError::Timeout { duration: 2 });
    }
}
