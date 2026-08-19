use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use miranda_control_plane::ControlPlaneError;
use miranda_core::ExecutionError;
use miranda_storage::StorageError;

pub struct HttpError(StatusCode, String);

impl From<ControlPlaneError> for HttpError {
    fn from(err: ControlPlaneError) -> Self {
        let status = match &err {
            ControlPlaneError::Domain(_) => StatusCode::CONFLICT,

            ControlPlaneError::Storage(StorageError::WorkflowNotFound(_))
            | ControlPlaneError::Storage(StorageError::WorkflowVersionNotFound(_))
            | ControlPlaneError::Storage(StorageError::ExecutionNotFound(_))
            | ControlPlaneError::Storage(StorageError::SnapshotNotFound { .. })
            | ControlPlaneError::Storage(StorageError::ArtifactNotFound { .. }) => {
                StatusCode::NOT_FOUND
            }
            ControlPlaneError::Storage(StorageError::OptimisticLockFailed { .. }) => {
                StatusCode::CONFLICT
            }
            ControlPlaneError::Storage(StorageError::ArtifactIsDirectory { .. }) => {
                StatusCode::BAD_REQUEST
            }
            ControlPlaneError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,

            ControlPlaneError::Engine(_) => StatusCode::INTERNAL_SERVER_ERROR,

            ControlPlaneError::WorkerNotFound(_) => StatusCode::NOT_FOUND,
            ControlPlaneError::WorkerUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,

            ControlPlaneError::Queue(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ControlPlaneError::Routing(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ControlPlaneError::Scheduler(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ControlPlaneError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        };

        HttpError(status, err.to_string())
    }
}

impl From<ExecutionError> for HttpError {
    fn from(err: ExecutionError) -> Self {
        HttpError(StatusCode::CONFLICT, err.to_string())
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use miranda_core::id::{ExecutionId, WorkflowTaskId};

    use super::*;

    #[test]
    fn artifact_not_found_maps_to_404() {
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();
        let err = ControlPlaneError::Storage(StorageError::ArtifactNotFound {
            execution_id,
            task_id,
            path: "out.txt".to_string(),
        });

        let http_err: HttpError = err.into();

        assert_eq!(http_err.0, StatusCode::NOT_FOUND);
    }

    #[test]
    fn artifact_is_directory_maps_to_400() {
        let execution_id = ExecutionId::new();
        let task_id = WorkflowTaskId::new();
        let err = ControlPlaneError::Storage(StorageError::ArtifactIsDirectory {
            execution_id,
            task_id,
            path: "dir".to_string(),
        });

        let http_err: HttpError = err.into();

        assert_eq!(http_err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn optimistic_lock_failed_maps_to_409() {
        let err = ControlPlaneError::Storage(StorageError::OptimisticLockFailed {
            id: ExecutionId::new(),
            expected: 1,
            actual: 2,
        });

        let http_err: HttpError = err.into();

        assert_eq!(http_err.0, StatusCode::CONFLICT);
    }

    #[test]
    fn worker_unavailable_maps_to_503() {
        let err = ControlPlaneError::WorkerUnavailable(miranda_core::id::WorkerId::new());

        let http_err: HttpError = err.into();

        assert_eq!(http_err.0, StatusCode::SERVICE_UNAVAILABLE);
    }
}
