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
            | ControlPlaneError::Storage(StorageError::SnapshotNotFound { .. }) => {
                StatusCode::NOT_FOUND
            }
            ControlPlaneError::Storage(StorageError::OptimisticLockFailed { .. }) => {
                StatusCode::CONFLICT
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
