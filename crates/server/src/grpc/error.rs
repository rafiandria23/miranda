use miranda_control_plane::error::ControlPlaneError;
use miranda_storage::error::StorageError;
use tonic::Status;

pub fn to_status(err: ControlPlaneError) -> Status {
    match err {
        ControlPlaneError::Domain(e) => Status::failed_precondition(e.to_string()),

        ControlPlaneError::Storage(StorageError::WorkflowNotFound(_))
        | ControlPlaneError::Storage(StorageError::WorkflowVersionNotFound(_))
        | ControlPlaneError::Storage(StorageError::ExecutionNotFound(_))
        | ControlPlaneError::Storage(StorageError::SnapshotNotFound { .. }) => {
            Status::not_found(err.to_string())
        }
        ControlPlaneError::Storage(StorageError::OptimisticLockFailed { .. }) => {
            Status::aborted(err.to_string())
        }
        ControlPlaneError::Storage(_) => Status::internal(err.to_string()),

        ControlPlaneError::Engine(_) => Status::internal(err.to_string()),

        ControlPlaneError::WorkerNotFound(_) => Status::not_found(err.to_string()),
        ControlPlaneError::WorkerUnavailable(_) => Status::unavailable(err.to_string()),

        ControlPlaneError::Queue(_) => Status::internal(err.to_string()),
        ControlPlaneError::Routing(_) => Status::internal(err.to_string()),
        ControlPlaneError::Scheduler(_) => Status::internal(err.to_string()),

        ControlPlaneError::InvalidRequest(msg) => Status::invalid_argument(msg),
    }
}
