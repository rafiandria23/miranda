mod executions;
mod workflows;

use axum::Router;
use std::sync::Arc;

use crate::bootstrap::ServerControlPlane;

pub fn router(control_plane: Arc<ServerControlPlane>) -> Router {
    Router::new()
        .merge(workflows::routes())
        .merge(executions::routes())
        .with_state(control_plane)
}
