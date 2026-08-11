mod durable;
mod memory;

pub use durable::DurableRouter;
pub use memory::InMemoryRouter;

use miranda_core::{id::WorkerId, router::WorkerRegistration};
use std::{future::Future, pin::Pin};
use time::Duration;

use crate::error::ControlPlaneError;

pub const DEFAULT_WORKER_STALENESS_THRESHOLD: Duration = Duration::seconds(120);

pub trait Router: Send + Sync {
    fn register<'a>(
        &'a self,
        registration: WorkerRegistration,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>>;

    fn deregister<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>>;

    fn touch<'a>(
        &'a self,
        worker_id: WorkerId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ControlPlaneError>> + Send + 'a>>;

    fn select_worker<'a>(
        &'a self,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<WorkerId>> + Send + 'a>>;

    fn worker_satisfies<'a>(
        &'a self,
        worker_id: WorkerId,
        capability: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

    fn reap_stale<'a>(
        &'a self,
        threshold: Duration,
    ) -> Pin<Box<dyn Future<Output = Vec<WorkerId>> + Send + 'a>>;
}
