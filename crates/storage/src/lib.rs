pub mod error;
pub mod traits;

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "postgres")]
pub mod postgres;

// Public re-exports for clean ergonomics
pub use error::StorageError;
pub use traits::WorkflowStore;

#[cfg(feature = "memory")]
pub use memory::MemoryStore;

#[cfg(feature = "postgres")]
pub use postgres::{PostgresConfig, PostgresStore};
