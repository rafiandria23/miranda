use std::{future::Future, pin::Pin};
use time::Duration;

use crate::error::StorageError;

pub trait LeadershipStore: Send + Sync {
    fn try_acquire<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>>;

    fn renew<'a>(
        &'a self,
        holder_id: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StorageError>> + Send + 'a>>;

    fn release<'a>(
        &'a self,
        holder_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>;

    fn current_holder<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, StorageError>> + Send + 'a>>;
}
