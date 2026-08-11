use std::{future::Future, pin::Pin, sync::Arc};

pub trait TaskNotifier: Send + Sync {
    fn notify_ready<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub struct NullTaskNotifier;

impl TaskNotifier for NullTaskNotifier {
    fn notify_ready<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}

impl TaskNotifier for Arc<dyn TaskNotifier + '_> {
    fn notify_ready<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        (**self).notify_ready()
    }
}
