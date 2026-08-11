use miranda_control_plane::notifier::TaskNotifier;
use miranda_core::id::WorkerId;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::sync::{RwLock, mpsc};

use super::service::proto::TaskNotification;

#[derive(Clone, Default)]
pub struct GrpcTaskNotifier {
    subscribers: Arc<RwLock<HashMap<WorkerId, mpsc::Sender<TaskNotification>>>>,
}

impl GrpcTaskNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn subscribe(&self, worker_id: WorkerId) -> mpsc::Receiver<TaskNotification> {
        let (tx, rx) = mpsc::channel(6);

        self.subscribers.write().await.insert(worker_id, tx);

        rx
    }

    pub async fn unsubscribe(&self, worker_id: WorkerId) {
        self.subscribers.write().await.remove(&worker_id);
    }
}

impl TaskNotifier for GrpcTaskNotifier {
    fn notify_ready<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let subscribers = self.subscribers.read().await;

            let mut stale = Vec::new();

            for (worker_id, tx) in subscribers.iter() {
                if tx.try_send(TaskNotification {}).is_err() {
                    stale.push(*worker_id);
                }
            }

            drop(subscribers);

            if !stale.is_empty() {
                let mut subscribers = self.subscribers.write().await;

                for worker_id in stale {
                    subscribers.remove(&worker_id);
                }
            }
        })
    }
}
