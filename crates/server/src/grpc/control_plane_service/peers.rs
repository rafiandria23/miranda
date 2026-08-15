use miranda_storage::peer_store::PeerStore;
use std::{collections::HashMap, sync::Arc};
use time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::client::PeerClient;

pub const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::seconds(12);
pub const DEFAULT_STALENESS_THRESHOLD: Duration = Duration::seconds(60);

pub struct PeerManager<S> {
    id: String,
    grpc_address: String,
    store: S,
    peers: Arc<RwLock<HashMap<String, PeerClient>>>,
}

impl<S: PeerStore> PeerManager<S> {
    pub fn new(grpc_address: String, store: S) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            grpc_address,
            store,
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) {
        if let Err(e) = self.store.register(&self.id, &self.grpc_address).await {
            tracing::warn!(id = %self.id, error = %e, "failed to register as peer");
        }

        let std_discovery_interval: std::time::Duration = DEFAULT_DISCOVERY_INTERVAL
            .try_into()
            .expect("peer discovery interval must be non-negative");

        let mut ticker = tokio::time::interval(std_discovery_interval);

        loop {
            ticker.tick().await;

            if let Err(e) = self.store.touch(&self.id).await {
                tracing::warn!(id = %self.id, error = %e, "failed to send peer heartbeat");
            }

            let active = match self.store.list_active(DEFAULT_STALENESS_THRESHOLD).await {
                Ok(peers) => peers,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to list active peers");
                    continue;
                }
            };

            let mut peers = self.peers.write().await;
            let mut updated = HashMap::new();

            for peer in active {
                if peer.id == self.id {
                    continue;
                }

                if let Some(existing) = peers.remove(&peer.id) {
                    updated.insert(peer.id, existing);
                    continue;
                }

                match PeerClient::connect(peer.grpc_address.clone()).await {
                    Ok(client) => {
                        tracing::info!(peer_id = %peer.id, "discovered new peer");
                        updated.insert(peer.id, client);
                    }
                    Err(e) => {
                        tracing::warn!(peer_id = %peer.id, error = %e, "failed to connect to peer");
                    }
                }
            }

            *peers = updated;
        }
    }

    pub async fn broadcast_notify(&self) {
        let peers = self.peers.read().await;

        for (peer_id, client) in peers.iter() {
            if let Err(e) = client.notify_ready().await {
                tracing::warn!(peer_id = %peer_id, error = %e, "failed to notify peer");
            }
        }
    }
}
