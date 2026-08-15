use tonic::{Status, transport::Channel};

pub mod proto {
    tonic::include_proto!("miranda.control_plane.v1");
}

use proto::{NotifyReadyRequest, control_plane_service_client::ControlPlaneServiceClient};

pub struct PeerClient {
    client: ControlPlaneServiceClient<Channel>,
}

impl PeerClient {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = ControlPlaneServiceClient::connect(addr).await?;

        Ok(Self { client })
    }

    pub async fn notify_ready(&self) -> Result<(), Status> {
        let mut client = self.client.clone();

        client.notify_ready(NotifyReadyRequest {}).await?;

        Ok(())
    }
}
