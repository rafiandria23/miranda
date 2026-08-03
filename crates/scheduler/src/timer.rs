use std::time::Duration;
use tokio::time::sleep;

pub async fn delay(duration: Duration) {
    if !duration.is_zero() {
        sleep(duration).await
    }
}
