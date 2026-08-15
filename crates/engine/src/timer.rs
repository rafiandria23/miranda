use std::time::Duration;

pub async fn delay(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await
    }
}
