use std::time::Duration;

pub async fn delay(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn delay_returns_immediately_for_zero_duration() {
        let start = Instant::now();
        delay(Duration::ZERO).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn delay_waits_for_the_given_duration() {
        let start = Instant::now();
        delay(Duration::from_millis(20)).await;
        assert!(start.elapsed() >= Duration::from_millis(20));
    }
}
