use std::time::Duration;
use tokio::time::sleep;

pub async fn delay(duration: Duration) {
    if !duration.is_zero() {
        sleep(duration).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn zero_duration_returns_immediately() {
        let before = Instant::now();

        delay(Duration::ZERO).await;

        assert!(
            before.elapsed() < Duration::from_millis(20),
            "zero-duration delay should not sleep"
        );
    }

    #[tokio::test]
    async fn non_zero_duration_sleeps_at_least_that_long() {
        let duration = Duration::from_millis(50);
        let before = Instant::now();

        delay(duration).await;

        assert!(
            before.elapsed() >= duration,
            "delay returned before the requested duration elapsed"
        );
    }
}
