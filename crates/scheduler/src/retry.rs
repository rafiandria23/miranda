use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    Fixed(Duration),
    Linear { initial: Duration, factor: Duration },
    Exponential { initial: Duration, factor: u32 },
}

impl Backoff {
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return Duration::ZERO;
        }

        let retry_count = attempt - 1;

        match self {
            Self::Fixed(duration) => *duration,
            Self::Linear { initial, factor } => *initial + (*factor * (retry_count - 1)),
            Self::Exponential { initial, factor } => {
                let multiplier = factor.saturating_pow(retry_count - 1);

                *initial * multiplier
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: Backoff,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, backoff: Backoff) -> Self {
        Self {
            max_attempts,
            backoff,
        }
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }

    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        self.backoff.calculate_delay(attempt)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Backoff::Fixed(Duration::from_millis(120)),
        }
    }
}
