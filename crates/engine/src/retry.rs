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
            max_attempts: 6,
            backoff: Backoff::Fixed(Duration::from_millis(120)),
        }
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_backoff_returns_zero_for_first_attempt() {
        let backoff = Backoff::Fixed(Duration::from_millis(100));

        assert_eq!(backoff.calculate_delay(0), Duration::ZERO);
        assert_eq!(backoff.calculate_delay(1), Duration::ZERO);
    }

    #[test]
    fn fixed_backoff_returns_same_duration_for_every_retry() {
        let backoff = Backoff::Fixed(Duration::from_millis(100));

        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(100));
        assert_eq!(backoff.calculate_delay(3), Duration::from_millis(100));
        assert_eq!(backoff.calculate_delay(10), Duration::from_millis(100));
    }

    #[test]
    fn linear_backoff_returns_zero_for_first_attempt() {
        let backoff = Backoff::Linear {
            initial: Duration::from_millis(50),
            factor: Duration::from_millis(25),
        };

        assert_eq!(backoff.calculate_delay(0), Duration::ZERO);
        assert_eq!(backoff.calculate_delay(1), Duration::ZERO);
    }

    #[test]
    fn linear_backoff_increases_by_factor_per_retry() {
        let backoff = Backoff::Linear {
            initial: Duration::from_millis(50),
            factor: Duration::from_millis(25),
        };

        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(50));
        assert_eq!(backoff.calculate_delay(3), Duration::from_millis(75));
        assert_eq!(backoff.calculate_delay(4), Duration::from_millis(100));
    }

    #[test]
    fn exponential_backoff_returns_zero_for_first_attempt() {
        let backoff = Backoff::Exponential {
            initial: Duration::from_millis(10),
            factor: 2,
        };

        assert_eq!(backoff.calculate_delay(0), Duration::ZERO);
        assert_eq!(backoff.calculate_delay(1), Duration::ZERO);
    }

    #[test]
    fn exponential_backoff_multiplies_by_factor_per_retry() {
        let backoff = Backoff::Exponential {
            initial: Duration::from_millis(10),
            factor: 2,
        };

        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(10));
        assert_eq!(backoff.calculate_delay(3), Duration::from_millis(20));
        assert_eq!(backoff.calculate_delay(4), Duration::from_millis(40));
        assert_eq!(backoff.calculate_delay(5), Duration::from_millis(80));
    }

    #[test]
    fn exponential_backoff_saturates_instead_of_overflowing() {
        let backoff = Backoff::Exponential {
            initial: Duration::from_secs(1),
            factor: u32::MAX,
        };

        // Should not panic despite the enormous factor.
        let delay = backoff.calculate_delay(5);

        assert!(delay >= Duration::from_secs(1));
    }

    #[test]
    fn should_retry_allows_attempts_below_max() {
        let policy = RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO));

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
    }

    #[test]
    fn should_retry_disallows_attempts_at_or_above_max() {
        let policy = RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO));

        assert!(!policy.should_retry(3));
        assert!(!policy.should_retry(4));
    }

    #[test]
    fn should_retry_never_retries_when_max_attempts_is_zero() {
        let policy = RetryPolicy::new(0, Backoff::Fixed(Duration::ZERO));

        assert!(!policy.should_retry(0));
    }

    #[test]
    fn delay_for_attempt_delegates_to_backoff() {
        let policy = RetryPolicy::new(5, Backoff::Fixed(Duration::from_millis(200)));

        assert_eq!(policy.delay_for_attempt(1), Duration::ZERO);
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));
    }

    #[test]
    fn default_policy_uses_three_attempts_and_fixed_backoff() {
        let policy = RetryPolicy::default();

        assert_eq!(policy.max_attempts, 6);
        assert_eq!(policy.backoff, Backoff::Fixed(Duration::from_millis(120)));
    }
}
