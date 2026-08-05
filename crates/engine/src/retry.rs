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

#[cfg(test)]
mod tests {
    use super::*;

    mod backoff {
        use super::*;

        #[test]
        fn fixed_returns_zero_for_first_attempt() {
            let backoff = Backoff::Fixed(Duration::from_secs(5));

            assert_eq!(backoff.calculate_delay(0), Duration::ZERO);
            assert_eq!(backoff.calculate_delay(1), Duration::ZERO);
        }

        #[test]
        fn fixed_returns_same_duration_for_every_retry() {
            let backoff = Backoff::Fixed(Duration::from_secs(5));

            assert_eq!(backoff.calculate_delay(2), Duration::from_secs(5));
            assert_eq!(backoff.calculate_delay(3), Duration::from_secs(5));
            assert_eq!(backoff.calculate_delay(10), Duration::from_secs(5));
        }

        #[test]
        fn linear_returns_zero_for_first_attempt() {
            let backoff = Backoff::Linear {
                initial: Duration::from_secs(1),
                factor: Duration::from_secs(2),
            };

            assert_eq!(backoff.calculate_delay(0), Duration::ZERO);
            assert_eq!(backoff.calculate_delay(1), Duration::ZERO);
        }

        #[test]
        fn linear_grows_by_factor_per_retry() {
            let backoff = Backoff::Linear {
                initial: Duration::from_secs(1),
                factor: Duration::from_secs(2),
            };

            assert_eq!(backoff.calculate_delay(2), Duration::from_secs(1));
            assert_eq!(backoff.calculate_delay(3), Duration::from_secs(3));
            assert_eq!(backoff.calculate_delay(4), Duration::from_secs(5));
            assert_eq!(backoff.calculate_delay(5), Duration::from_secs(7));
        }

        #[test]
        fn linear_with_zero_factor_stays_at_initial() {
            let backoff = Backoff::Linear {
                initial: Duration::from_secs(1),
                factor: Duration::ZERO,
            };

            assert_eq!(backoff.calculate_delay(2), Duration::from_secs(1));
            assert_eq!(backoff.calculate_delay(5), Duration::from_secs(1));
        }

        #[test]
        fn exponential_returns_zero_for_first_attempt() {
            let backoff = Backoff::Exponential {
                initial: Duration::from_secs(1),
                factor: 2,
            };

            assert_eq!(backoff.calculate_delay(0), Duration::ZERO);
            assert_eq!(backoff.calculate_delay(1), Duration::ZERO);
        }

        #[test]
        fn exponential_grows_by_factor_power_per_retry() {
            let backoff = Backoff::Exponential {
                initial: Duration::from_secs(1),
                factor: 2,
            };

            assert_eq!(backoff.calculate_delay(2), Duration::from_secs(1));
            assert_eq!(backoff.calculate_delay(3), Duration::from_secs(2));
            assert_eq!(backoff.calculate_delay(4), Duration::from_secs(4));
            assert_eq!(backoff.calculate_delay(5), Duration::from_secs(8));
        }

        #[test]
        fn exponential_with_factor_zero_collapses_after_first_retry() {
            let backoff = Backoff::Exponential {
                initial: Duration::from_secs(1),
                factor: 0,
            };

            assert_eq!(backoff.calculate_delay(2), Duration::from_secs(1));
            assert_eq!(backoff.calculate_delay(3), Duration::ZERO);
            assert_eq!(backoff.calculate_delay(4), Duration::ZERO);
        }

        #[test]
        fn exponential_saturates_instead_of_overflowing_the_multiplier() {
            let backoff = Backoff::Exponential {
                initial: Duration::from_nanos(1),
                factor: u32::MAX,
            };

            // retry_count - 1 = 30, which would overflow a plain pow but
            // must saturate instead of panicking.
            assert_eq!(
                backoff.calculate_delay(32),
                Duration::from_nanos(u32::MAX as u64)
            );
        }
    }

    mod retry_policy {
        use super::*;

        #[test]
        fn should_retry_while_below_max_attempts() {
            let policy = RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO));

            assert!(policy.should_retry(1));
            assert!(policy.should_retry(2));
        }

        #[test]
        fn should_not_retry_at_or_above_max_attempts() {
            let policy = RetryPolicy::new(3, Backoff::Fixed(Duration::ZERO));

            assert!(!policy.should_retry(3));
            assert!(!policy.should_retry(4));
        }

        #[test]
        fn should_not_retry_when_max_attempts_is_zero() {
            let policy = RetryPolicy::new(0, Backoff::Fixed(Duration::ZERO));

            assert!(!policy.should_retry(0));
        }

        #[test]
        fn delay_for_attempt_delegates_to_backoff() {
            let policy = RetryPolicy::new(5, Backoff::Fixed(Duration::from_millis(50)));

            assert_eq!(policy.delay_for_attempt(1), Duration::ZERO);
            assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(50));
        }

        #[test]
        fn default_policy_uses_three_attempts_and_fixed_backoff() {
            let policy = RetryPolicy::default();

            assert_eq!(policy.max_attempts, 3);
            assert_eq!(policy.backoff, Backoff::Fixed(Duration::from_millis(120)));
        }
    }
}
