//! Reconnection delays, like socket.io-client's `Backoff` from `backo2`.

use std::time::Duration;

/// Grows the delay before each reconnection attempt, with random jitter.
///
/// Attempt `n` waits `delay * 2^n`, moved up or down by a random share of up
/// to `randomization_factor` of itself, and capped at `delay_max`.
#[derive(Debug)]
pub struct Backoff {
    delay: Duration,
    delay_max: Duration,
    randomization_factor: f64,
    /// The number of attempts before giving up, or `None` for no limit.
    max_attempts: Option<u32>,
    attempts: u32,
}

impl Backoff {
    /// Creates a backoff with no attempts made.
    ///
    /// A `randomization_factor` outside 0 to 1 means no jitter, as in JS.
    #[must_use]
    pub fn new(
        delay: Duration,
        delay_max: Duration,
        randomization_factor: f64,
        max_attempts: Option<u32>,
    ) -> Self {
        let randomization_factor = if randomization_factor > 0.0 && randomization_factor <= 1.0 {
            randomization_factor
        } else {
            0.0
        };

        Self {
            delay,
            delay_max,
            randomization_factor,
            max_attempts,
            attempts: 0,
        }
    }

    /// Returns the number of attempts since the last reset.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Counts an attempt and returns its delay, or `None` once the attempts
    /// ran out.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.max_attempts.is_some_and(|max| self.attempts >= max) {
            return None;
        }

        // The cap is reached long before the exponent overflows `f64`.
        let exponent = i32::try_from(self.attempts).unwrap_or(i32::MAX).min(64);
        let millis = self.delay.as_secs_f64() * 1000.0 * 2_f64.powi(exponent);

        self.attempts = self.attempts.saturating_add(1);

        let random = fastrand::f64();
        let deviation = (random * self.randomization_factor * millis).floor();

        // One random number picks both the deviation and its sign, as in JS.
        let millis = if (random * 10.0).floor() % 2.0 == 0.0 {
            millis - deviation
        } else {
            millis + deviation
        };

        let max_millis = self.delay_max.as_secs_f64() * 1000.0;

        Some(Duration::from_secs_f64(
            millis.clamp(0.0, max_millis).floor() / 1000.0,
        ))
    }

    /// Starts counting attempts from zero again.
    pub fn reset(&mut self) {
        self.attempts = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff(randomization_factor: f64, max_attempts: Option<u32>) -> Backoff {
        Backoff::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            randomization_factor,
            max_attempts,
        )
    }

    #[test]
    fn delay_doubles_up_to_the_cap() {
        let mut backoff = backoff(0.0, None);
        let delays: Vec<_> = std::iter::from_fn(|| backoff.next_delay())
            .take(5)
            .map(|delay| delay.as_millis())
            .collect();
        assert_eq!(delays, [1000, 2000, 4000, 5000, 5000]);
    }

    #[test]
    fn jitter_stays_within_the_factor() {
        let mut backoff = backoff(0.5, None);
        for _ in 0..100 {
            backoff.reset();
            let millis = backoff.next_delay().unwrap().as_millis();
            assert!((500..=1500).contains(&millis), "{millis}");
        }
    }

    #[test]
    fn factor_outside_zero_to_one_means_no_jitter() {
        for factor in [-0.5, 1.5, f64::NAN] {
            let mut backoff = backoff(factor, None);
            assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        }
    }

    #[test]
    fn attempts_run_out_until_reset() {
        let mut backoff = backoff(0.0, Some(2));
        backoff.next_delay().unwrap();
        backoff.next_delay().unwrap();
        assert_eq!(backoff.attempts(), 2);
        assert_eq!(backoff.next_delay(), None);

        backoff.reset();
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn huge_attempt_counts_stay_at_the_cap() {
        let mut backoff = backoff(0.5, None);
        backoff.attempts = u32::MAX - 1;
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(5)));
    }
}
