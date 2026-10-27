//! Reconnection delays, like socket.io-client's `Backoff` from `backo2`.

use std::time::Duration;

/// Grows the delay before each reconnection attempt, with random jitter, like
/// `Backoff` from backo2.
///
/// Attempt `n` waits `delay * 2^n`, moved up or down by a random share of up
/// to `randomization_factor` of itself, and capped at `delay_max`.
#[derive(Debug)]
pub struct Backoff {
    /// The first delay, like `Backoff.ms`.
    delay: Duration,
    /// The longest delay, like `Backoff.max`.
    delay_max: Duration,
    /// The share of jitter, like `Backoff.jitter`.
    randomization_factor: f64,
    /// The number of attempts before giving up, or `None` for no limit, like
    /// `Manager._reconnectionAttempts`.
    max_attempts: Option<u32>,
    /// The attempts since the last reset, like `Backoff.attempts`.
    attempts: u32,
}

impl Backoff {
    /// Creates a backoff with no attempts made, like the `Backoff` constructor.
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

    /// Returns the number of attempts since the last reset, like
    /// `Backoff.attempts`.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Counts an attempt and returns its delay, or `None` once the attempts
    /// ran out, like `Backoff.duration` with the attempt check in
    /// `Manager.reconnect`.
    ///
    /// Draws the jitter from `rng`, so a caller that owns a seeded generator
    /// gets reproducible delays where JS calls `Math.random`.
    pub fn next_delay(&mut self, rng: &mut fastrand::Rng) -> Option<Duration> {
        if self.max_attempts.is_some_and(|max| self.attempts >= max) {
            return None;
        }

        // Keep both operands finite: zero delay stays zero, and subtractive
        // jitter never subtracts infinity from infinity. Saturated positive
        // delays still exceed every Duration cap after jitter.
        let exponent = i32::try_from(self.attempts).unwrap_or(i32::MAX);
        let multiplier = 2_f64.powi(exponent).min(f64::MAX);
        let millis = (self.delay.as_secs_f64() * 1000.0 * multiplier).min(f64::MAX);

        self.attempts = self.attempts.saturating_add(1);

        let random = rng.f64();
        let deviation = (random * self.randomization_factor * millis).floor();

        // One random number picks both the deviation and its sign, as in JS.
        let millis = if (random * 10.0).floor() % 2.0 == 0.0 {
            millis - deviation
        } else {
            millis + deviation
        };

        let max_millis = self.delay_max.as_secs_f64() * 1000.0;
        let seconds = millis.clamp(0.0, max_millis).floor() / 1000.0;
        // The configured cap is exact; its floating representation may round
        // beyond Duration::MAX, so keep the original value at the ceiling.
        Some(if seconds >= self.delay_max.as_secs_f64() {
            self.delay_max
        } else {
            Duration::from_secs_f64(seconds)
        })
    }

    /// Starts counting attempts from zero again, like `Backoff.reset`.
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
        let delays: Vec<_> = std::iter::from_fn(|| backoff.next_delay(&mut fastrand::Rng::new()))
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
            let millis = backoff
                .next_delay(&mut fastrand::Rng::new())
                .unwrap()
                .as_millis();
            assert!((500..=1500).contains(&millis), "{millis}");
        }
    }

    #[test]
    fn factor_outside_zero_to_one_means_no_jitter() {
        for factor in [-0.5, 1.5, f64::NAN] {
            let mut backoff = backoff(factor, None);
            assert_eq!(
                backoff.next_delay(&mut fastrand::Rng::new()),
                Some(Duration::from_secs(1))
            );
        }
    }

    #[test]
    fn attempts_run_out_until_reset() {
        let mut backoff = backoff(0.0, Some(2));
        backoff.next_delay(&mut fastrand::Rng::new()).unwrap();
        backoff.next_delay(&mut fastrand::Rng::new()).unwrap();
        assert_eq!(backoff.attempts(), 2);
        assert_eq!(backoff.next_delay(&mut fastrand::Rng::new()), None);

        backoff.reset();
        assert_eq!(
            backoff.next_delay(&mut fastrand::Rng::new()),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn seeded_generators_give_equal_delays() {
        let delays = |seed| {
            let mut rng = fastrand::Rng::with_seed(seed);
            let mut backoff = backoff(0.5, None);
            (0..4)
                .map(|_| backoff.next_delay(&mut rng))
                .collect::<Vec<_>>()
        };
        assert_eq!(delays(7), delays(7));
        assert_ne!(delays(7), delays(8));
    }

    #[test]
    fn huge_attempt_counts_stay_at_the_cap() {
        let mut backoff = backoff(0.5, None);
        backoff.attempts = u32::MAX - 1;
        assert_eq!(
            backoff.next_delay(&mut fastrand::Rng::new()),
            Some(Duration::from_secs(5))
        );
    }

    #[test]
    fn maximum_duration_stays_within_the_configured_cap() {
        let near_max = Duration::MAX.checked_sub(Duration::from_secs(1)).unwrap();
        for (cap, expected) in [
            (Duration::MAX, Duration::MAX),
            (near_max, near_max),
            (Duration::from_micros(2500), Duration::from_millis(2)),
        ] {
            let mut backoff = Backoff::new(Duration::MAX, cap, 0.0, None);
            let mut rng = fastrand::Rng::with_seed(7);
            assert_eq!(backoff.next_delay(&mut rng), Some(expected));
        }
    }

    #[test]
    fn delays_keep_growing_after_sixty_four_attempts() {
        let mut backoff = Backoff::new(Duration::from_nanos(1), Duration::MAX, 0.0, None);
        backoff.attempts = 64;
        let mut rng = fastrand::Rng::with_seed(7);
        let first = backoff.next_delay(&mut rng).unwrap();
        let second = backoff.next_delay(&mut rng).unwrap();
        assert!(second > first);
    }

    #[test]
    fn enormous_attempt_counts_preserve_zero_delays_and_caps() {
        let fractional_cap = Duration::from_micros(2500);
        for (delay, cap, expected) in [
            (Duration::ZERO, fractional_cap, Duration::ZERO),
            (Duration::ZERO, Duration::MAX, Duration::ZERO),
            (
                Duration::from_nanos(1),
                fractional_cap,
                Duration::from_millis(2),
            ),
            (Duration::from_nanos(1), Duration::MAX, Duration::MAX),
            (Duration::MAX, fractional_cap, Duration::from_millis(2)),
            (Duration::MAX, Duration::MAX, Duration::MAX),
        ] {
            for jitter in [0.0, 0.5, 1.0] {
                let mut backoff = Backoff::new(delay, cap, jitter, None);
                backoff.attempts = u32::MAX - 1;
                let mut rng = fastrand::Rng::with_seed(7);
                let delays: [_; 3] = std::array::from_fn(|_| backoff.next_delay(&mut rng));
                assert_eq!(delays, [Some(expected); 3]);
            }
        }
    }
}
