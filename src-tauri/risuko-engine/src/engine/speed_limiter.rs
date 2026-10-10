pub use risuko_bt::limiter::RateLimiter as SpeedLimiter;

pub fn parse_speed_limit(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        serde_json::Value::String(s) => parse_speed_limit_str(s),
        _ => 0,
    }
}

fn parse_speed_limit_str(s: &str) -> u64 {
    let s = s.trim();
    let (num_part, multiplier) = match s.as_bytes().last() {
        Some(b'G' | b'g') => (&s[..s.len() - 1], 1024.0 * 1024.0 * 1024.0),
        Some(b'M' | b'm') => (&s[..s.len() - 1], 1024.0 * 1024.0),
        Some(b'K' | b'k') => (&s[..s.len() - 1], 1024.0),
        _ => (s, 1.0),
    };
    let n = num_part.trim();
    if let Ok(int) = n.parse::<u64>() {
        return int.saturating_mul(multiplier as u64);
    }
    match n.parse::<f64>() {
        Ok(f) if f.is_finite() && f > 0.0 => (f * multiplier) as u64,
        _ => {
            tracing::warn!("invalid speed limit {s:?}, treating as unlimited");
            0
        }
    }
}

pub const MIN_EMA_SAMPLE_SECS: f64 = 0.1;

#[derive(Default)]
pub struct SpeedEma {
    ema: f64,
    initialized: bool,
}

impl SpeedEma {
    const ALPHA: f64 = 0.3;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, bytes: u64, elapsed_secs: f64) -> u64 {
        let instant = bytes as f64 / elapsed_secs;
        self.ema = if self.initialized {
            Self::ALPHA * instant + (1.0 - Self::ALPHA) * self.ema
        } else {
            self.initialized = true;
            instant
        };
        self.ema as u64
    }

    pub fn get(&self) -> u64 {
        self.ema as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn speed_ema_seeds_on_first_sample() {
        let mut ema = SpeedEma::new();
        assert_eq!(ema.update(1000, 1.0), 1000);
        let next = ema.update(2000, 1.0);
        assert!((1000..2000).contains(&next), "got {next}");
    }

    #[test]
    fn speed_ema_does_not_reseed_after_decay_below_one() {
        let mut ema = SpeedEma::new();
        assert_eq!(ema.update(1, 10.0), 0);
        assert_eq!(ema.get(), 0);
        let next = ema.update(1000, 1.0);
        assert!(next > 0 && next < 1000, "expected EMA blend, got {next}");
    }

    #[test]
    fn parse_limits() {
        assert_eq!(parse_speed_limit(&json!(0)), 0);
        assert_eq!(parse_speed_limit(&json!("0")), 0);
        assert_eq!(parse_speed_limit(&json!("10K")), 10 * 1024);
        assert_eq!(parse_speed_limit(&json!("10k")), 10 * 1024);
        assert_eq!(parse_speed_limit(&json!("5M")), 5 * 1024 * 1024);
        assert_eq!(parse_speed_limit(&json!("5m")), 5 * 1024 * 1024);
        assert_eq!(parse_speed_limit(&json!("1024")), 1024);
        assert_eq!(parse_speed_limit(&json!("1G")), 1024 * 1024 * 1024);
        assert_eq!(parse_speed_limit(&json!("1.5M")), 1536 * 1024);
        assert_eq!(parse_speed_limit(&json!(" 10 M")), 10 * 1024 * 1024);
        assert_eq!(parse_speed_limit(&json!("abc")), 0);
        assert_eq!(parse_speed_limit(&json!("")), 0);
        assert_eq!(parse_speed_limit(&json!(null)), 0);
    }

    #[tokio::test]
    async fn unlimited_returns_immediately() {
        let lim = SpeedLimiter::new(0);
        let start = std::time::Instant::now();
        lim.acquire(10 * 1024 * 1024).await;
        assert!(start.elapsed() < std::time::Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_acquire_eventually_completes() {
        let lim = SpeedLimiter::new(1024);
        tokio::time::timeout(std::time::Duration::from_secs(60), lim.acquire(8 * 1024))
            .await
            .expect("oversized acquire must not hang");
    }

    #[tokio::test]
    async fn limited_throttles_then_refills() {
        let lim = SpeedLimiter::new(4 * 1024 * 1024);
        lim.acquire(4 * 1024 * 1024).await;
        let start = std::time::Instant::now();
        lim.acquire(1024 * 1024).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(150));
        assert!(elapsed < std::time::Duration::from_millis(800));
    }
}
