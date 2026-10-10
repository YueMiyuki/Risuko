use std::collections::HashMap;
use std::time::Instant;

use serde_json::{Map, Value};
use url::Url;

const MAX_CONSECUTIVE_FAILS: u32 = 3;
const FAIL_WINDOW_SECS: u64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Inorder,
    Feedback,
    Adaptive,
}

impl Strategy {
    pub fn from_option(v: Option<&Value>) -> Self {
        match v.and_then(Value::as_str) {
            Some("inorder") => Strategy::Inorder,
            Some("adaptive") => Strategy::Adaptive,
            _ => Strategy::Feedback,
        }
    }
}

#[derive(Default)]
pub struct ServerStat {
    pub fail_count: u32,
    pub last_fail: Option<Instant>,
    pub ema_bps: f64,
}

#[derive(Default)]
pub struct ServerStats {
    by_host: HashMap<String, ServerStat>,
}

impl ServerStats {
    pub fn record_failure(&mut self, host: &str) {
        let entry = self.by_host.entry(host.to_string()).or_default();
        let now = Instant::now();
        if entry
            .last_fail
            .is_some_and(|t| now.duration_since(t).as_secs() > FAIL_WINDOW_SECS)
        {
            entry.fail_count = 0;
        }
        entry.fail_count = entry.fail_count.saturating_add(1);
        entry.last_fail = Some(now);
    }

    pub fn record_success(&mut self, host: &str, bytes: u64, secs: f64) {
        let entry = self.by_host.entry(host.to_string()).or_default();
        entry.fail_count = 0;
        entry.last_fail = None;
        if secs > 0.001 {
            let bps = bytes as f64 / secs;
            entry.ema_bps = if entry.ema_bps == 0.0 {
                bps
            } else {
                entry.ema_bps * 0.7 + bps * 0.3
            };
        }
    }

    pub fn is_blacklisted(&self, host: &str) -> bool {
        self.by_host.get(host).is_some_and(|s| {
            if s.fail_count < MAX_CONSECUTIVE_FAILS {
                return false;
            }
            match s.last_fail {
                Some(t) => Instant::now().duration_since(t).as_secs() <= FAIL_WINDOW_SECS,
                None => false,
            }
        })
    }

    pub fn get(&self, host: &str) -> Option<&ServerStat> {
        self.by_host.get(host)
    }
}

pub fn pick(
    strategy: Strategy,
    uris: &[String],
    stats: &ServerStats,
    failed_in_this_attempt: &[usize],
) -> Option<usize> {
    if uris.is_empty() {
        return None;
    }
    let host_of = |uri: &str| {
        Url::parse(uri)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
    };

    let candidates: Vec<usize> = (0..uris.len())
        .filter(|i| !failed_in_this_attempt.contains(i))
        .filter(|i| {
            if strategy == Strategy::Inorder {
                return true;
            }
            match host_of(&uris[*i]) {
                Some(h) => !stats.is_blacklisted(&h),
                None => true,
            }
        })
        .collect();

    if candidates.is_empty() {
        return None;
    }

    match strategy {
        Strategy::Inorder | Strategy::Feedback => Some(candidates[0]),
        Strategy::Adaptive => {
            let known: Vec<f64> = candidates
                .iter()
                .filter_map(|&i| host_of(&uris[i]).and_then(|h| stats.get(&h).map(|s| s.ema_bps)))
                .collect();
            let baseline = if known.is_empty() {
                f64::EPSILON
            } else {
                let min_known = known
                    .iter()
                    .copied()
                    .fold(f64::INFINITY, f64::min)
                    .max(f64::EPSILON);
                min_known * 0.5
            };
            let weights: Vec<f64> = candidates
                .iter()
                .map(|&i| {
                    host_of(&uris[i])
                        .and_then(|h| stats.get(&h).map(|s| s.ema_bps))
                        .unwrap_or(baseline)
                })
                .collect();
            let total: f64 = weights.iter().sum();
            if total <= 0.0 {
                return Some(candidates[0]);
            }
            let (best_pos, _) = weights
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap();
            Some(candidates[best_pos])
        }
    }
}

pub fn strategy_from_options(options: &Map<String, Value>) -> Strategy {
    Strategy::from_option(options.get("uri-selector"))
}

pub fn host_of(uri: &str) -> String {
    Url::parse(uri)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_ascii_lowercase()))
        .unwrap_or_else(|| uri.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_default_is_feedback() {
        assert_eq!(Strategy::from_option(None), Strategy::Feedback);
        assert_eq!(
            Strategy::from_option(Some(&Value::String("garbage".into()))),
            Strategy::Feedback
        );
        assert_eq!(
            Strategy::from_option(Some(&Value::String("inorder".into()))),
            Strategy::Inorder
        );
    }

    #[test]
    fn feedback_skips_blacklisted_host() {
        let uris = vec!["http://a.test/x".into(), "http://b.test/x".into()];
        let mut stats = ServerStats::default();
        for _ in 0..MAX_CONSECUTIVE_FAILS {
            stats.record_failure("a.test");
        }
        let pick = pick(Strategy::Feedback, &uris, &stats, &[]);
        assert_eq!(pick, Some(1));
    }

    #[test]
    fn inorder_ignores_blacklist_but_honors_failed_in_attempt() {
        let uris = vec!["http://a.test/x".into(), "http://b.test/x".into()];
        let mut stats = ServerStats::default();
        for _ in 0..MAX_CONSECUTIVE_FAILS {
            stats.record_failure("a.test");
        }
        assert_eq!(pick(Strategy::Feedback, &uris, &stats, &[]), Some(1));
        assert_eq!(pick(Strategy::Inorder, &uris, &stats, &[]), Some(0));
        assert_eq!(pick(Strategy::Inorder, &uris, &stats, &[0]), Some(1));
    }

    #[test]
    fn pick_returns_none_when_all_blacklisted() {
        let uris = vec!["http://a.test/x".into()];
        let mut stats = ServerStats::default();
        for _ in 0..MAX_CONSECUTIVE_FAILS {
            stats.record_failure("a.test");
        }
        assert_eq!(pick(Strategy::Feedback, &uris, &stats, &[]), None);
    }

    #[test]
    fn skip_already_failed_in_attempt() {
        let uris = vec!["http://a.test/x".into(), "http://b.test/x".into()];
        let stats = ServerStats::default();
        assert_eq!(pick(Strategy::Inorder, &uris, &stats, &[0]), Some(1));
        assert_eq!(pick(Strategy::Inorder, &uris, &stats, &[0, 1]), None);
    }

    #[test]
    fn success_records_ema() {
        let mut s = ServerStats::default();
        s.record_success("a.test", 1024 * 1024, 1.0);
        let stat = s.get("a.test").unwrap();
        assert!(stat.ema_bps > 0.0);
        assert_eq!(stat.fail_count, 0);
    }
}
