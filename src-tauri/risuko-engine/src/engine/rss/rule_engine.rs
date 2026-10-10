use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use globset::{GlobBuilder, GlobMatcher};
use regex::Regex;

use super::parser::normalize_series;
use super::types::{
    EpisodeKey, EpisodeRecord, EpisodeSelector, ParsedMeta, Pattern, PatternKind, RssItem, RssRule,
    RuleEvaluation, Schedule, Weekday,
};

const QUALITY_BASE_SCORE: i32 = 1000;
const QUALITY_STEP: i32 = 100;

pub fn evaluate_rule(rule: &RssRule, item: &RssItem, parsed: &ParsedMeta) -> RuleEvaluation {
    if !rule.is_active {
        return reject("rule disabled");
    }
    if !rule.feed_ids.is_empty() && !rule.feed_ids.iter().any(|f| f == &item.feed_id) {
        return reject("feed not in scope");
    }

    for p in &rule.title_must {
        if !pattern_matches(p, &item.title) {
            return reject(format!("missing required pattern '{}'", p.value));
        }
    }
    for p in &rule.title_must_not {
        if pattern_matches(p, &item.title) {
            return reject(format!("excluded by pattern '{}'", p.value));
        }
    }

    if let Some(len) = item.enclosure_length {
        if let Some(min) = rule.min_size_bytes {
            if len < min {
                return reject("below min size");
            }
        }
        if let Some(max) = rule.max_size_bytes {
            if len > max {
                return reject("above max size");
            }
        }
    }

    if let Some(min) = rule.min_seeders {
        if let Some(s) = parsed.seeders {
            if s < min {
                return reject("below min seeders");
            }
        }
    }

    if let Some(ref want) = rule.series_filter {
        let want_norm = normalize_series(want);
        if !want_norm.is_empty() {
            let have_norm = parsed.series.as_deref().map(normalize_series);
            match have_norm {
                Some(have) if have.contains(&want_norm) => {}
                _ => return reject("series filter mismatch"),
            }
        }
    }

    if let Some(ref allowed) = rule.seasons {
        if let Some(s) = parsed.season {
            if !allowed.contains(&s) {
                return reject("season not allowed");
            }
        } else if !allowed.is_empty() {
            return reject("no season detected");
        }
    }

    if let Some(ref sel) = rule.episodes {
        let ep = parsed.episode.or(parsed.absolute_episode);
        if !episode_in_selector(sel, ep) {
            return reject("episode not in selector");
        }
    }

    let tags_lower: Vec<String> = parsed
        .quality_tags
        .iter()
        .map(|t| t.to_lowercase())
        .collect();
    if !rule.required_qualities.is_empty() {
        let any = rule
            .required_qualities
            .iter()
            .any(|q| tags_lower.iter().any(|t| t == &q.to_lowercase()));
        if !any {
            return reject("missing required quality");
        }
    }
    for forbidden in &rule.forbidden_qualities {
        if tags_lower.iter().any(|t| t == &forbidden.to_lowercase()) {
            return reject(format!("forbidden quality '{}'", forbidden));
        }
    }

    let score = quality_score(&rule.quality_preferences, &parsed.quality_tags);

    RuleEvaluation {
        matched: true,
        score,
        reason: "matched".into(),
    }
}

pub fn quality_score(preferences: &[String], tags: &[String]) -> i32 {
    if preferences.is_empty() {
        return QUALITY_BASE_SCORE;
    }
    let tags_lower: Vec<String> = tags.iter().map(|t| t.to_lowercase()).collect();
    preferences
        .iter()
        .position(|p| tags_lower.contains(&p.to_lowercase()))
        .map(|idx| QUALITY_BASE_SCORE - (idx as i32) * QUALITY_STEP)
        .unwrap_or(0)
}

fn pattern_matches(p: &Pattern, text: &str) -> bool {
    match p.kind {
        PatternKind::Contains => {
            fn normalize(s: &str, case_sensitive: bool) -> String {
                let mut out = String::with_capacity(s.len());
                let mut prev_space = false;
                for ch in s.chars() {
                    let mapped = if matches!(ch, '.' | '_') { ' ' } else { ch };
                    if mapped == ' ' {
                        if prev_space {
                            continue;
                        }
                        prev_space = true;
                    } else {
                        prev_space = false;
                    }
                    out.push(mapped);
                }
                if case_sensitive {
                    out
                } else {
                    out.to_lowercase()
                }
            }
            normalize(text, p.case_sensitive).contains(&normalize(&p.value, p.case_sensitive))
        }
        PatternKind::Glob => match cached_glob(&p.value, p.case_sensitive) {
            Some(matcher) => matcher.is_match(text),
            None => false,
        },
        PatternKind::Regex => {
            let pattern_src = if p.case_sensitive {
                p.value.clone()
            } else {
                format!("(?i){}", p.value)
            };
            match cached_regex(&pattern_src) {
                Some(re) => re.is_match(text),
                None => false,
            }
        }
    }
}

const PATTERN_CACHE_CAP: usize = 1024;

fn cached_regex(pattern_src: &str) -> Option<Regex> {
    static CACHE: OnceLock<Mutex<HashMap<String, Option<Regex>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = cache.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(cached) = map.get(pattern_src) {
        return cached.clone();
    }
    let compiled = Regex::new(pattern_src).ok();
    if map.len() >= PATTERN_CACHE_CAP {
        map.clear();
    }
    map.insert(pattern_src.to_string(), compiled.clone());
    compiled
}

fn cached_glob(value: &str, case_sensitive: bool) -> Option<GlobMatcher> {
    static CACHE: OnceLock<Mutex<HashMap<(String, bool), Option<GlobMatcher>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (value.to_string(), case_sensitive);
    let mut map = cache.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(cached) = map.get(&key) {
        return cached.clone();
    }
    let compiled = GlobBuilder::new(value)
        .case_insensitive(!case_sensitive)
        .build()
        .ok()
        .map(|g| g.compile_matcher());
    if map.len() >= PATTERN_CACHE_CAP {
        map.clear();
    }
    map.insert(key, compiled.clone());
    compiled
}

fn episode_in_selector(sel: &EpisodeSelector, ep: Option<u32>) -> bool {
    match sel {
        EpisodeSelector::All => true,
        EpisodeSelector::From { start } => ep.map(|e| e >= *start).unwrap_or(false),
        EpisodeSelector::Range { start, end } => {
            ep.map(|e| e >= *start && e <= *end).unwrap_or(false)
        }
        EpisodeSelector::Specific { values } => ep.map(|e| values.contains(&e)).unwrap_or(false),
    }
}

fn reject(reason: impl Into<String>) -> RuleEvaluation {
    RuleEvaluation {
        matched: false,
        score: 0,
        reason: reason.into(),
    }
}

pub fn schedule_allows(sched: &Schedule, ts_secs: u64) -> bool {
    let local = ts_secs as i64 + sched.tz_offset_min as i64 * 60;
    let day_secs = local.rem_euclid(86_400);
    let hour = (day_secs / 3600) as u8;
    let mut day_index = local.div_euclid(86_400);
    if sched.start_hour > sched.end_hour && hour < sched.end_hour {
        day_index -= 1;
    }
    let weekday = unix_weekday(day_index);

    if !sched.days.is_empty() && !sched.days.contains(&weekday) {
        return false;
    }
    if sched.start_hour == sched.end_hour {
        return true;
    }
    if sched.start_hour < sched.end_hour {
        hour >= sched.start_hour && hour < sched.end_hour
    } else {
        hour >= sched.start_hour || hour < sched.end_hour
    }
}

fn unix_weekday(days_since_epoch: i64) -> Weekday {
    let idx = (days_since_epoch.rem_euclid(7) + 3).rem_euclid(7);
    match idx {
        0 => Weekday::Mon,
        1 => Weekday::Tue,
        2 => Weekday::Wed,
        3 => Weekday::Thu,
        4 => Weekday::Fri,
        5 => Weekday::Sat,
        _ => Weekday::Sun,
    }
}

pub fn episode_key_for(parsed: &ParsedMeta) -> Option<EpisodeKey> {
    let series = parsed.series.as_ref().map(|s| normalize_series(s))?;
    if series.is_empty() {
        return None;
    }
    let (episode, absolute) = if let Some(s) = parsed.episode {
        (
            s,
            parsed.season.is_none() && parsed.absolute_episode.is_some(),
        )
    } else {
        let s = parsed.absolute_episode?;
        (s, true)
    };
    Some(EpisodeKey {
        series,
        season: if absolute { None } else { parsed.season },
        episode,
        absolute,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub enum DedupeDecision {
    Download,
    Upgrade,
    Skip(&'static str),
}

pub fn dedupe_decision(
    rule: &RssRule,
    candidate_score: i32,
    existing: Option<&EpisodeRecord>,
    now_secs: u64,
) -> DedupeDecision {
    let Some(existing) = existing else {
        return DedupeDecision::Download;
    };
    if rule.cooldown_secs > 0
        && now_secs.saturating_sub(existing.downloaded_at) < rule.cooldown_secs
    {
        return DedupeDecision::Skip("cooldown active");
    }
    if !rule.upgrade_existing {
        return DedupeDecision::Skip("already downloaded");
    }
    if candidate_score > existing.score {
        DedupeDecision::Upgrade
    } else {
        DedupeDecision::Skip("not a quality upgrade")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::rss::parser::parse_title;
    use crate::engine::rss::types::{
        EpisodeRecord, Pattern, PatternKind, RssItem, RuleMode, RuleStats,
    };

    fn rule_skeleton(name: &str) -> RssRule {
        RssRule {
            id: "r1".into(),
            name: name.into(),
            is_active: true,
            auto_download: true,
            feed_ids: vec![],
            priority: 0,
            mode: RuleMode::AnyMatch,
            title_must: vec![],
            title_must_not: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            min_seeders: None,
            series_filter: None,
            seasons: None,
            episodes: None,
            quality_preferences: vec![],
            required_qualities: vec![],
            forbidden_qualities: vec![],
            upgrade_existing: false,
            download_dir: None,
            filename_template: None,
            schedule: None,
            cooldown_secs: 0,
            stats: RuleStats::default(),
        }
    }

    fn item(title: &str) -> RssItem {
        RssItem {
            id: "i1".into(),
            feed_id: "f1".into(),
            title: title.into(),
            link: "".into(),
            pub_date: None,
            description: "".into(),
            content: String::new(),
            enclosure_url: Some("https://example/x.torrent".into()),
            enclosure_type: None,
            enclosure_length: None,
            is_read: false,
            is_downloaded: false,
            download_path: None,
            parsed_meta: None,
            matched_rule_id: None,
            media_urls: Vec::new(),
        }
    }

    fn pat_contains(v: &str) -> Pattern {
        Pattern {
            value: v.into(),
            kind: PatternKind::Contains,
            case_sensitive: false,
        }
    }

    #[test]
    fn must_and_must_not() {
        let mut rule = rule_skeleton("r");
        rule.title_must = vec![pat_contains("show name")];
        rule.title_must_not = vec![pat_contains("hdtv")];

        let it = item("Show.Name.S01E02.1080p.WEB-DL");
        let parsed = parse_title(&it.title);
        assert!(evaluate_rule(&rule, &it, &parsed).matched);

        let it = item("Show.Name.S01E02.HDTV");
        let parsed = parse_title(&it.title);
        assert!(!evaluate_rule(&rule, &it, &parsed).matched);

        let it = item("Other.Show.S01E02.1080p");
        let parsed = parse_title(&it.title);
        assert!(!evaluate_rule(&rule, &it, &parsed).matched);
    }

    #[test]
    fn series_filter_matches_only_when_parsed_series_contains_filter() {
        let it = item("episode");
        let mut rule = rule_skeleton("r");
        rule.series_filter = Some("attack on titan".into());
        let parsed = ParsedMeta {
            series: Some("titan".into()),
            ..ParsedMeta::default()
        };
        assert!(!evaluate_rule(&rule, &it, &parsed).matched);

        rule.series_filter = Some("titan".into());
        let parsed = ParsedMeta {
            series: Some("Attack on Titan".into()),
            ..ParsedMeta::default()
        };
        assert!(evaluate_rule(&rule, &it, &parsed).matched);
    }

    #[test]
    fn quality_scoring_orders_preferences() {
        let prefs: Vec<String> = ["2160p", "1080p", "720p"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(quality_score(&prefs, &["1080p".into()]) > quality_score(&prefs, &["720p".into()]));
        assert!(
            quality_score(&prefs, &["2160p".into()]) > quality_score(&prefs, &["1080p".into()])
        );
        assert_eq!(quality_score(&prefs, &["480p".into()]), 0);
    }

    #[test]
    fn upgrade_replaces_lower_score() {
        let mut rule = rule_skeleton("r");
        rule.upgrade_existing = true;
        let existing = EpisodeRecord {
            item_id: "old".into(),
            feed_id: "f1".into(),
            score: 800,
            downloaded_at: 0,
            file_path: None,
            rule_id: None,
        };
        assert_eq!(
            dedupe_decision(&rule, 1000, Some(&existing), 100),
            DedupeDecision::Upgrade
        );
        assert!(matches!(
            dedupe_decision(&rule, 800, Some(&existing), 100),
            DedupeDecision::Skip(_)
        ));
    }

    #[test]
    fn cooldown_blocks_repeat() {
        let mut rule = rule_skeleton("r");
        rule.upgrade_existing = true;
        rule.cooldown_secs = 3600;
        let existing = EpisodeRecord {
            item_id: "old".into(),
            feed_id: "f1".into(),
            score: 0,
            downloaded_at: 1000,
            file_path: None,
            rule_id: None,
        };
        assert!(matches!(
            dedupe_decision(&rule, 9999, Some(&existing), 1500),
            DedupeDecision::Skip(_)
        ));
        assert_eq!(
            dedupe_decision(&rule, 9999, Some(&existing), 5000),
            DedupeDecision::Upgrade
        );
    }

    #[test]
    fn schedule_window_basic() {
        let sched = Schedule {
            days: vec![],
            start_hour: 9,
            end_hour: 17,
            tz_offset_min: 0,
        };
        assert!(schedule_allows(&sched, 12 * 3600));
        assert!(!schedule_allows(&sched, 18 * 3600));
    }

    #[test]
    fn schedule_wraps_midnight() {
        let sched = Schedule {
            days: vec![],
            start_hour: 22,
            end_hour: 6,
            tz_offset_min: 0,
        };
        assert!(schedule_allows(&sched, 23 * 3600));
        assert!(schedule_allows(&sched, 3 * 3600));
        assert!(!schedule_allows(&sched, 12 * 3600));
    }

    #[test]
    fn episode_key_for_standard() {
        let parsed = parse_title("Show.Name.S01E02.1080p");
        let key = episode_key_for(&parsed).unwrap();
        assert_eq!(key.season, Some(1));
        assert_eq!(key.episode, 2);
        assert!(!key.absolute);
    }

    #[test]
    fn glob_pattern_matches() {
        let p = Pattern {
            value: "*.1080p.*".into(),
            kind: PatternKind::Glob,
            case_sensitive: false,
        };
        assert!(pattern_matches(&p, "Show.S01E02.1080p.WEB-DL"));
        assert!(!pattern_matches(&p, "Show.S01E02.720p.WEB-DL"));
    }
}
