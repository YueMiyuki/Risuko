use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RssFeed {
    pub id: String,
    pub url: String,
    pub title: String,
    pub site_link: String,
    pub description: String,
    pub update_interval_secs: u64,
    pub last_fetched_at: Option<u64>,
    pub created_at: u64,
    pub is_active: bool,
    pub error_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub series: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute_episode: Option<u32>,
    #[serde(default)]
    pub quality_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seeders: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RssItem {
    pub id: String,
    pub feed_id: String,
    pub title: String,
    pub link: String,
    pub pub_date: Option<u64>,
    pub description: String,
    #[serde(default)]
    pub content: String,
    pub enclosure_url: Option<String>,
    pub enclosure_type: Option<String>,
    pub enclosure_length: Option<u64>,
    pub is_read: bool,
    pub is_downloaded: bool,
    #[serde(default)]
    pub download_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parsed_meta: Option<ParsedMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_rule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_urls: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum PatternKind {
    #[default]
    Contains,
    Glob,
    Regex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    pub value: String,
    #[serde(default)]
    pub kind: PatternKind,
    #[serde(default)]
    pub case_sensitive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[derive(Default)]
pub enum EpisodeSelector {
    #[default]
    All,
    From {
        start: u32,
    },
    Range {
        start: u32,
        end: u32,
    },
    Specific {
        values: Vec<u32>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Weekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub days: Vec<Weekday>,
    pub start_hour: u8,
    pub end_hour: u8,
    #[serde(default)]
    pub tz_offset_min: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum RuleMode {
    #[default]
    AnyMatch,
    BestMatch,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuleStats {
    #[serde(default)]
    pub last_matched_at: Option<u64>,
    #[serde(default)]
    pub match_count: u64,
    #[serde(default)]
    pub download_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RssRule {
    pub id: String,
    pub name: String,
    #[serde(default = "default_true")]
    pub is_active: bool,
    #[serde(default = "default_true")]
    pub auto_download: bool,

    #[serde(default)]
    pub feed_ids: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub mode: RuleMode,

    #[serde(default)]
    pub title_must: Vec<Pattern>,
    #[serde(default)]
    pub title_must_not: Vec<Pattern>,
    #[serde(default)]
    pub min_size_bytes: Option<u64>,
    #[serde(default)]
    pub max_size_bytes: Option<u64>,
    #[serde(default)]
    pub min_seeders: Option<u32>,

    #[serde(default)]
    pub series_filter: Option<String>,
    #[serde(default)]
    pub seasons: Option<Vec<u32>>,
    #[serde(default)]
    pub episodes: Option<EpisodeSelector>,

    #[serde(default)]
    pub quality_preferences: Vec<String>,
    #[serde(default)]
    pub required_qualities: Vec<String>,
    #[serde(default)]
    pub forbidden_qualities: Vec<String>,
    #[serde(default)]
    pub upgrade_existing: bool,

    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub filename_template: Option<String>,

    #[serde(default)]
    pub schedule: Option<Schedule>,
    #[serde(default)]
    pub cooldown_secs: u64,

    #[serde(default)]
    pub stats: RuleStats,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EpisodeKey {
    pub series: String,
    pub season: Option<u32>,
    pub episode: u32,
    pub absolute: bool,
}

impl EpisodeKey {
    pub fn to_storage_key(&self) -> String {
        format!(
            "{}|s{}|e{}|{}",
            self.series,
            self.season.map(|v| v.to_string()).unwrap_or_default(),
            self.episode,
            if self.absolute { "abs" } else { "std" },
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeRecord {
    pub item_id: String,
    pub feed_id: String,
    pub score: i32,
    pub downloaded_at: u64,
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default)]
    pub rule_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RssStore {
    pub feeds: Vec<RssFeed>,
    pub items: HashMap<String, Vec<RssItem>>,
    #[serde(default, deserialize_with = "deserialize_rules_lossy")]
    pub rules: Vec<RssRule>,
    #[serde(default)]
    pub episode_history: HashMap<String, EpisodeRecord>,
}

fn deserialize_rules_lossy<'de, D>(deserializer: D) -> Result<Vec<RssRule>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    if let serde_json::Value::Array(arr) = value {
        return Ok(arr
            .into_iter()
            .filter_map(|elem| serde_json::from_value::<RssRule>(elem).ok())
            .collect());
    }
    Ok(serde_json::from_value(value).unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleEvaluation {
    pub matched: bool,
    pub score: i32,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunMatch {
    pub item_id: String,
    pub feed_id: String,
    pub title: String,
    pub matched: bool,
    pub score: i32,
    pub reason: String,
}
