pub mod parser;
pub mod rule_engine;
pub mod types;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use self::rule_engine::{
    dedupe_decision, episode_key_for, evaluate_rule, schedule_allows, DedupeDecision,
};
use self::types::*;
use crate::traits::StorageBackend;

const RSS_STORE_KEY: &str = "rss";
const DEFAULT_UPDATE_INTERVAL_SECS: u64 = 1800;
const MIN_UPDATE_INTERVAL_SECS: u64 = 60;
const MAX_ITEMS_PER_FEED: usize = 500;
const MAX_CONSECUTIVE_ERRORS: u32 = 5;
const MAX_EPISODE_HISTORY: usize = 10_000;
const MAX_FEED_BYTES: usize = 8 * 1024 * 1024;
const MAX_BACKOFF_SECS: u64 = 6 * 3600;
const FETCH_CONCURRENCY: usize = 6;

use crate::engine::util::now_secs;

fn item_id(guid_or_link: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(guid_or_link.as_bytes()))
}

struct PendingDownload {
    feed_id: String,
    item_id: String,
    title: String,
    rule_id: String,
    rule_name: String,
    key: Option<EpisodeKey>,
    score: i32,
    queued_at: u64,
    prev_episode: Option<EpisodeRecord>,
}

pub struct RssManager {
    store: Arc<Mutex<RssStore>>,
    storage: Arc<dyn StorageBackend>,
    save_gate: Arc<Mutex<()>>,
    pending: Arc<Mutex<HashMap<String, PendingDownload>>>,
    monitor_active: Arc<AtomicBool>,
}

struct FetchFailure {
    message: String,
    transient: bool,
}

enum Fetched {
    NotModified,
    Parsed {
        feed: Box<feed_rs::model::Feed>,
        etag: Option<String>,
        last_modified: Option<String>,
    },
}

async fn persist(
    store: &Mutex<RssStore>,
    storage: &Arc<dyn StorageBackend>,
    gate: &Mutex<()>,
) -> Result<(), String> {
    let _gate = gate.lock().await;
    let wrapper = {
        let s = store.lock().await;
        let data =
            serde_json::to_value(&*s).map_err(|e| format!("Serialize RSS data failed: {e}"))?;
        serde_json::json!({ "data": data })
    };
    let storage = Arc::clone(storage);
    tokio::task::spawn_blocking(move || storage.save(RSS_STORE_KEY, &wrapper))
        .await
        .map_err(|e| format!("RSS save task failed: {e}"))?
}

fn feed_retry_delay(interval: u64, error_count: u32) -> u64 {
    if error_count == 0 {
        return interval;
    }
    let shift = error_count.min(16);
    interval
        .saturating_mul(1u64 << shift)
        .min(MAX_BACKOFF_SECS)
        .max(interval)
}

fn feed_is_due(feed: &RssFeed, now: u64) -> bool {
    let last = feed
        .last_attempt_at
        .into_iter()
        .chain(feed.last_fetched_at)
        .max()
        .unwrap_or(0);
    now >= last.saturating_add(feed_retry_delay(
        feed.update_interval_secs,
        feed.error_count,
    ))
}

impl RssManager {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            store: Arc::new(Mutex::new(RssStore::default())),
            storage,
            save_gate: Arc::new(Mutex::new(())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            monitor_active: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn load(&self) -> Result<(), String> {
        if let Some(val) = self.storage.load(RSS_STORE_KEY)? {
            if let Some(data_val) = val.get("data").cloned() {
                let data: RssStore = serde_json::from_value(data_val)
                    .map_err(|e| format!("Failed to parse RSS data: {e}"))?;
                // blocking_lock() would panic inside a Tokio worker
                let mut s = self
                    .store
                    .try_lock()
                    .map_err(|_| "RSS store busy during load".to_string())?;
                *s = data;
                for items in s.items.values_mut() {
                    for item in items.iter_mut() {
                        if item.parsed_meta.is_none() {
                            item.parsed_meta = Some(parser::parse_title(&item.title));
                        }
                    }
                }
                s.rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
            }
        }
        Ok(())
    }

    pub async fn save(&self) -> Result<(), String> {
        persist(&self.store, &self.storage, &self.save_gate).await
    }

    pub async fn add_feed(&self, url: &str) -> Result<RssFeed, String> {
        let parsed = match fetch_feed(url, None, None).await {
            Ok(Fetched::Parsed { feed, .. }) => feed,
            Ok(Fetched::NotModified) => return Err("Feed returned no content".to_string()),
            Err(e) => return Err(e.message),
        };

        let title = parsed
            .title
            .map(|t| t.content)
            .unwrap_or_else(|| url.to_string());
        let site_link = parsed
            .links
            .first()
            .map(|l| l.href.clone())
            .unwrap_or_default();
        let description = parsed.description.map(|d| d.content).unwrap_or_default();

        let feed = RssFeed {
            id: Uuid::new_v4().to_string(),
            url: url.to_string(),
            title,
            site_link,
            description,
            update_interval_secs: DEFAULT_UPDATE_INTERVAL_SECS,
            last_fetched_at: Some(now_secs()),
            created_at: now_secs(),
            is_active: true,
            error_count: 0,
            last_attempt_at: Some(now_secs()),
            etag: None,
            last_modified: None,
        };

        let items = extract_items(&feed.id, &parsed.entries);

        {
            let mut s = self.store.lock().await;
            if s.feeds.iter().any(|f| f.url == url) {
                return Err("Feed already subscribed".to_string());
            }
            s.feeds.push(feed.clone());
            let mut items_list = items;
            items_list.truncate(MAX_ITEMS_PER_FEED);
            s.items.insert(feed.id.clone(), items_list);
        }

        self.save().await?;
        Ok(feed)
    }

    pub async fn remove_feed(&self, feed_id: &str) -> Result<(), String> {
        let mut s = self.store.lock().await;
        s.feeds.retain(|f| f.id != feed_id);
        s.items.remove(feed_id);
        s.rules.retain_mut(|r| {
            let was_scoped = !r.feed_ids.is_empty();
            r.feed_ids.retain(|f| f != feed_id);
            !was_scoped || !r.feed_ids.is_empty()
        });
        drop(s);
        self.save().await
    }

    pub async fn update_feed(&self, feed_id: &str) -> Result<Vec<RssItem>, String> {
        let (url, etag, last_modified) = {
            let s = self.store.lock().await;
            let f = s
                .feeds
                .iter()
                .find(|f| f.id == feed_id)
                .ok_or_else(|| "Feed not found".to_string())?;
            (f.url.clone(), f.etag.clone(), f.last_modified.clone())
        };

        let result = fetch_feed(&url, etag, last_modified).await;
        let outcome = self.apply_fetch(feed_id, result).await;
        self.save().await?;
        outcome
    }

    async fn apply_fetch(
        &self,
        feed_id: &str,
        result: Result<Fetched, FetchFailure>,
    ) -> Result<Vec<RssItem>, String> {
        let mut s = self.store.lock().await;
        let now = now_secs();
        let feed = s
            .feeds
            .iter_mut()
            .find(|f| f.id == feed_id)
            .ok_or_else(|| "Feed not found".to_string())?;
        feed.last_attempt_at = Some(now);

        match result {
            Ok(Fetched::NotModified) => {
                feed.last_fetched_at = Some(now);
                feed.error_count = 0;
                Ok(Vec::new())
            }
            Ok(Fetched::Parsed {
                feed: parsed,
                etag,
                last_modified,
            }) => {
                feed.last_fetched_at = Some(now);
                feed.error_count = 0;
                feed.etag = etag;
                feed.last_modified = last_modified;
                if let Some(title) = parsed.title {
                    feed.title = title.content;
                }

                let existing = s.items.entry(feed_id.to_string()).or_default();
                let known: HashMap<String, usize> = existing
                    .iter()
                    .enumerate()
                    .map(|(i, it)| (it.id.clone(), i))
                    .collect();
                let mut fresh: Vec<RssItem> = Vec::new();
                let mut seen_new: HashSet<String> = HashSet::new();
                for entry in parsed.entries.iter().take(MAX_ITEMS_PER_FEED) {
                    let id = entry_item_id(entry);
                    if let Some(&idx) = known.get(&id) {
                        let old = &mut existing[idx];
                        if old.content.is_empty() {
                            if let Some(body) = entry.content.as_ref().and_then(|c| c.body.clone())
                            {
                                old.content = body;
                            }
                        }
                    } else if seen_new.insert(id) {
                        fresh.push(extract_item(feed_id, entry));
                    }
                }

                if !fresh.is_empty() {
                    let mut merged = Vec::with_capacity(fresh.len() + existing.len());
                    merged.extend(fresh.iter().cloned());
                    merged.append(existing);
                    merged.truncate(MAX_ITEMS_PER_FEED);
                    *existing = merged;
                }
                Ok(fresh)
            }
            Err(e) => {
                feed.error_count = feed.error_count.saturating_add(1);
                if !e.transient && feed.error_count >= MAX_CONSECUTIVE_ERRORS {
                    tracing::warn!(
                        "Feed '{}' disabled after {} consecutive errors",
                        feed.title,
                        feed.error_count
                    );
                    feed.is_active = false;
                }
                Err(e.message)
            }
        }
    }

    pub async fn update_all_feeds(&self) -> Vec<(String, Vec<RssItem>)> {
        self.update_feeds(false).await
    }

    async fn update_feeds(&self, only_due: bool) -> Vec<(String, Vec<RssItem>)> {
        let now = now_secs();
        let due: Vec<(String, String, Option<String>, Option<String>)> = {
            let s = self.store.lock().await;
            s.feeds
                .iter()
                .filter(|f| f.is_active && (!only_due || feed_is_due(f, now)))
                .map(|f| {
                    (
                        f.id.clone(),
                        f.url.clone(),
                        f.etag.clone(),
                        f.last_modified.clone(),
                    )
                })
                .collect()
        };
        if due.is_empty() {
            return Vec::new();
        }

        let fetched: Vec<(String, Result<Fetched, FetchFailure>)> = futures_util::stream::iter(due)
            .map(|(id, url, etag, last_modified)| async move {
                let result = fetch_feed(&url, etag, last_modified).await;
                (id, result)
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await;

        let mut all_new = Vec::new();
        for (feed_id, result) in fetched {
            match self.apply_fetch(&feed_id, result).await {
                Ok(new_items) if !new_items.is_empty() => {
                    all_new.push((feed_id, new_items));
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("Failed to update feed {}: {}", feed_id, e);
                }
            }
        }
        if let Err(e) = self.save().await {
            tracing::warn!("Failed to save RSS store: {}", e);
        }
        all_new
    }

    pub async fn get_feeds(&self) -> Vec<RssFeed> {
        self.store.lock().await.feeds.clone()
    }

    pub async fn get_items(&self, feed_id: &str) -> Vec<RssItem> {
        let candidates: Vec<(String, String)> = {
            let s = self.store.lock().await;
            let Some(items) = s.items.get(feed_id) else {
                return Vec::new();
            };
            items
                .iter()
                .filter(|i| i.is_downloaded)
                .filter_map(|i| Some((i.id.clone(), i.download_path.clone()?)))
                .collect()
        };

        let stale: HashSet<String> = if candidates.is_empty() {
            HashSet::new()
        } else {
            tokio::task::spawn_blocking(move || {
                candidates
                    .into_iter()
                    .filter(|(_, path)| {
                        let path = std::path::Path::new(path);
                        !path.exists() && path.parent().is_some_and(|parent| parent.exists())
                    })
                    .map(|(id, _)| id)
                    .collect()
            })
            .await
            .unwrap_or_default()
        };

        let mut changed = false;
        let items = {
            let mut s = self.store.lock().await;
            let Some(items) = s.items.get_mut(feed_id) else {
                return Vec::new();
            };
            if !stale.is_empty() {
                for item in items.iter_mut() {
                    if item.is_downloaded && stale.contains(&item.id) {
                        item.is_downloaded = false;
                        item.download_path = None;
                        changed = true;
                    }
                }
            }
            items.clone()
        };
        if changed {
            let _ = self.save().await;
        }
        items
    }

    pub async fn get_item(&self, feed_id: &str, item_id: &str) -> Result<RssItem, String> {
        let s = self.store.lock().await;
        let items = s
            .items
            .get(feed_id)
            .ok_or_else(|| "Feed not found".to_string())?;
        items
            .iter()
            .find(|i| i.id == item_id)
            .cloned()
            .ok_or_else(|| "Item not found".to_string())
    }

    pub async fn update_feed_settings(
        &self,
        feed_id: &str,
        interval: Option<u64>,
        is_active: Option<bool>,
    ) -> Result<(), String> {
        let mut s = self.store.lock().await;
        let feed = s
            .feeds
            .iter_mut()
            .find(|f| f.id == feed_id)
            .ok_or_else(|| "Feed not found".to_string())?;
        if let Some(interval) = interval {
            feed.update_interval_secs = interval.max(MIN_UPDATE_INTERVAL_SECS);
        }
        if let Some(active) = is_active {
            feed.is_active = active;
            if active {
                feed.error_count = 0;
            }
        }
        drop(s);
        self.save().await
    }

    pub async fn mark_item_downloaded(
        &self,
        feed_id: &str,
        item_id: &str,
        download_path: Option<String>,
    ) -> Result<(), String> {
        let mut s = self.store.lock().await;
        if let Some(items) = s.items.get_mut(feed_id) {
            if let Some(item) = items.iter_mut().find(|i| i.id == item_id) {
                item.is_downloaded = true;
                item.is_read = true;
                item.download_path = download_path;
            }
        }
        drop(s);
        self.save().await
    }

    pub async fn mark_item_read(&self, feed_id: &str, item_id: &str) -> Result<(), String> {
        self.mark_items_read(vec![(feed_id.to_string(), item_id.to_string())])
            .await
    }

    pub async fn mark_items_read(&self, entries: Vec<(String, String)>) -> Result<(), String> {
        let mut s = self.store.lock().await;
        for (feed_id, item_id) in &entries {
            if let Some(items) = s.items.get_mut(feed_id.as_str()) {
                if let Some(item) = items.iter_mut().find(|i| i.id == *item_id) {
                    item.is_read = true;
                }
            }
        }
        drop(s);
        self.save().await
    }

    pub async fn clear_item_download(&self, feed_id: &str, item_id: &str) -> Result<(), String> {
        let mut s = self.store.lock().await;
        let mut path_to_delete: Option<String> = None;
        if let Some(items) = s.items.get_mut(feed_id) {
            if let Some(item) = items.iter_mut().find(|i| i.id == item_id) {
                path_to_delete = item.download_path.take();
                item.is_downloaded = false;
            }
        }
        drop(s);

        if let Some(path) = path_to_delete {
            let p = std::path::Path::new(&path);
            if p.exists() {
                if let Err(e) = tokio::fs::remove_file(p).await {
                    tracing::warn!("Failed to delete downloaded file {}: {}", path, e);
                }
            }
        }

        self.save().await
    }

    pub async fn delete_items(
        &self,
        items_by_feed: Vec<(String, Vec<String>)>,
    ) -> Result<(), String> {
        let mut s = self.store.lock().await;
        let mut paths_to_delete: Vec<String> = Vec::new();
        for (feed_id, item_ids) in &items_by_feed {
            if let Some(items) = s.items.get_mut(feed_id) {
                let id_set: std::collections::HashSet<&str> =
                    item_ids.iter().map(|s| s.as_str()).collect();
                for item in items.iter() {
                    if id_set.contains(item.id.as_str()) {
                        if let Some(ref path) = item.download_path {
                            paths_to_delete.push(path.clone());
                        }
                    }
                }
                items.retain(|i| !id_set.contains(i.id.as_str()));
            }
        }
        drop(s);

        for path in &paths_to_delete {
            let p = std::path::Path::new(path);
            if p.exists() {
                if let Err(e) = tokio::fs::remove_file(p).await {
                    tracing::warn!("Failed to delete downloaded file {}: {}", path, e);
                }
            }
        }

        self.save().await
    }

    pub async fn get_item_download_url(
        &self,
        feed_id: &str,
        item_id: &str,
    ) -> Result<String, String> {
        let item = self.get_item(feed_id, item_id).await?;
        item.enclosure_url
            .clone()
            .or_else(|| (!item.link.is_empty()).then(|| item.link.clone()))
            .ok_or_else(|| "No downloadable URL found for this item".to_string())
    }

    pub async fn get_item_download_urls(
        &self,
        feed_id: &str,
        item_id: &str,
    ) -> Result<Vec<String>, String> {
        let item = self.get_item(feed_id, item_id).await?;
        let mut urls: Vec<String> = Vec::new();
        if let Some(ref u) = item.enclosure_url {
            urls.push(u.clone());
        }
        for u in &item.media_urls {
            if !urls.contains(u) {
                urls.push(u.clone());
            }
        }
        if urls.is_empty() && !item.link.is_empty() {
            urls.push(item.link.clone());
        }
        if urls.is_empty() {
            return Err("No downloadable URL found for this item".into());
        }
        Ok(urls)
    }

    pub async fn get_item_download_path(
        &self,
        feed_id: &str,
        item_id: &str,
    ) -> Result<String, String> {
        let item = self.get_item(feed_id, item_id).await?;
        item.download_path
            .ok_or_else(|| "No download path recorded".to_string())
    }

    pub async fn add_rule(&self, rule: RssRule) -> Result<RssRule, String> {
        validate_rule(&rule)?;
        let rule = RssRule {
            id: Uuid::new_v4().to_string(),
            stats: RuleStats::default(),
            ..rule
        };
        let mut s = self.store.lock().await;
        s.rules.push(rule.clone());
        s.rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
        drop(s);
        self.save().await?;
        Ok(rule)
    }

    pub async fn update_rule(&self, rule: RssRule) -> Result<RssRule, String> {
        validate_rule(&rule)?;
        let mut s = self.store.lock().await;
        let idx = s
            .rules
            .iter()
            .position(|r| r.id == rule.id)
            .ok_or_else(|| "Rule not found".to_string())?;
        let preserved_stats = s.rules[idx].stats.clone();
        let updated = RssRule {
            stats: preserved_stats,
            ..rule
        };
        s.rules[idx] = updated.clone();
        s.rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
        drop(s);
        self.save().await?;
        Ok(updated)
    }

    pub async fn remove_rule(&self, rule_id: &str) -> Result<(), String> {
        let mut s = self.store.lock().await;
        s.rules.retain(|r| r.id != rule_id);
        drop(s);
        self.save().await
    }

    pub async fn reorder_rules(&self, ordered_ids: Vec<String>) -> Result<(), String> {
        let mut s = self.store.lock().await;
        let n = ordered_ids.len() as i32;
        for (idx, id) in ordered_ids.iter().enumerate() {
            if let Some(rule) = s.rules.iter_mut().find(|r| &r.id == id) {
                rule.priority = n - idx as i32;
            }
        }
        s.rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
        drop(s);
        self.save().await
    }

    pub async fn get_rules(&self) -> Vec<RssRule> {
        self.store.lock().await.rules.clone()
    }

    pub async fn best_matching_rule(
        &self,
        item: &RssItem,
        parsed: &ParsedMeta,
    ) -> Option<(RssRule, i32)> {
        let s = self.store.lock().await;
        let mut best: Option<(RssRule, i32)> = None;
        for rule in &s.rules {
            if !rule.auto_download {
                continue;
            }
            let eval = evaluate_rule(rule, item, parsed);
            if !eval.matched {
                continue;
            }
            match rule.mode {
                RuleMode::AnyMatch => {
                    if let Some((_, bs)) = &best {
                        if eval.score < *bs {
                            continue;
                        }
                    }
                    return Some((rule.clone(), eval.score));
                }
                RuleMode::BestMatch => {
                    if best.as_ref().map(|(_, s)| eval.score > *s).unwrap_or(true) {
                        best = Some((rule.clone(), eval.score));
                    }
                }
            }
        }
        best
    }

    pub async fn dry_run_rule(&self, rule: RssRule, sample_size: usize) -> Vec<DryRunMatch> {
        let items: Vec<RssItem> = {
            let s = self.store.lock().await;
            let mut all: Vec<RssItem> = s.items.values().flat_map(|v| v.iter().cloned()).collect();
            all.sort_by_key(|i| std::cmp::Reverse(i.pub_date.unwrap_or(0)));
            all.truncate(sample_size.max(1));
            all
        };
        items
            .into_iter()
            .map(|item| {
                let parsed = item
                    .parsed_meta
                    .clone()
                    .unwrap_or_else(|| parser::parse_title(&item.title));
                let eval = evaluate_rule(&rule, &item, &parsed);
                DryRunMatch {
                    item_id: item.id,
                    feed_id: item.feed_id,
                    title: item.title,
                    matched: eval.matched,
                    score: eval.score,
                    reason: eval.reason,
                }
            })
            .collect()
    }

    pub fn start_polling(rss: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

            loop {
                let new_items_per_feed = rss.update_feeds(true).await;

                let mut dirty = false;
                for (feed_id, new_items) in &new_items_per_feed {
                    for item in new_items {
                        if item.is_downloaded {
                            continue;
                        }
                        dirty |= rss.evaluate_and_download(feed_id, item).await;
                    }
                }
                if dirty {
                    if let Err(e) = rss.save().await {
                        tracing::warn!("Failed to save RSS store: {}", e);
                    }
                }

                let min_interval = {
                    let s = rss.store.lock().await;
                    s.feeds
                        .iter()
                        .filter(|f| f.is_active)
                        .map(|f| f.update_interval_secs)
                        .min()
                        .unwrap_or(DEFAULT_UPDATE_INTERVAL_SECS)
                };
                let min_interval = min_interval.max(MIN_UPDATE_INTERVAL_SECS);

                tokio::time::sleep(tokio::time::Duration::from_secs(min_interval)).await;
            }
        })
    }

    async fn evaluate_and_download(&self, feed_id: &str, item: &RssItem) -> bool {
        let parsed = item
            .parsed_meta
            .clone()
            .unwrap_or_else(|| parser::parse_title(&item.title));
        let Some((rule, score)) = self.best_matching_rule(item, &parsed).await else {
            return false;
        };

        let now = now_secs();
        if let Some(ref sched) = rule.schedule {
            if !schedule_allows(sched, now) {
                tracing::debug!(
                    "RSS rule '{}' matched '{}' but schedule denies",
                    rule.name,
                    item.title
                );
                return false;
            }
        }

        let key = episode_key_for(&parsed);
        let decision = if let Some(ref k) = key {
            let storage_key = k.to_storage_key();
            let existing = {
                let s = self.store.lock().await;
                s.episode_history.get(&storage_key).cloned()
            };
            dedupe_decision(&rule, score, existing.as_ref(), now)
        } else {
            DedupeDecision::Download
        };

        match decision {
            DedupeDecision::Skip(reason) => {
                tracing::debug!(
                    "RSS rule '{}' skipped '{}': {}",
                    rule.name,
                    item.title,
                    reason
                );
                return false;
            }
            DedupeDecision::Download | DedupeDecision::Upgrade => {}
        }

        let mut opts = serde_json::Map::new();
        if let Some(ref dir) = rule.download_dir {
            opts.insert("dir".to_string(), Value::String(dir.clone()));
        }

        let Some(manager) = super::get_manager().await else {
            tracing::warn!("RSS auto-download: engine not available");
            return false;
        };

        let kind = classify_item_kind(item);
        let download_path: Option<String> = if kind == ItemKind::Article {
            let dir = match opts.get("dir").and_then(|v| v.as_str()) {
                Some(d) => d.to_string(),
                None => manager
                    .get_global_option()
                    .await
                    .get("dir")
                    .and_then(|v| v.as_str())
                    .unwrap_or(".")
                    .to_string(),
            };
            let filename = article_filename(item);
            let html = build_article_html(item);
            let dir_path = std::path::Path::new(&dir);
            if let Err(e) = tokio::fs::create_dir_all(dir_path).await {
                tracing::warn!("Auto-download: failed to create dir {}: {}", dir, e);
                return false;
            }
            let file_path = dir_path.join(&filename);
            let path_str = file_path.to_string_lossy().to_string();
            if let Err(e) = tokio::fs::write(&file_path, html.as_bytes()).await {
                tracing::warn!("Auto-download: failed to write article html: {}", e);
                return false;
            }
            let mut media_opts = opts.clone();
            media_opts.remove("out");
            let mut queued: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for extra in item
                .enclosure_url
                .iter()
                .cloned()
                .chain(item.media_urls.iter().cloned())
            {
                if !queued.insert(extra.clone()) {
                    continue;
                }
                if let Err(e) = manager
                    .add_http_task(vec![extra.clone()], media_opts.clone())
                    .await
                {
                    tracing::warn!("Auto-download extra media failed for '{}': {}", extra, e);
                }
            }
            Some(path_str)
        } else {
            let url = match self.get_item_download_url(&item.feed_id, &item.id).await {
                Ok(u) => u,
                Err(e) => {
                    tracing::warn!("RSS auto-download: no URL for '{}': {}", item.title, e);
                    return false;
                }
            };
            let gid = match manager.add_http_task(vec![url.clone()], opts.clone()).await {
                Ok(g) => g,
                Err(e) => {
                    tracing::warn!("Auto-download failed for '{}': {}", item.title, e);
                    return false;
                }
            };
            let mut media_opts = opts.clone();
            media_opts.remove("out");
            for extra in &item.media_urls {
                if extra == &url {
                    continue;
                }
                if let Err(e) = manager
                    .add_http_task(vec![extra.clone()], media_opts.clone())
                    .await
                {
                    tracing::warn!("Auto-download extra media failed for '{}': {}", extra, e);
                }
            }

            let prev_episode = {
                let mut s = self.store.lock().await;
                let prev = key
                    .as_ref()
                    .and_then(|k| s.episode_history.get(&k.to_storage_key()).cloned());
                if let Some(k) = key.as_ref() {
                    record_episode(&mut s, k, feed_id, &item.id, &rule.id, score, now, None);
                }
                prev
            };
            self.track_download(
                gid,
                PendingDownload {
                    feed_id: feed_id.to_string(),
                    item_id: item.id.clone(),
                    title: item.title.clone(),
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    key: key.clone(),
                    score,
                    queued_at: now,
                    prev_episode,
                },
            )
            .await;
            return true;
        };

        let mut s = self.store.lock().await;
        record_download(
            &mut s,
            feed_id,
            &item.id,
            &rule.id,
            key.as_ref(),
            score,
            now,
            download_path,
        );
        drop(s);
        tracing::info!("Auto-downloaded '{}' via rule '{}'", item.title, rule.name);
        true
    }

    async fn track_download(&self, gid: String, pending: PendingDownload) {
        let mut map = self.pending.lock().await;
        map.insert(gid, pending);
        if !self.monitor_active.swap(true, Ordering::SeqCst) {
            tokio::spawn(run_download_monitor(
                Arc::clone(&self.store),
                Arc::clone(&self.storage),
                Arc::clone(&self.save_gate),
                Arc::clone(&self.pending),
                Arc::clone(&self.monitor_active),
            ));
        }
    }
}

async fn run_download_monitor(
    store: Arc<Mutex<RssStore>>,
    storage: Arc<dyn StorageBackend>,
    gate: Arc<Mutex<()>>,
    pending: Arc<Mutex<HashMap<String, PendingDownload>>>,
    active: Arc<AtomicBool>,
) {
    let fields = ["status".to_string(), "dir".to_string(), "files".to_string()];
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let gids: Vec<String> = {
            let map = pending.lock().await;
            if map.is_empty() {
                active.store(false, Ordering::SeqCst);
                return;
            }
            map.keys().cloned().collect()
        };

        let engine = super::get_manager().await;
        let mut changed = false;
        for gid in gids {
            let status_val = match &engine {
                Some(engine) => engine.tell_status(&gid, &fields).await.ok(),
                None => None,
            };
            let status = status_val
                .as_ref()
                .and_then(|v| v.get("status"))
                .and_then(|s| s.as_str())
                .unwrap_or("removed");
            if !matches!(status, "complete" | "error" | "removed") {
                continue;
            }
            let Some(p) = pending.lock().await.remove(&gid) else {
                continue;
            };
            if status == "complete" {
                let download_path = status_val
                    .as_ref()
                    .and_then(|v| v.get("files"))
                    .and_then(|f| f.as_array())
                    .and_then(|a| a.first())
                    .and_then(|f| f.get("path"))
                    .and_then(|p| p.as_str())
                    .filter(|p| !p.is_empty())
                    .map(|p| p.to_string());
                let mut s = store.lock().await;
                record_download(
                    &mut s,
                    &p.feed_id,
                    &p.item_id,
                    &p.rule_id,
                    p.key.as_ref(),
                    p.score,
                    p.queued_at,
                    download_path,
                );
                drop(s);
                tracing::info!("Auto-downloaded '{}' via rule '{}'", p.title, p.rule_name);
            } else {
                tracing::warn!(
                    "RSS auto-download task {} for '{}' ended with status '{}'",
                    gid,
                    p.title,
                    status
                );
                let mut s = store.lock().await;
                rollback_episode(&mut s, &p);
            }
            changed = true;
        }
        if changed {
            if let Err(e) = persist(&store, &storage, &gate).await {
                tracing::warn!("Failed to save RSS store: {}", e);
            }
        }
    }
}

fn validate_rule(rule: &RssRule) -> Result<(), String> {
    if rule.name.trim().is_empty() {
        return Err("Rule name cannot be empty".into());
    }
    for p in rule.title_must.iter().chain(rule.title_must_not.iter()) {
        match p.kind {
            PatternKind::Regex => {
                regex::Regex::new(&p.value)
                    .map_err(|e| format!("Invalid regex pattern '{}': {e}", p.value))?;
            }
            PatternKind::Glob => {
                globset::Glob::new(&p.value)
                    .map_err(|e| format!("Invalid glob pattern '{}': {e}", p.value))?;
            }
            PatternKind::Contains => {}
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn record_download(
    s: &mut RssStore,
    feed_id: &str,
    item_id: &str,
    rule_id: &str,
    key: Option<&EpisodeKey>,
    score: i32,
    now: u64,
    path: Option<String>,
) {
    if let Some(items) = s.items.get_mut(feed_id) {
        if let Some(it) = items.iter_mut().find(|i| i.id == item_id) {
            it.is_downloaded = true;
            it.is_read = true;
            it.download_path = path.clone();
            it.matched_rule_id = Some(rule_id.to_string());
        }
    }
    if let Some(rule_mut) = s.rules.iter_mut().find(|r| r.id == rule_id) {
        rule_mut.stats.last_matched_at = Some(now);
        rule_mut.stats.match_count = rule_mut.stats.match_count.saturating_add(1);
        rule_mut.stats.download_count = rule_mut.stats.download_count.saturating_add(1);
    }
    if let Some(k) = key {
        record_episode(s, k, feed_id, item_id, rule_id, score, now, path);
    }
}

#[allow(clippy::too_many_arguments)]
fn record_episode(
    s: &mut RssStore,
    key: &EpisodeKey,
    feed_id: &str,
    item_id: &str,
    rule_id: &str,
    score: i32,
    now: u64,
    path: Option<String>,
) {
    s.episode_history.insert(
        key.to_storage_key(),
        EpisodeRecord {
            item_id: item_id.to_string(),
            feed_id: feed_id.to_string(),
            score,
            downloaded_at: now,
            file_path: path,
            rule_id: Some(rule_id.to_string()),
        },
    );
    if s.episode_history.len() > MAX_EPISODE_HISTORY {
        let mut entries: Vec<(String, u64)> = s
            .episode_history
            .iter()
            .map(|(k, v)| (k.clone(), v.downloaded_at))
            .collect();
        entries.sort_by_key(|(_, ts)| *ts);
        let excess = s.episode_history.len() - MAX_EPISODE_HISTORY;
        for (k, _) in entries.into_iter().take(excess) {
            s.episode_history.remove(&k);
        }
    }
}

fn rollback_episode(s: &mut RssStore, p: &PendingDownload) {
    let Some(k) = p.key.as_ref() else {
        return;
    };
    let storage_key = k.to_storage_key();
    let ours = s
        .episode_history
        .get(&storage_key)
        .is_some_and(|r| r.item_id == p.item_id && r.file_path.is_none());
    if !ours {
        return;
    }
    match p.prev_episode.clone() {
        Some(prev) => {
            s.episode_history.insert(storage_key, prev);
        }
        None => {
            s.episode_history.remove(&storage_key);
        }
    }
}

fn http_client() -> Result<&'static risuko_http::Client, String> {
    static CLIENT: std::sync::OnceLock<risuko_http::Client> = std::sync::OnceLock::new();
    if let Some(c) = CLIENT.get() {
        return Ok(c);
    }
    let client = risuko_http::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("Risuko/1.0")
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .build()
        .map_err(|e| format!("Failed to build rss http client: {e}"))?;
    let _ = CLIENT.set(client);
    CLIENT
        .get()
        .ok_or_else(|| "rss http client unavailable".to_string())
}

fn header_string(
    resp: &risuko_http::Response,
    name: risuko_http::header::HeaderName,
) -> Option<String> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
}

async fn fetch_feed(
    url: &str,
    etag: Option<String>,
    last_modified: Option<String>,
) -> Result<Fetched, FetchFailure> {
    let transient = |message: String| FetchFailure {
        message,
        transient: true,
    };
    let client = http_client().map_err(transient)?;
    let mut req = client.get(url);
    if let Some(etag) = etag.as_deref() {
        req = req.header(risuko_http::header::IF_NONE_MATCH, etag);
    }
    if let Some(lm) = last_modified.as_deref() {
        req = req.header(risuko_http::header::IF_MODIFIED_SINCE, lm);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| transient(format!("Failed to fetch feed: {e}")))?;

    let status = resp.status();
    if status == risuko_http::StatusCode::NOT_MODIFIED {
        return Ok(Fetched::NotModified);
    }
    if !status.is_success() {
        return Err(FetchFailure {
            message: format!("Feed returned HTTP {status}"),
            transient: status.is_server_error()
                || status == risuko_http::StatusCode::TOO_MANY_REQUESTS
                || status == risuko_http::StatusCode::REQUEST_TIMEOUT,
        });
    }
    let new_etag = header_string(&resp, risuko_http::header::ETAG);
    let new_last_modified = header_string(&resp, risuko_http::header::LAST_MODIFIED);
    let body = resp.bytes_limited(MAX_FEED_BYTES).await.map_err(|e| {
        let too_large = e.to_string().contains("exceeds");
        FetchFailure {
            message: if too_large {
                "Feed body too large".to_string()
            } else {
                format!("Failed to read feed body: {e}")
            },
            transient: !too_large,
        }
    })?;

    let feed = tokio::task::spawn_blocking(move || feed_rs::parser::parse(&body[..]))
        .await
        .map_err(|e| transient(format!("Feed parse task failed: {e}")))?
        .map_err(|e| FetchFailure {
            message: format!("Failed to parse feed: {e}"),
            transient: false,
        })?;
    Ok(Fetched::Parsed {
        feed: Box::new(feed),
        etag: new_etag,
        last_modified: new_last_modified,
    })
}

fn entry_item_id(entry: &feed_rs::model::Entry) -> String {
    if entry.id.is_empty() {
        item_id(entry.links.first().map(|l| l.href.as_str()).unwrap_or(""))
    } else {
        item_id(&entry.id)
    }
}

fn extract_items(feed_id: &str, entries: &[feed_rs::model::Entry]) -> Vec<RssItem> {
    entries
        .iter()
        .map(|entry| extract_item(feed_id, entry))
        .collect()
}

fn extract_item(feed_id: &str, entry: &feed_rs::model::Entry) -> RssItem {
    {
        {
            let guid = entry.id.clone();
            let link = entry
                .links
                .first()
                .map(|l| l.href.clone())
                .unwrap_or_default();

            let id_source = if guid.is_empty() { &link } else { &guid };
            let id = item_id(id_source);

            let title = entry
                .title
                .as_ref()
                .map(|t| t.content.clone())
                .unwrap_or_default();

            let description = entry
                .summary
                .as_ref()
                .map(|s| s.content.clone())
                .or_else(|| entry.content.as_ref().and_then(|c| c.body.clone()))
                .unwrap_or_default();

            let content = entry
                .content
                .as_ref()
                .and_then(|c| c.body.clone())
                .unwrap_or_default();

            let pub_date = entry
                .published
                .or(entry.updated)
                .map(|dt| dt.timestamp() as u64);

            let (enc_url, enc_type, enc_len) = extract_enclosure(entry);

            let media_urls = extract_media_urls(entry, enc_url.as_deref());

            let parsed_meta = if title.is_empty() {
                None
            } else {
                Some(parser::parse_title(&title))
            };

            RssItem {
                id,
                feed_id: feed_id.to_string(),
                title,
                link,
                pub_date,
                description,
                content,
                enclosure_url: enc_url,
                enclosure_type: enc_type,
                enclosure_length: enc_len,
                is_read: false,
                is_downloaded: false,
                download_path: None,
                parsed_meta,
                matched_rule_id: None,
                media_urls,
            }
        }
    }
}

fn extract_enclosure(
    entry: &feed_rs::model::Entry,
) -> (Option<String>, Option<String>, Option<u64>) {
    let mut candidates: Vec<(String, Option<String>, Option<u64>)> = Vec::new();

    for media in &entry.media {
        for content in &media.content {
            if let Some(ref url) = content.url {
                candidates.push((
                    url.to_string(),
                    content.content_type.as_ref().map(|m| m.to_string()),
                    content.size,
                ));
            }
        }
    }

    for link in &entry.links {
        if link.rel.as_deref() == Some("enclosure") {
            candidates.push((link.href.clone(), link.media_type.clone(), link.length));
        }
    }

    match candidates
        .into_iter()
        .min_by_key(|(url, mime, _)| std::cmp::Reverse(media_score(url, mime.as_deref())))
    {
        Some((url, mime, len)) => (Some(url), mime, len),
        None => (None, None, None),
    }
}

fn media_score(url: &str, mime: Option<&str>) -> i32 {
    let lower_url = url.to_ascii_lowercase();
    if lower_url.starts_with("magnet:") {
        return 1000;
    }
    let mime_lower = mime.map(|m| m.to_ascii_lowercase()).unwrap_or_default();
    if mime_lower.contains("bittorrent") || lower_url.ends_with(".torrent") {
        return 900;
    }
    if mime_lower.starts_with("video/") {
        return 800;
    }
    if mime_lower.starts_with("audio/") {
        return 700;
    }
    if mime_lower == "application/octet-stream" {
        return 500;
    }
    if mime_lower.starts_with("application/") {
        return 400;
    }
    if mime_lower.starts_with("text/html") || mime_lower == "application/xhtml+xml" {
        return 50;
    }
    if mime_lower.starts_with("image/") {
        return 100;
    }
    if has_media_ext(&lower_url, MEDIA_EXTS) {
        return 600;
    }
    if has_media_ext(&lower_url, IMAGE_EXTS) {
        return 100;
    }
    300
}

const MEDIA_EXTS: &[&str] = &[
    ".mp4", ".mkv", ".webm", ".mov", ".avi", ".flv", ".m4v", ".ts", ".m4a", ".mp3", ".flac",
    ".ogg", ".opus", ".wav", ".aac", ".zip", ".rar", ".7z", ".tar", ".gz", ".pdf", ".epub",
];
const IMAGE_EXTS: &[&str] = &[
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg", ".avif",
];

fn has_media_ext(url: &str, exts: &[&str]) -> bool {
    let path = url.split('?').next().unwrap_or(url);
    let path = path.split('#').next().unwrap_or(path);
    exts.iter().any(|e| path.ends_with(e))
}

fn extract_media_urls(entry: &feed_rs::model::Entry, primary: Option<&str>) -> Vec<String> {
    use std::collections::BTreeSet;

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out: Vec<String> = Vec::new();
    let push = |url: String, out: &mut Vec<String>, seen: &mut BTreeSet<String>| {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return;
        }
        if Some(trimmed) == primary {
            return;
        }
        let lower = trimmed.to_ascii_lowercase();
        if !(lower.starts_with("http://")
            || lower.starts_with("https://")
            || lower.starts_with("magnet:"))
        {
            return;
        }
        if seen.insert(trimmed.to_string()) {
            out.push(trimmed.to_string());
        }
    };

    for link in &entry.links {
        if link.rel.as_deref() == Some("enclosure") {
            push(link.href.clone(), &mut out, &mut seen);
        }
    }
    for media in &entry.media {
        for content in &media.content {
            if let Some(ref u) = content.url {
                push(u.to_string(), &mut out, &mut seen);
            }
        }
    }

    let mut bodies: Vec<&str> = Vec::new();
    if let Some(ref s) = entry.summary {
        bodies.push(&s.content);
    }
    if let Some(ref c) = entry.content {
        if let Some(ref body) = c.body {
            bodies.push(body);
        }
    }
    for body in bodies {
        for url in scrape_media_from_html(body) {
            push(url, &mut out, &mut seen);
        }
    }

    out
}

fn scrape_media_from_html(html: &str) -> Vec<String> {
    use std::sync::OnceLock;
    static SRC_RE: OnceLock<regex::Regex> = OnceLock::new();
    static HREF_RE: OnceLock<regex::Regex> = OnceLock::new();
    static SRCSET_RE: OnceLock<regex::Regex> = OnceLock::new();

    let src_re = SRC_RE.get_or_init(|| {
        regex::Regex::new(
            r#"(?i)<(?:img|video|audio|source|embed|iframe)\b[^>]*?\bsrc\s*=\s*["']([^"']+)["']"#,
        )
        .expect("src regex")
    });
    let href_re = HREF_RE.get_or_init(|| {
        regex::Regex::new(r#"(?i)<a\b[^>]*?\bhref\s*=\s*["']([^"']+)["']"#).expect("href regex")
    });
    let srcset_re = SRCSET_RE.get_or_init(|| {
        regex::Regex::new(r#"(?i)\bsrcset\s*=\s*["']([^"']+)["']"#).expect("srcset regex")
    });

    let mut urls: Vec<String> = Vec::new();

    for cap in src_re.captures_iter(html) {
        if let Some(m) = cap.get(1) {
            urls.push(decode_html_entities(m.as_str()));
        }
    }
    for cap in srcset_re.captures_iter(html) {
        if let Some(m) = cap.get(1) {
            for entry in m.as_str().split(',') {
                if let Some(first) = entry.split_whitespace().next() {
                    urls.push(decode_html_entities(first));
                }
            }
        }
    }
    for cap in href_re.captures_iter(html) {
        if let Some(m) = cap.get(1) {
            let href = decode_html_entities(m.as_str());
            let lower = href.to_ascii_lowercase();
            if has_media_ext(&lower, MEDIA_EXTS) || has_media_ext(&lower, IMAGE_EXTS) {
                urls.push(href);
            }
        }
    }

    urls
}

fn decode_html_entities(s: &str) -> String {
    let s = s
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">");

    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'&' && i + 2 < bytes.len() && bytes[i + 1] == b'#' {
            let rest = &s[i + 2..];
            let (is_hex, digits_start) = if rest.starts_with('x') || rest.starts_with('X') {
                (true, 1)
            } else {
                (false, 0)
            };
            let digits: &str = {
                let src = &rest[digits_start..];
                match src.find(';') {
                    Some(end) => &src[..end],
                    None => "",
                }
            };
            if !digits.is_empty() {
                let parsed = if is_hex {
                    u32::from_str_radix(digits, 16).ok()
                } else {
                    digits.parse::<u32>().ok()
                };
                if let Some(ch) = parsed.and_then(char::from_u32) {
                    out.push(ch);
                    i += 2 + digits_start + digits.len() + 1;
                    continue;
                }
            }
        }
        out.push(s[i..].chars().next().unwrap());
        i += s[i..].chars().next().unwrap().len_utf8();
    }
    out
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Media,
    Article,
}

const MEDIA_PAYLOAD_THRESHOLD: i32 = 400;

pub fn classify_item_kind(item: &RssItem) -> ItemKind {
    match item.enclosure_url.as_deref() {
        Some(url) => {
            let mime = item.enclosure_type.as_deref();
            if media_score(url, mime) >= MEDIA_PAYLOAD_THRESHOLD {
                ItemKind::Media
            } else {
                ItemKind::Article
            }
        }
        None => ItemKind::Article,
    }
}

pub fn build_article_html(item: &RssItem) -> String {
    let title = if item.title.is_empty() {
        "Untitled".to_string()
    } else {
        html_escape(&item.title)
    };
    let body_src = if item.content.is_empty() {
        &item.description
    } else {
        &item.content
    };
    let body = if body_src.is_empty() {
        "<p><em>(No content provided by the feed.)</em></p>".to_string()
    } else {
        sanitize_article_html(body_src)
    };
    let base_tag = if is_safe_http_url(&item.link) {
        format!(r#"<base href="{}" />"#, html_escape(&item.link))
    } else {
        String::new()
    };
    let source_link = if is_safe_http_url(&item.link) {
        format!(
            r#"<p class="rss-source"><a href="{href}" target="_blank" rel="noopener">{href}</a></p>"#,
            href = html_escape(&item.link)
        )
    } else {
        String::new()
    };
    let csp = "default-src 'none'; \
               img-src http: https: data:; \
               media-src http: https:; \
               style-src 'unsafe-inline'; \
               font-src http: https: data:; \
               script-src 'none'; object-src 'none'; \
               frame-src 'none'; frame-ancestors 'none'; \
               base-uri 'self'; form-action 'none'";
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta http-equiv="Content-Security-Policy" content="{csp}" />
{base_tag}
<title>{title}</title>
<style>
  body {{ font: 16px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 760px; margin: 2rem auto; padding: 0 1rem; }}
  img, video {{ max-width: 100%; height: auto; }}
  pre, code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  pre {{ background: rgba(127, 127, 127, .1); padding: .75rem; overflow: auto; }}
  blockquote {{ border-left: 3px solid #ccc; padding-left: 1rem; color: #555; }}
  .rss-source {{ font-size: .85em; color: #888; margin-top: 2rem; word-break: break-all; }}
  @media (prefers-color-scheme: dark) {{
    body {{ background: #111; color: #ddd; }}
    a {{ color: #6cf; }}
    blockquote {{ color: #aaa; }}
  }}
</style>
</head>
<body>
<h1>{title}</h1>
{body}
{source_link}
</body>
</html>
"#
    )
}

pub fn article_filename(item: &RssItem) -> String {
    const MAX_STEM: usize = 80;
    let raw = if item.title.is_empty() {
        "article".to_string()
    } else {
        item.title.clone()
    };
    let mut stem = crate::engine::util::safe_filename(raw.trim(), "article");
    if stem.chars().count() > MAX_STEM {
        stem = stem.chars().take(MAX_STEM).collect();
    }
    let id_suffix: String = item.id.chars().take(8).collect();
    format!("{stem}__{id_suffix}.html")
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn is_safe_http_url(s: &str) -> bool {
    let lower = s.trim().to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn sanitize_article_html(html: &str) -> String {
    use std::sync::OnceLock;
    static BLOCK_RES: OnceLock<Vec<regex::Regex>> = OnceLock::new();
    static SELF_CLOSE_RE: OnceLock<regex::Regex> = OnceLock::new();
    static ON_ATTR_RE: OnceLock<regex::Regex> = OnceLock::new();
    static JS_HREF_RE: OnceLock<regex::Regex> = OnceLock::new();

    let block_res = BLOCK_RES.get_or_init(|| {
        const TAGS: &[&str] = &[
            "script", "style", "iframe", "object", "embed", "frame", "frameset", "noscript",
            "template",
        ];
        TAGS.iter()
            .map(|t| {
                regex::Regex::new(&format!(r"(?is)<{t}\b[^>]*>.*?</{t}\s*>"))
                    .expect("block tag regex")
            })
            .collect()
    });
    let self_close_re = SELF_CLOSE_RE.get_or_init(|| {
        regex::Regex::new(
            r"(?is)<(?:script|style|iframe|object|embed|frame|frameset|noscript|template|meta|link)\b[^>]*/?>",
        )
        .expect("self-close regex")
    });
    let on_attr_re = ON_ATTR_RE.get_or_init(|| {
        regex::Regex::new(r#"(?i)\son[a-z]+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+)"#)
            .expect("on-attr regex")
    });
    let js_href_re = JS_HREF_RE.get_or_init(|| {
        regex::Regex::new(
            r#"(?i)\b(?:href|src|xlink:href)\s*=\s*(?:"\s*(?:javascript|vbscript|data)\s*:[^"]*"|'\s*(?:javascript|vbscript|data)\s*:[^']*'|(?:javascript|vbscript|data):[^\s>]*)"#,
        )
        .expect("js-href regex")
    });

    let mut s = std::borrow::Cow::Borrowed(html);
    for re in block_res {
        s = std::borrow::Cow::Owned(re.replace_all(&s, "").into_owned());
    }
    let s = self_close_re.replace_all(&s, "").into_owned();
    let s = on_attr_re.replace_all(&s, "").into_owned();
    let s = js_href_re.replace_all(&s, "href=\"#\"").into_owned();
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::FileStorage;
    use tempfile::TempDir;

    struct RssTestCtx {
        _dir: TempDir,
        mgr: RssManager,
    }

    fn test_manager() -> RssTestCtx {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let mgr = RssManager::new(storage);
        RssTestCtx { _dir: dir, mgr }
    }

    fn sample_feed(id: &str, url: &str) -> RssFeed {
        RssFeed {
            id: id.into(),
            url: url.into(),
            title: "Test Feed".into(),
            site_link: "https://example.com".into(),
            description: "desc".into(),
            update_interval_secs: DEFAULT_UPDATE_INTERVAL_SECS,
            last_fetched_at: None,
            created_at: 1,
            is_active: true,
            error_count: 0,
            last_attempt_at: None,
            etag: None,
            last_modified: None,
        }
    }

    #[test]
    fn article_filename_strips_reserved_characters() {
        let item = sample_item("f", "abcdef123456", "a\"b/c:d ");
        assert_eq!(article_filename(&item), "a_b_c_d__abcdef12.html");
    }

    fn sample_item(feed_id: &str, item_id: &str, title: &str) -> RssItem {
        RssItem {
            id: item_id.into(),
            feed_id: feed_id.into(),
            title: title.into(),
            link: "https://example.com/item".into(),
            pub_date: Some(12345),
            description: "desc".into(),
            content: String::new(),
            enclosure_url: Some("https://example.com/file.torrent".into()),
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

    #[test]
    fn item_id_is_deterministic() {
        let a = item_id("hello");
        let b = item_id("hello");
        let c = item_id("world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn media_score_prefers_payload_over_image() {
        assert!(
            media_score("https://x/a.mp4", Some("video/mp4"))
                > media_score("https://x/a.jpg", Some("image/jpeg"))
        );
        assert!(
            media_score("https://x/a.torrent", Some("application/x-bittorrent"))
                > media_score("https://x/cover.jpg", Some("image/jpeg"))
        );
        assert!(
            media_score("magnet:?xt=urn:btih:abc", None)
                > media_score("https://x/a.mp4", Some("video/mp4"))
        );
        assert!(media_score("https://x/a.mkv", None) > media_score("https://x/a.png", None));
    }

    #[test]
    fn scrape_media_from_html_finds_imgs_and_videos() {
        let html = r#"<p>hi</p>
            <img src="https://cdn/x.jpg" />
            <video src='https://cdn/v.mp4'></video>
            <img srcset="https://cdn/a.png 1x, https://cdn/b.png 2x">
            <a href="https://cdn/file.zip">dl</a>
            <a href="https://cdn/page.html">no</a>"#;
        let urls = scrape_media_from_html(html);
        assert!(urls.contains(&"https://cdn/x.jpg".to_string()));
        assert!(urls.contains(&"https://cdn/v.mp4".to_string()));
        assert!(urls.contains(&"https://cdn/a.png".to_string()));
        assert!(urls.contains(&"https://cdn/b.png".to_string()));
        assert!(urls.contains(&"https://cdn/file.zip".to_string()));
        assert!(!urls.iter().any(|u| u.ends_with("page.html")));
    }

    #[test]
    fn scrape_media_decodes_amp_entities() {
        let html = r#"<img src="https://cdn/x.jpg?a=1&amp;b=2" />"#;
        let urls = scrape_media_from_html(html);
        assert_eq!(urls, vec!["https://cdn/x.jpg?a=1&b=2".to_string()]);
    }

    #[test]
    fn get_feeds_returns_populated() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
        }
        let feeds = rt.block_on(mgr.get_feeds());
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].id, "f1");
    }

    #[test]
    fn get_items_returns_feed_items() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            s.items
                .insert("f1".into(), vec![sample_item("f1", "i1", "Item 1")]);
        }
        let items = rt.block_on(mgr.get_items("f1"));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "Item 1");
    }

    #[test]
    fn get_items_missing_feed_returns_empty() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let items = rt.block_on(mgr.get_items("none"));
        assert!(items.is_empty());
    }

    #[test]
    fn remove_feed_deletes_items_and_rules() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            s.items
                .insert("f1".into(), vec![sample_item("f1", "i1", "Item 1")]);
            s.rules.push(RssRule {
                id: "r1".into(),
                name: "Rule".into(),
                is_active: true,
                auto_download: true,
                feed_ids: vec!["f1".into()],
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
            });
        }
        rt.block_on(mgr.remove_feed("f1")).unwrap();
        let feeds = rt.block_on(mgr.get_feeds());
        assert!(feeds.is_empty());
        let items = rt.block_on(mgr.get_items("f1"));
        assert!(items.is_empty());
        let rules = rt.block_on(mgr.get_rules());
        assert!(rules.is_empty());
    }

    #[test]
    fn update_feed_settings_changes_interval_and_active() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
        }
        rt.block_on(mgr.update_feed_settings("f1", Some(60), Some(false)))
            .unwrap();
        let feeds = rt.block_on(mgr.get_feeds());
        assert_eq!(feeds[0].update_interval_secs, 60);
        assert!(!feeds[0].is_active);
    }

    #[test]
    fn update_feed_settings_not_found() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt
            .block_on(mgr.update_feed_settings("none", Some(60), None))
            .unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn mark_item_downloaded_updates_state() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            s.items
                .insert("f1".into(), vec![sample_item("f1", "i1", "Item 1")]);
        }
        let file = ctx._dir.path().join("file.txt");
        std::fs::write(&file, "x").unwrap();
        let path = file.to_string_lossy().to_string();
        rt.block_on(mgr.mark_item_downloaded("f1", "i1", Some(path.clone())))
            .unwrap();
        let items = rt.block_on(mgr.get_items("f1"));
        assert!(items[0].is_downloaded);
        assert!(items[0].is_read);
        assert_eq!(items[0].download_path, Some(path));
    }

    #[test]
    fn get_item_download_url_prefers_enclosure() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            let mut item = sample_item("f1", "i1", "Item 1");
            item.enclosure_url = Some("https://enc.example.com/file.torrent".into());
            item.link = "https://link.example.com/".into();
            s.items.insert("f1".into(), vec![item]);
        }
        let url = rt.block_on(mgr.get_item_download_url("f1", "i1")).unwrap();
        assert_eq!(url, "https://enc.example.com/file.torrent");
    }

    #[test]
    fn get_item_download_url_falls_back_to_link() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            let mut item = sample_item("f1", "i1", "Item 1");
            item.enclosure_url = None;
            item.link = "https://link.example.com/".into();
            s.items.insert("f1".into(), vec![item]);
        }
        let url = rt.block_on(mgr.get_item_download_url("f1", "i1")).unwrap();
        assert_eq!(url, "https://link.example.com/");
    }

    #[test]
    fn get_item_download_url_no_url() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            let mut item = sample_item("f1", "i1", "Item 1");
            item.enclosure_url = None;
            item.link = String::new();
            s.items.insert("f1".into(), vec![item]);
        }
        let err = rt
            .block_on(mgr.get_item_download_url("f1", "i1"))
            .unwrap_err();
        assert!(err.contains("No downloadable URL"));
    }

    #[test]
    fn get_item_download_urls_includes_inline_media() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            let mut item = sample_item("f1", "i1", "Item 1");
            item.enclosure_url = Some("https://enc/cover.jpg".into());
            item.media_urls = vec![
                "https://cdn/img1.jpg".into(),
                "https://cdn/video.mp4".into(),
                "https://enc/cover.jpg".into(),
            ];
            item.link = "https://link.example.com/article".into();
            s.items.insert("f1".into(), vec![item]);
        }
        let urls = rt.block_on(mgr.get_item_download_urls("f1", "i1")).unwrap();
        assert_eq!(
            urls,
            vec![
                "https://enc/cover.jpg".to_string(),
                "https://cdn/img1.jpg".to_string(),
                "https://cdn/video.mp4".to_string(),
            ]
        );
    }

    #[test]
    fn get_item_download_urls_link_fallback_only_when_empty() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            let mut item = sample_item("f1", "i1", "Item 1");
            item.enclosure_url = None;
            item.media_urls = Vec::new();
            item.link = "https://link.example.com/".into();
            s.items.insert("f1".into(), vec![item]);
        }
        let urls = rt.block_on(mgr.get_item_download_urls("f1", "i1")).unwrap();
        assert_eq!(urls, vec!["https://link.example.com/".to_string()]);
    }

    fn empty_v2_rule(name: &str) -> RssRule {
        RssRule {
            id: String::new(),
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

    #[test]
    fn add_rule_generates_id_and_validates() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut rule = empty_v2_rule("Global");
        rule.title_must = vec![Pattern {
            value: r".*".into(),
            kind: PatternKind::Regex,
            case_sensitive: false,
        }];
        let created = rt.block_on(mgr.add_rule(rule)).unwrap();
        assert!(!created.id.is_empty());
        let rules = rt.block_on(mgr.get_rules());
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn add_rule_rejects_invalid_regex() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut rule = empty_v2_rule("Bad");
        rule.title_must = vec![Pattern {
            value: r"[bad".into(),
            kind: PatternKind::Regex,
            case_sensitive: false,
        }];
        let err = rt.block_on(mgr.add_rule(rule)).unwrap_err();
        assert!(err.contains("Invalid regex"));
    }

    #[test]
    fn remove_rule_deletes() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut rule = empty_v2_rule("Rule");
        rule.title_must = vec![Pattern {
            value: "test".into(),
            kind: PatternKind::Contains,
            case_sensitive: false,
        }];
        let created = rt.block_on(mgr.add_rule(rule)).unwrap();
        rt.block_on(mgr.remove_rule(&created.id)).unwrap();
        let rules = rt.block_on(mgr.get_rules());
        assert!(rules.is_empty());
    }

    fn item_for_match(feed_id: &str, title: &str) -> RssItem {
        let mut it = sample_item(feed_id, "i1", title);
        it.parsed_meta = Some(parser::parse_title(title));
        it
    }

    #[test]
    fn best_matching_rule_substring() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut rule = empty_v2_rule("Match");
        rule.feed_ids = vec!["f1".into()];
        rule.title_must = vec![Pattern {
            value: "hello".into(),
            kind: PatternKind::Contains,
            case_sensitive: false,
        }];
        rt.block_on(mgr.add_rule(rule)).unwrap();
        let item = item_for_match("f1", "Hello World");
        let parsed = item.parsed_meta.clone().unwrap_or_default();
        assert!(rt
            .block_on(mgr.best_matching_rule(&item, &parsed))
            .is_some());
    }

    #[test]
    fn best_matching_rule_respects_feed_scope() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut rule = empty_v2_rule("Scope");
        rule.feed_ids = vec!["f1".into()];
        rule.title_must = vec![Pattern {
            value: "hello".into(),
            kind: PatternKind::Contains,
            case_sensitive: false,
        }];
        rt.block_on(mgr.add_rule(rule)).unwrap();
        let item = item_for_match("f2", "Hello World");
        let parsed = item.parsed_meta.clone().unwrap_or_default();
        assert!(rt
            .block_on(mgr.best_matching_rule(&item, &parsed))
            .is_none());
    }

    #[test]
    fn best_matching_rule_skips_inactive() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut rule = empty_v2_rule("Inactive");
        rule.is_active = false;
        rule.title_must = vec![Pattern {
            value: "hello".into(),
            kind: PatternKind::Contains,
            case_sensitive: false,
        }];
        rt.block_on(mgr.add_rule(rule)).unwrap();
        let item = item_for_match("f1", "Hello World");
        let parsed = item.parsed_meta.clone().unwrap_or_default();
        assert!(rt
            .block_on(mgr.best_matching_rule(&item, &parsed))
            .is_none());
    }

    #[test]
    fn delete_items_removes_selected() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            s.items.insert(
                "f1".into(),
                vec![
                    sample_item("f1", "i1", "A"),
                    sample_item("f1", "i2", "B"),
                    sample_item("f1", "i3", "C"),
                ],
            );
        }
        rt.block_on(mgr.delete_items(vec![("f1".into(), vec!["i1".into(), "i3".into()])]))
            .unwrap();
        let items = rt.block_on(mgr.get_items("f1"));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "i2");
    }

    #[test]
    fn extract_items_keeps_full_content_alongside_summary() {
        let xml = br#"<?xml version="1.0"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel><title>t</title>
<item>
  <title>Post</title>
  <link>https://example.com/post/</link>
  <description>Short summary</description>
  <content:encoded>&lt;p&gt;Full body here&lt;/p&gt;</content:encoded>
</item>
</channel></rss>"#;
        let parsed = feed_rs::parser::parse(&xml[..]).unwrap();
        let items = extract_items("f1", &parsed.entries);
        assert_eq!(items[0].description, "Short summary");
        assert!(items[0].content.contains("Full body here"));
        let html = build_article_html(&items[0]);
        assert!(html.contains("Full body here"));
    }

    #[test]
    fn get_items_clears_downloaded_when_file_missing() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let file = ctx._dir.path().join("article.html");
        std::fs::write(&file, "x").unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            let mut item = sample_item("f1", "i1", "Item 1");
            item.is_downloaded = true;
            item.download_path = Some(file.to_string_lossy().to_string());
            s.items.insert("f1".into(), vec![item]);
        }
        let items = rt.block_on(mgr.get_items("f1"));
        assert!(items[0].is_downloaded);
        std::fs::remove_file(&file).unwrap();
        let items = rt.block_on(mgr.get_items("f1"));
        assert!(!items[0].is_downloaded);
        assert!(items[0].download_path.is_none());
    }

    #[test]
    fn load_and_save_round_trip() {
        let dir = TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let mgr = RssManager::new(storage.clone());
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let mut s = mgr.store.blocking_lock();
            s.feeds.push(sample_feed("f1", "https://a.com"));
            s.items
                .insert("f1".into(), vec![sample_item("f1", "i1", "Item")]);
        }
        rt.block_on(mgr.save()).unwrap();

        let mgr2 = RssManager::new(storage);
        mgr2.load().unwrap();
        let feeds = rt.block_on(mgr2.get_feeds());
        assert_eq!(feeds.len(), 1);
        let items = rt.block_on(mgr2.get_items("f1"));
        assert_eq!(items.len(), 1);
    }

    fn rss_xml(count: usize) -> Vec<u8> {
        let mut xml =
            String::from("<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>t</title>");
        for n in 0..count {
            xml.push_str(&format!(
                "<item><title>Item {n}</title><guid>g{n}</guid><link>https://e.com/{n}</link></item>"
            ));
        }
        xml.push_str("</channel></rss>");
        xml.into_bytes()
    }

    fn parsed(count: usize) -> Result<Fetched, FetchFailure> {
        let feed = feed_rs::parser::parse(&rss_xml(count)[..]).unwrap();
        Ok(Fetched::Parsed {
            feed: Box::new(feed),
            etag: Some("\"abc\"".into()),
            last_modified: None,
        })
    }

    #[tokio::test]
    async fn apply_fetch_caps_window_and_does_not_refire_old_entries() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        mgr.store
            .lock()
            .await
            .feeds
            .push(sample_feed("f1", "https://a.com"));
        let fresh = mgr
            .apply_fetch("f1", parsed(MAX_ITEMS_PER_FEED + 100))
            .await
            .unwrap();
        assert_eq!(fresh.len(), MAX_ITEMS_PER_FEED);
        let again = mgr
            .apply_fetch("f1", parsed(MAX_ITEMS_PER_FEED + 100))
            .await
            .unwrap();
        assert!(again.is_empty());
        let s = mgr.store.lock().await;
        assert_eq!(s.items["f1"].len(), MAX_ITEMS_PER_FEED);
        assert_eq!(s.feeds[0].etag.as_deref(), Some("\"abc\""));
    }

    #[tokio::test]
    async fn apply_fetch_not_modified_resets_errors() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        {
            let mut s = mgr.store.lock().await;
            let mut f = sample_feed("f1", "https://a.com");
            f.error_count = 3;
            s.feeds.push(f);
        }
        let fresh = mgr
            .apply_fetch("f1", Ok(Fetched::NotModified))
            .await
            .unwrap();
        assert!(fresh.is_empty());
        let s = mgr.store.lock().await;
        assert_eq!(s.feeds[0].error_count, 0);
        assert!(s.feeds[0].last_attempt_at.is_some());
    }

    #[tokio::test]
    async fn transient_errors_never_disable_feed() {
        let ctx = test_manager();
        let mgr = &ctx.mgr;
        mgr.store
            .lock()
            .await
            .feeds
            .push(sample_feed("f1", "https://a.com"));
        for _ in 0..(MAX_CONSECUTIVE_ERRORS + 2) {
            let r = mgr
                .apply_fetch(
                    "f1",
                    Err(FetchFailure {
                        message: "down".into(),
                        transient: true,
                    }),
                )
                .await;
            assert!(r.is_err());
        }
        assert!(mgr.store.lock().await.feeds[0].is_active);
        for _ in 0..MAX_CONSECUTIVE_ERRORS {
            let _ = mgr
                .apply_fetch(
                    "f1",
                    Err(FetchFailure {
                        message: "gone".into(),
                        transient: false,
                    }),
                )
                .await;
        }
        assert!(!mgr.store.lock().await.feeds[0].is_active);
    }

    #[test]
    fn failing_feeds_back_off_from_their_own_interval() {
        assert_eq!(feed_retry_delay(300, 0), 300);
        assert_eq!(feed_retry_delay(300, 1), 600);
        assert_eq!(feed_retry_delay(300, 3), 2400);
        assert_eq!(feed_retry_delay(300, 30), MAX_BACKOFF_SECS);
        let mut f = sample_feed("f1", "https://a.com");
        f.update_interval_secs = 60;
        f.error_count = 2;
        f.last_attempt_at = Some(1000);
        assert!(!feed_is_due(&f, 1000 + 239));
        assert!(feed_is_due(&f, 1000 + 240));
    }

    #[test]
    fn rollback_restores_previous_episode_record() {
        let key = EpisodeKey {
            series: "show".into(),
            season: Some(1),
            episode: 2,
            absolute: false,
        };
        let mut s = RssStore::default();
        let prev = EpisodeRecord {
            item_id: "old".into(),
            feed_id: "f1".into(),
            score: 1,
            downloaded_at: 5,
            file_path: Some("/x".into()),
            rule_id: None,
        };
        s.episode_history.insert(key.to_storage_key(), prev.clone());
        record_episode(&mut s, &key, "f1", "new", "r1", 9, 10, None);
        let p = PendingDownload {
            feed_id: "f1".into(),
            item_id: "new".into(),
            title: "t".into(),
            rule_id: "r1".into(),
            rule_name: "n".into(),
            key: Some(key.clone()),
            score: 9,
            queued_at: 10,
            prev_episode: Some(prev),
        };
        rollback_episode(&mut s, &p);
        assert_eq!(s.episode_history[&key.to_storage_key()].item_id, "old");
    }
}
