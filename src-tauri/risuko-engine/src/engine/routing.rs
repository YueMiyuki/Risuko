use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRoutingRule {
    pub id: String,
    pub label: String,
    pub pattern: String,
    pub dir: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingDecision {
    pub tag: Option<String>,
    pub dir: String,
}

pub fn resolve_routing(
    rules: &[TaskRoutingRule],
    filename: &str,
    default_dir: &str,
    file_category_dirs: &std::collections::HashMap<String, String>,
) -> RoutingDecision {
    for rule in rules {
        if !rule.enabled {
            continue;
        }
        if glob_matches(&rule.pattern, filename) && !rule.dir.trim().is_empty() {
            return RoutingDecision {
                tag: Some(rule.label.clone()),
                dir: rule.dir.trim().to_string(),
            };
        }
    }

    if let Some(category) = super::upload::resolve_category(filename) {
        if let Some(cat_dir) = file_category_dirs.get(&category) {
            if !cat_dir.trim().is_empty() {
                return RoutingDecision {
                    tag: Some(category),
                    dir: cat_dir.trim().to_string(),
                };
            }
        }
    }

    RoutingDecision {
        tag: None,
        dir: default_dir.to_string(),
    }
}

pub(crate) fn strip_part_suffix(filename: &str) -> &str {
    filename
        .strip_suffix(".part")
        .filter(|stem| !stem.is_empty())
        .unwrap_or(filename)
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return false;
    }

    let normalized_pattern = pattern.to_ascii_lowercase();
    let normalized_text = text.to_ascii_lowercase();

    let pat = match glob::Pattern::new(&normalized_pattern) {
        Ok(p) => p,
        Err(_) => return false,
    };
    pat.matches(&normalized_text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn rule(id: &str, label: &str, pattern: &str, dir: &str, enabled: bool) -> TaskRoutingRule {
        TaskRoutingRule {
            id: id.into(),
            label: label.into(),
            pattern: pattern.into(),
            dir: dir.into(),
            enabled,
        }
    }

    #[test]
    fn custom_rule_wins_over_default() {
        let rules = vec![rule("r1", "Movies", "*.mkv", "/Movies", true)];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "movie.mkv", "/Downloads", &cats);
        assert_eq!(dec.tag, Some("Movies".into()));
        assert_eq!(dec.dir, "/Movies");
    }

    #[test]
    fn category_fallback_when_no_rule_matches() {
        let rules: Vec<TaskRoutingRule> = vec![];
        let mut cats = HashMap::new();
        cats.insert("music".into(), "/Music".into());
        let dec = resolve_routing(&rules, "song.mp3", "/Downloads", &cats);
        assert_eq!(dec.tag, Some("music".into()));
        assert_eq!(dec.dir, "/Music");
    }

    #[test]
    fn default_when_nothing_matches() {
        let rules: Vec<TaskRoutingRule> = vec![];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "unknown.xyz", "/Downloads", &cats);
        assert_eq!(dec.tag, None);
        assert_eq!(dec.dir, "/Downloads");
    }

    #[test]
    fn first_match_wins() {
        let rules = vec![
            rule("r1", "Movies", "*.mkv", "/Movies", true),
            rule("r2", "Video", "*.mkv", "/Video", true),
        ];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "movie.mkv", "/Downloads", &cats);
        assert_eq!(dec.tag, Some("Movies".into()));
        assert_eq!(dec.dir, "/Movies");
    }

    #[test]
    fn part_suffix_is_stripped_only_on_request() {
        let rules = vec![rule("r1", "Movies", "*.mkv", "/Movies", true)];
        let mut cats = HashMap::new();
        cats.insert("music".into(), "/Music".into());
        let dec = resolve_routing(
            &rules,
            strip_part_suffix("movie.mkv.part"),
            "/Downloads",
            &cats,
        );
        assert_eq!(dec.dir, "/Movies");
        let dec = resolve_routing(&[], strip_part_suffix("song.mp3.part"), "/Downloads", &cats);
        assert_eq!(dec.dir, "/Music");
        assert_eq!(strip_part_suffix(".part"), ".part");
        let rules = vec![rule("r2", "Parts", "*.part", "/Parts", true)];
        let dec = resolve_routing(&rules, "movie.mkv.part", "/Downloads", &cats);
        assert_eq!(dec.dir, "/Parts");
    }

    #[test]
    fn disabled_rule_skipped() {
        let rules = vec![rule("r1", "Movies", "*.mkv", "/Movies", false)];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "movie.mkv", "/Downloads", &cats);
        assert_eq!(dec.tag, None);
        assert_eq!(dec.dir, "/Downloads");
    }

    #[test]
    fn custom_rule_wins_over_category() {
        let rules = vec![rule("r1", "ISO", "*.iso", "/ISO", true)];
        let mut cats = HashMap::new();
        cats.insert("compressed".into(), "/Compressed".into());
        let dec = resolve_routing(&rules, "image.iso", "/Downloads", &cats);
        assert_eq!(dec.tag, Some("ISO".into()));
        assert_eq!(dec.dir, "/ISO");
    }

    #[test]
    fn glob_prefix_match() {
        let rules = vec![rule("r1", "Movie", "*movie*", "/Movies", true)];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "my-movie-file.txt", "/Downloads", &cats);
        assert_eq!(dec.tag, Some("Movie".into()));
        assert_eq!(dec.dir, "/Movies");
    }

    #[test]
    fn invalid_glob_pattern_is_skipped() {
        let rules = vec![rule("r1", "Bad", "[invalid", "/Bad", true)];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "file.txt", "/Downloads", &cats);
        assert_eq!(dec.tag, None);
        assert_eq!(dec.dir, "/Downloads");
    }

    #[test]
    fn glob_match_is_case_insensitive() {
        let rules = vec![rule("r1", "Movie", "*.mkv", "/Movies", true)];
        let cats = HashMap::new();
        let dec = resolve_routing(&rules, "My.Video.MKV", "/Downloads", &cats);
        assert_eq!(dec.tag, Some("Movie".into()));
        assert_eq!(dec.dir, "/Movies");
    }
}
