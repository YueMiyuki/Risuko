use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuleMatch {
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub min_size: Option<u64>,
    #[serde(default)]
    pub max_size: Option<u64>,
    #[serde(default)]
    pub task_kinds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadRule {
    pub id: String,
    pub label: String,
    pub sink_id: String,
    #[serde(default)]
    pub r#match: RuleMatch,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

pub struct RuleInput<'a> {
    pub file_path: &'a Path,
    pub size: u64,
    pub category: Option<&'a str>,
    pub task_kind: &'a str,
}

impl UploadRule {
    pub fn matches(&self, input: &RuleInput<'_>) -> bool {
        if !self.enabled {
            return false;
        }
        let m = &self.r#match;

        if let Some(min) = m.min_size {
            if input.size < min {
                return false;
            }
        }
        if let Some(max) = m.max_size {
            if input.size > max {
                return false;
            }
        }

        if !m.task_kinds.is_empty()
            && !m
                .task_kinds
                .iter()
                .any(|k| k.eq_ignore_ascii_case(input.task_kind))
        {
            return false;
        }

        if !m.categories.is_empty() {
            match input.category {
                Some(c) if m.categories.iter().any(|x| x.eq_ignore_ascii_case(c)) => {}
                _ => return false,
            }
        }

        if !m.extensions.is_empty() {
            let ext = input
                .file_path
                .extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_ascii_lowercase());
            match ext {
                Some(e) if m.extensions.iter().any(|x| x.eq_ignore_ascii_case(&e)) => {}
                _ => return false,
            }
        }

        true
    }
}

pub fn select_rule_sink<'a>(rules: &'a [UploadRule], input: &RuleInput<'_>) -> Option<&'a str> {
    rules
        .iter()
        .find(|r| r.matches(input))
        .map(|r| r.sink_id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn rule(id: &str, sink: &str, m: RuleMatch) -> UploadRule {
        UploadRule {
            id: id.into(),
            label: id.into(),
            sink_id: sink.into(),
            r#match: m,
            enabled: true,
        }
    }

    #[test]
    fn empty_match_matches_anything() {
        let r = rule("r1", "s1", RuleMatch::default());
        let path = PathBuf::from("foo.mp4");
        let inp = RuleInput {
            file_path: &path,
            size: 100,
            category: None,
            task_kind: "Http",
        };
        assert!(r.matches(&inp));
    }

    #[test]
    fn extension_filter() {
        let r = rule(
            "r1",
            "s1",
            RuleMatch {
                extensions: vec!["mp4".into()],
                ..Default::default()
            },
        );
        let mp4 = PathBuf::from("a.mp4");
        let mkv = PathBuf::from("a.mkv");
        let inp_mp4 = RuleInput {
            file_path: &mp4,
            size: 1,
            category: None,
            task_kind: "Http",
        };
        let inp_mkv = RuleInput {
            file_path: &mkv,
            size: 1,
            category: None,
            task_kind: "Http",
        };
        assert!(r.matches(&inp_mp4));
        assert!(!r.matches(&inp_mkv));
    }

    #[test]
    fn first_match_wins() {
        let rules = vec![
            rule(
                "video",
                "s_video",
                RuleMatch {
                    extensions: vec!["mp4".into()],
                    ..Default::default()
                },
            ),
            rule("default", "s_default", RuleMatch::default()),
        ];
        let path = PathBuf::from("clip.mp4");
        let inp = RuleInput {
            file_path: &path,
            size: 100,
            category: None,
            task_kind: "Http",
        };
        assert_eq!(select_rule_sink(&rules, &inp), Some("s_video"));
    }

    #[test]
    fn disabled_rule_skipped() {
        let mut r = rule(
            "r1",
            "s1",
            RuleMatch {
                extensions: vec!["mp4".into()],
                ..Default::default()
            },
        );
        r.enabled = false;
        let path = PathBuf::from("a.mp4");
        let inp = RuleInput {
            file_path: &path,
            size: 1,
            category: None,
            task_kind: "Http",
        };
        assert!(!r.matches(&inp));
    }

    #[test]
    fn size_bounds() {
        let r = rule(
            "r1",
            "s1",
            RuleMatch {
                min_size: Some(10),
                max_size: Some(20),
                ..Default::default()
            },
        );
        let p = PathBuf::from("x");
        for (size, expected) in [
            (5u64, false),
            (10, true),
            (15, true),
            (20, true),
            (21, false),
        ] {
            let inp = RuleInput {
                file_path: &p,
                size,
                category: None,
                task_kind: "Http",
            };
            assert_eq!(r.matches(&inp), expected, "size {size}");
        }
    }
}
