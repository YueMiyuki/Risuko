use std::sync::OnceLock;

use regex::Regex;

use super::types::ParsedMeta;

struct ParserRegexes {
    season_episode: Regex,
    season_episode_x: Regex,
    episode_only: Regex,
    anime_loose: Regex,
    year: Regex,
    resolution: Regex,
    codec: Regex,
    source: Regex,
    hdr: Regex,
    container: Regex,
    group: Regex,
    seeders: Regex,
    bracket_group: Regex,
    language: Regex,
}

fn regexes() -> &'static ParserRegexes {
    static R: OnceLock<ParserRegexes> = OnceLock::new();
    R.get_or_init(|| ParserRegexes {
        season_episode: Regex::new(r"(?i)\bS(\d{1,2})E(\d{1,3})(?:[-_ ]?E?\d{1,3})?\b").unwrap(),
        season_episode_x: Regex::new(r"(?i)\b(\d{1,2})x(\d{1,3})\b").unwrap(),
        episode_only: Regex::new(r"(?i)\b(?:EP|Episode|E)[ ._-]?(\d{1,3})\b").unwrap(),
        anime_loose: Regex::new(r"(?:^|[\s\-\[\]])(\d{1,3})(?:v\d)?(?:[\s\-\[\]]|$)").unwrap(),
        year: Regex::new(r"\b(19\d{2}|20\d{2})\b").unwrap(),
        resolution: Regex::new(r"(?i)\b(2160p|1440p|1080p|720p|480p|4k|uhd)\b").unwrap(),
        codec: Regex::new(r"(?i)\b(x265|h\.?265|hevc|x264|h\.?264|avc|av1|vp9)\b").unwrap(),
        source: Regex::new(
            r"(?i)\b(BluRay|Blu-Ray|WEB[-_ ]?DL|WEBRip|HDTV|DVDRip|BDRip|BRRip|REMUX|HDRip|CAM|TS)\b",
        )
        .unwrap(),
        hdr: Regex::new(r"(?i)\b(HDR10\+|HDR10|HDR|DV|Dolby[._ ]?Vision)\b").unwrap(),
        container: Regex::new(r"(?i)\.(mkv|mp4|avi|m4v|mov|webm|flv|ts)\b").unwrap(),
        group: Regex::new(r"-([A-Za-z0-9_]+?)(?:\.[A-Za-z0-9]{2,4})?\s*$").unwrap(),
        seeders: Regex::new(r"(?i)\[(?:S[: =]?)?(\d{1,5})\s*[Ss](?:eeders?)?(?:\s*[/|]\s*\d+\s*[Ll])?\]").unwrap(),
        bracket_group: Regex::new(r"^\[([^\]]+)\]").unwrap(),
        language: Regex::new(
            r"(?i)\b(English|Japanese|Korean|Chinese|Spanish|French|German|Italian|Russian|Portuguese|JPN|ENG|CHS|CHT|SUB|DUB|MULTi)\b",
        )
        .unwrap(),
    })
}

pub fn normalize_series(s: &str) -> String {
    let lowered = s.to_lowercase();
    let mut out = String::with_capacity(lowered.len());
    let mut prev_space = false;
    for c in lowered.chars() {
        if c.is_alphanumeric() {
            out.push(c);
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }
    out.trim().to_string()
}

pub fn parse_title(raw: &str) -> ParsedMeta {
    let r = regexes();
    let mut meta = ParsedMeta::default();
    let title = raw.trim();

    let mut quality_tags: Vec<String> = Vec::new();

    if let Some(c) = r.resolution.captures(title) {
        let v = c.get(1).unwrap().as_str().to_lowercase();
        let canon = match v.as_str() {
            "4k" | "uhd" => "2160p".to_string(),
            other => other.to_string(),
        };
        quality_tags.push(canon);
    }
    if let Some(c) = r.codec.captures(title) {
        let raw_codec = c.get(1).unwrap().as_str().to_lowercase();
        let canon = match raw_codec.replace('.', "").as_str() {
            "h265" | "hevc" | "x265" => "x265".to_string(),
            "h264" | "avc" | "x264" => "x264".to_string(),
            other => other.to_string(),
        };
        meta.codec = Some(canon.clone());
        quality_tags.push(canon);
    }
    if let Some(c) = r.source.captures(title) {
        let raw_src = c.get(1).unwrap().as_str();
        let canon = canonical_source(raw_src);
        meta.source = Some(canon.clone());
        quality_tags.push(canon);
    }
    if let Some(c) = r.hdr.captures(title) {
        let v = c.get(1).unwrap().as_str().to_lowercase();
        let canon = if v.contains("dolby") || v == "dv" {
            "DV".to_string()
        } else if v == "hdr10+" {
            "HDR10+".to_string()
        } else if v == "hdr10" {
            "HDR10".to_string()
        } else {
            "HDR".to_string()
        };
        quality_tags.push(canon);
    }
    if let Some(c) = r.container.captures(title) {
        meta.container = Some(c.get(1).unwrap().as_str().to_lowercase());
    }
    if let Some(c) = r.language.captures(title) {
        meta.language = Some(c.get(1).unwrap().as_str().to_string());
    }
    if let Some(c) = r.seeders.captures(title) {
        if let Ok(n) = c.get(1).unwrap().as_str().parse::<u32>() {
            meta.seeders = Some(n);
        }
    }

    meta.quality_tags = quality_tags;

    if let Some(c) = r.year.captures(title) {
        if let Ok(y) = c.get(1).unwrap().as_str().parse::<u32>() {
            meta.year = Some(y);
        }
    }

    if let Some(c) = r.bracket_group.captures(title) {
        let candidate = c.get(1).unwrap().as_str().trim();
        if !candidate.chars().all(|ch| ch.is_ascii_digit()) {
            meta.group = Some(candidate.to_string());
        }
    }
    if meta.group.is_none() {
        if let Some(c) = r.group.captures(title) {
            let candidate = c.get(1).unwrap().as_str();
            let lower = candidate.to_lowercase();
            let is_known_token = matches!(
                lower.as_str(),
                "1080p"
                    | "720p"
                    | "2160p"
                    | "480p"
                    | "1440p"
                    | "x264"
                    | "x265"
                    | "hevc"
                    | "av1"
                    | "dl"
                    | "rip"
                    | "ray"
            );
            let cap0 = c.get(0).unwrap();
            let preceding_is_source_prefix = title[..cap0.start()]
                .rsplit(|c: char| !c.is_ascii_alphanumeric())
                .next()
                .map(|tok| {
                    matches!(
                        tok.to_ascii_uppercase().as_str(),
                        "WEB" | "BLU" | "BD" | "HD" | "DVD" | "BR" | "WEBR"
                    )
                })
                .unwrap_or(false);
            if !is_known_token
                && !preceding_is_source_prefix
                && candidate.chars().any(|c| c.is_alphabetic())
            {
                meta.group = Some(candidate.to_string());
            }
        }
    }

    let mut ep_match_start: Option<usize> = None;
    if let Some(c) = r.season_episode.captures(title) {
        meta.season = c.get(1).and_then(|m| m.as_str().parse().ok());
        meta.episode = c.get(2).and_then(|m| m.as_str().parse().ok());
        ep_match_start = c.get(0).map(|m| m.start());
    } else if let Some(c) = r.season_episode_x.captures(title) {
        meta.season = c.get(1).and_then(|m| m.as_str().parse().ok());
        meta.episode = c.get(2).and_then(|m| m.as_str().parse().ok());
        ep_match_start = c.get(0).map(|m| m.start());
    } else if let Some(c) = r.episode_only.captures(title) {
        let n: Option<u32> = c.get(1).and_then(|m| m.as_str().parse().ok());
        meta.absolute_episode = n;
        meta.episode = n;
        ep_match_start = c.get(0).map(|m| m.start());
    } else if let Some(c) = r.anime_loose.captures(title) {
        if meta.group.is_some() {
            let n: Option<u32> = c.get(1).and_then(|m| m.as_str().parse().ok());
            if let Some(n) = n {
                if (1..=999).contains(&n) {
                    meta.absolute_episode = Some(n);
                    meta.episode = Some(n);
                    ep_match_start = c.get(0).map(|m| m.start());
                }
            }
        }
    }

    let series_end = ep_match_start
        .or_else(|| {
            [&r.resolution, &r.source, &r.year]
                .iter()
                .filter_map(|re| re.find(title).map(|m| m.start()))
                .min()
        })
        .unwrap_or(title.len());

    let mut series_slice = &title[..series_end];
    while let Some(end) = series_slice.find(']') {
        if series_slice.trim_start().starts_with('[') {
            series_slice = &series_slice[end + 1..];
        } else {
            break;
        }
    }
    let cleaned = series_slice
        .trim()
        .trim_end_matches(&['.', '-', '_', ' '][..])
        .trim();
    if !cleaned.is_empty() {
        meta.series = Some(normalize_display_series(cleaned));
    }

    meta
}

fn canonical_source(s: &str) -> String {
    let lower = s.to_lowercase().replace([' ', '_'], "-");
    match lower.as_str() {
        "blu-ray" | "bluray" => "BluRay".into(),
        "web-dl" | "webdl" => "WEB-DL".into(),
        "webrip" => "WEBRip".into(),
        "hdtv" => "HDTV".into(),
        "dvdrip" => "DVDRip".into(),
        "bdrip" => "BDRip".into(),
        "brrip" => "BRRip".into(),
        "remux" => "REMUX".into(),
        "hdrip" => "HDRip".into(),
        other => other.to_uppercase(),
    }
}

fn normalize_display_series(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for c in s.chars() {
        let mapped = if c == '.' || c == '_' { ' ' } else { c };
        if mapped.is_whitespace() {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(mapped);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_release() {
        let m = parse_title("Show.Name.S01E02.1080p.WEB-DL.x265-GROUP.mkv");
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(2));
        assert_eq!(m.codec.as_deref(), Some("x265"));
        assert_eq!(m.source.as_deref(), Some("WEB-DL"));
        assert!(m.quality_tags.iter().any(|t| t == "1080p"));
        assert_eq!(m.container.as_deref(), Some("mkv"));
        assert_eq!(m.group.as_deref(), Some("GROUP"));
        assert_eq!(m.series.as_deref(), Some("Show Name"));
    }

    #[test]
    fn parses_anime_bracket_form() {
        let m = parse_title("[Group] Show 03 (1080p) [HEVC].mkv");
        assert_eq!(m.group.as_deref(), Some("Group"));
        assert_eq!(m.episode, Some(3));
        assert_eq!(m.absolute_episode, Some(3));
        assert!(m.quality_tags.iter().any(|t| t == "1080p"));
        assert_eq!(m.codec.as_deref(), Some("x265"));
        assert_eq!(m.series.as_deref(), Some("Show"));
    }

    #[test]
    fn parses_movie_with_year_and_hdr() {
        let m = parse_title("Show.Name.2023.2160p.HDR.WEB-DL.AV1-RLSGRP");
        assert_eq!(m.year, Some(2023));
        assert_eq!(m.codec.as_deref(), Some("av1"));
        assert!(m.quality_tags.iter().any(|t| t == "2160p"));
        assert!(m.quality_tags.iter().any(|t| t == "HDR"));
        assert_eq!(m.group.as_deref(), Some("RLSGRP"));
        assert_eq!(m.series.as_deref(), Some("Show Name"));
    }

    #[test]
    fn parses_anime_dash_form() {
        let m = parse_title("[SubsPlease] Anime - 12 [1080p].mkv");
        assert_eq!(m.episode, Some(12));
        assert_eq!(m.absolute_episode, Some(12));
        assert_eq!(m.group.as_deref(), Some("SubsPlease"));
        assert_eq!(m.series.as_deref(), Some("Anime"));
    }

    #[test]
    fn parses_x_form_season() {
        let m = parse_title("Series.Title.1x02.HDTV.x264-AAA");
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(2));
        assert_eq!(m.source.as_deref(), Some("HDTV"));
    }

    #[test]
    fn parses_seeders_bracket() {
        let m = parse_title("Some.Release.1080p [123S/45L]");
        assert_eq!(m.seeders, Some(123));
    }

    #[test]
    fn normalize_collapses_punctuation() {
        assert_eq!(normalize_series("Show.Name!  - 2023"), "show name 2023");
    }
}
