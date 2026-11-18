export interface RssFeed {
	id: string;
	url: string;
	title: string;
	site_link: string;
	description: string;
	update_interval_secs: number;
	last_fetched_at: number | null;
	created_at: number;
	is_active: boolean;
	error_count: number;
}

interface ParsedMeta {
	series?: string | null;
	year?: number | null;
	season?: number | null;
	episode?: number | null;
	absolute_episode?: number | null;
	quality_tags: string[];
	group?: string | null;
	source?: string | null;
	codec?: string | null;
	container?: string | null;
	language?: string | null;
	seeders?: number | null;
}

export interface RssItem {
	id: string;
	feed_id: string;
	title: string;
	link: string;
	pub_date: number | null;
	description: string;
	content: string;
	enclosure_url: string | null;
	enclosure_type: string | null;
	enclosure_length: number | null;
	is_read: boolean;
	is_downloaded: boolean;
	download_path?: string;
	parsed_meta?: ParsedMeta | null;
	matched_rule_id?: string | null;
	media_urls?: string[];
}

export type PatternKind = "contains" | "glob" | "regex";

export interface Pattern {
	value: string;
	kind: PatternKind;
	case_sensitive: boolean;
}

export type EpisodeSelector =
	| { type: "all" }
	| { type: "from"; start: number }
	| { type: "range"; start: number; end: number }
	| { type: "specific"; values: number[] };

type Weekday = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";

interface Schedule {
	days: Weekday[];
	start_hour: number;
	end_hour: number;
	tz_offset_min: number;
}

type RuleMode = "any-match" | "best-match";

interface RuleStats {
	last_matched_at: number | null;
	match_count: number;
	download_count: number;
}

export interface RssRule {
	id: string;
	name: string;
	is_active: boolean;
	auto_download: boolean;
	feed_ids: string[];
	priority: number;
	mode: RuleMode;
	title_must: Pattern[];
	title_must_not: Pattern[];
	min_size_bytes: number | null;
	max_size_bytes: number | null;
	min_seeders: number | null;
	series_filter: string | null;
	seasons: number[] | null;
	episodes: EpisodeSelector | null;
	quality_preferences: string[];
	required_qualities: string[];
	forbidden_qualities: string[];
	upgrade_existing: boolean;
	download_dir: string | null;
	filename_template: string | null;
	schedule: Schedule | null;
	cooldown_secs: number;
	stats: RuleStats;
}

export interface DryRunMatch {
	item_id: string;
	feed_id: string;
	title: string;
	matched: boolean;
	score: number;
	reason: string;
}
