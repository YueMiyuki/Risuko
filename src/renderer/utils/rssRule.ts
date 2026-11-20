import type {
	DryRunMatch,
	EpisodeSelector,
	Pattern,
	RssRule,
} from "@shared/types/rss";

const BYTES_PER_MB = 1024 * 1024;
const INT32_MAX = 2147483647;
const INT32_MIN = -2147483648;

export type EpisodeKind = "none" | "from" | "range" | "specific";

export function emptyRule(): RssRule {
	return {
		id: "",
		name: "",
		is_active: true,
		auto_download: true,
		feed_ids: [],
		priority: 0,
		mode: "any-match",
		title_must: [],
		title_must_not: [],
		min_size_bytes: null,
		max_size_bytes: null,
		min_seeders: null,
		series_filter: null,
		seasons: null,
		episodes: null,
		quality_preferences: [],
		required_qualities: [],
		forbidden_qualities: [],
		upgrade_existing: false,
		download_dir: null,
		filename_template: null,
		schedule: null,
		cooldown_secs: 0,
		stats: { last_matched_at: null, match_count: 0, download_count: 0 },
	};
}

export function cloneRule(rule: RssRule): RssRule {
	return JSON.parse(JSON.stringify(rule)) as RssRule;
}

export function parseNumberList(text: string): number[] {
	return text
		.split(/[,\s]+/)
		.filter((token) => /^\d+$/.test(token))
		.map(Number)
		.filter((value) => Number.isSafeInteger(value) && value > 0);
}

export function parseEpisodeList(text: string): number[] {
	const tokens = text.split(/[,\s]+/).filter((token) => token !== "");
	const values = parseNumberList(text);
	return values.length === tokens.length ? values : [];
}

export function hasEmptyEpisodeList(selector: EpisodeSelector | null): boolean {
	return selector?.type === "specific" && selector.values.length === 0;
}

export function parseSeasons(text: string): number[] | null {
	const seasons = parseNumberList(text);
	return seasons.length > 0 ? seasons : null;
}

export function bytesToMb(bytes: number | null): number {
	return bytes ? Math.round((bytes / BYTES_PER_MB) * 100) / 100 : 0;
}

export function mbToBytes(mb: number): number | null {
	return mb > 0 ? Math.round(mb * BYTES_PER_MB) : null;
}

export function toggleFeedId(
	feedIds: string[],
	feedId: string,
	selected: boolean,
): string[] {
	if (selected) {
		return feedIds.includes(feedId) ? feedIds : [...feedIds, feedId];
	}
	return feedIds.filter((id) => id !== feedId);
}

export function sortRulesByPriority(rules: RssRule[]): RssRule[] {
	return [...rules].sort((a, b) => b.priority - a.priority);
}

export function episodeKind(selector: EpisodeSelector | null): EpisodeKind {
	if (!selector || selector.type === "all") {
		return "none";
	}
	return selector.type;
}

export function defaultEpisodeSelector(
	kind: EpisodeKind,
): EpisodeSelector | null {
	switch (kind) {
		case "from":
			return { type: "from", start: 1 };
		case "range":
			return { type: "range", start: 1, end: 12 };
		case "specific":
			return { type: "specific", values: [] };
		default:
			return null;
	}
}

export function countMatched(results: DryRunMatch[]): number {
	return results.filter((result) => result.matched).length;
}

function toInt(value: number, min: number, max: number): number {
	const parsed = Number.isFinite(value) ? Math.trunc(value) : 0;
	return Math.min(max, Math.max(min, parsed));
}

function cleanText(value: string | null): string | null {
	const trimmed = value?.trim() ?? "";
	return trimmed === "" ? null : trimmed;
}

function cleanPatterns(patterns: Pattern[]): Pattern[] {
	return patterns.filter((pattern) => pattern.value.trim() !== "");
}

function cleanSeasons(seasons: number[] | null): number[] | null {
	if (!seasons) {
		return null;
	}
	const values = seasons
		.filter((value) => Number.isSafeInteger(value) && value > 0)
		.map((value) => toInt(value, 1, INT32_MAX));
	return values.length > 0 ? values : null;
}

function cleanEpisodes(
	selector: EpisodeSelector | null,
): EpisodeSelector | null {
	if (!selector) {
		return null;
	}
	switch (selector.type) {
		case "from":
			return { type: "from", start: toInt(selector.start, 1, INT32_MAX) };
		case "range": {
			const start = toInt(selector.start, 1, INT32_MAX);
			return {
				type: "range",
				start,
				end: toInt(selector.end, start, INT32_MAX),
			};
		}
		case "specific": {
			const values = selector.values
				.filter((value) => Number.isSafeInteger(value) && value > 0)
				.map((value) => toInt(value, 1, INT32_MAX));
			return { type: "specific", values };
		}
		default:
			return null;
	}
}

export function sanitizeRule(rule: RssRule): RssRule {
	return {
		...rule,
		name: rule.name.trim(),
		priority: toInt(rule.priority, INT32_MIN, INT32_MAX),
		cooldown_secs: toInt(rule.cooldown_secs, 0, Number.MAX_SAFE_INTEGER),
		title_must: cleanPatterns(rule.title_must),
		title_must_not: cleanPatterns(rule.title_must_not),
		min_seeders:
			rule.min_seeders && rule.min_seeders > 0
				? toInt(rule.min_seeders, 1, INT32_MAX)
				: null,
		series_filter: cleanText(rule.series_filter),
		download_dir: cleanText(rule.download_dir),
		filename_template: cleanText(rule.filename_template),
		seasons: cleanSeasons(rule.seasons),
		episodes: cleanEpisodes(rule.episodes),
	};
}
