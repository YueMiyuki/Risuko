import assert from "node:assert/strict";
import { test } from "node:test";
import {
	bytesToMb,
	cloneRule,
	countMatched,
	defaultEpisodeSelector,
	emptyRule,
	episodeKind,
	hasEmptyEpisodeList,
	hasInvalidSeasons,
	mbToBytes,
	parseEpisodeList,
	parseNumberList,
	parseSeasons,
	sanitizeRule,
	sortRulesByPriority,
	toggleFeedId,
} from "./rssRule.ts";

test("a new rule starts active with the engine defaults and no id", () => {
	const rule = emptyRule();
	assert.equal(rule.id, "");
	assert.equal(rule.name, "");
	assert.equal(rule.is_active, true);
	assert.equal(rule.auto_download, true);
	assert.equal(rule.mode, "any-match");
	assert.deepEqual(rule.feed_ids, []);
	assert.equal(rule.episodes, null);
	assert.equal(rule.stats.match_count, 0);
});

test("cloning detaches nested patterns from the source rule", () => {
	const source = emptyRule();
	source.title_must = [{ value: "a", kind: "contains", case_sensitive: false }];
	const copy = cloneRule(source);
	copy.title_must[0].value = "b";
	assert.equal(source.title_must[0].value, "a");
});

test("number lists keep positive integers separated by commas or spaces", () => {
	assert.deepEqual(parseNumberList("1, 2 3,x,0,-4,5.5"), [1, 2, 3]);
	assert.deepEqual(parseNumberList(""), []);
	assert.equal(parseSeasons("  "), null);
	assert.deepEqual(parseSeasons("2,4"), [2, 4]);
});

test("episode lists reject the whole field when any token is not a positive integer", () => {
	assert.deepEqual(parseEpisodeList("1, 5 12"), [1, 5, 12]);
	assert.deepEqual(parseEpisodeList(" 3,  4, "), [3, 4]);
	assert.deepEqual(parseEpisodeList(""), []);
	assert.deepEqual(parseEpisodeList("1-12"), []);
	assert.deepEqual(parseEpisodeList("1, 5-7, 12"), []);
	assert.deepEqual(parseEpisodeList("1, 0"), []);
	assert.deepEqual(parseEpisodeList("2, 5.5"), []);
	assert.deepEqual(parseEpisodeList("9007199254740993"), []);
});

test("seasons reject the whole field when any token is not a positive integer", () => {
	assert.deepEqual(parseSeasons(" 1, 3 "), [1, 3]);
	assert.equal(parseSeasons("1-3"), null);
	assert.equal(parseSeasons("2, x"), null);
	assert.equal(hasInvalidSeasons(""), false);
	assert.equal(hasInvalidSeasons("  "), false);
	assert.equal(hasInvalidSeasons("2, 4"), false);
	assert.equal(hasInvalidSeasons("1-3"), true);
	assert.equal(hasInvalidSeasons("2, x"), true);
	assert.equal(hasInvalidSeasons("0"), true);
	assert.equal(hasInvalidSeasons("1.5"), true);
});

test("only a specific selector with no episodes counts as an empty list", () => {
	assert.equal(hasEmptyEpisodeList(null), false);
	assert.equal(hasEmptyEpisodeList({ type: "all" }), false);
	assert.equal(hasEmptyEpisodeList({ type: "from", start: 1 }), false);
	assert.equal(hasEmptyEpisodeList({ type: "specific", values: [] }), true);
	assert.equal(hasEmptyEpisodeList({ type: "specific", values: [2] }), false);
});

test("size conversion keeps fractional megabytes and maps zero to no limit", () => {
	assert.equal(mbToBytes(0), null);
	assert.equal(mbToBytes(-1), null);
	assert.equal(mbToBytes(2), 2 * 1024 * 1024);
	assert.equal(bytesToMb(null), 0);
	assert.equal(bytesToMb(512 * 1024), 0.5);
	assert.equal(bytesToMb(mbToBytes(1.25)), 1.25);
});

test("feed scope toggles without duplicates", () => {
	assert.deepEqual(toggleFeedId([], "a", true), ["a"]);
	assert.deepEqual(toggleFeedId(["a"], "a", true), ["a"]);
	assert.deepEqual(toggleFeedId(["a", "b"], "a", false), ["b"]);
	assert.deepEqual(toggleFeedId(["b"], "a", false), ["b"]);
});

test("rules sort by descending priority and keep the input untouched", () => {
	const low = { ...emptyRule(), id: "low", priority: 1 };
	const high = { ...emptyRule(), id: "high", priority: 9 };
	const tie = { ...emptyRule(), id: "tie", priority: 1 };
	const input = [low, high, tie];
	assert.deepEqual(
		sortRulesByPriority(input).map((rule) => rule.id),
		["high", "low", "tie"],
	);
	assert.deepEqual(
		input.map((rule) => rule.id),
		["low", "high", "tie"],
	);
});

test("episode selector kinds round trip through their defaults", () => {
	assert.equal(episodeKind(null), "none");
	assert.equal(episodeKind({ type: "all" }), "none");
	assert.equal(episodeKind({ type: "range", start: 1, end: 2 }), "range");
	assert.equal(defaultEpisodeSelector("none"), null);
	for (const kind of ["from", "range", "specific"] as const) {
		assert.equal(episodeKind(defaultEpisodeSelector(kind)), kind);
	}
});

test("sanitizing trims text, drops blank patterns and normalizes numbers", () => {
	const rule = emptyRule();
	rule.name = "  Show  ";
	rule.priority = 3.9;
	rule.cooldown_secs = -5;
	rule.min_seeders = 0;
	rule.series_filter = "   ";
	rule.download_dir = " /tmp/x ";
	rule.filename_template = "";
	rule.title_must = [
		{ value: "1080p", kind: "contains", case_sensitive: false },
		{ value: "  ", kind: "regex", case_sensitive: true },
	];
	rule.title_must_not = [{ value: "", kind: "glob", case_sensitive: false }];
	const clean = sanitizeRule(rule);
	assert.equal(clean.name, "Show");
	assert.equal(clean.priority, 3);
	assert.equal(clean.cooldown_secs, 0);
	assert.equal(clean.min_seeders, null);
	assert.equal(clean.series_filter, null);
	assert.equal(clean.download_dir, "/tmp/x");
	assert.equal(clean.filename_template, null);
	assert.deepEqual(
		clean.title_must.map((pattern) => pattern.value),
		["1080p"],
	);
	assert.deepEqual(clean.title_must_not, []);
	assert.equal(rule.name, "  Show  ");
});

test("sanitizing repairs episode selectors the engine would reject", () => {
	const base = emptyRule();
	assert.equal(
		sanitizeRule({ ...base, episodes: { type: "all" } }).episodes,
		null,
	);
	assert.deepEqual(
		sanitizeRule({ ...base, episodes: { type: "from", start: 0 } }).episodes,
		{ type: "from", start: 1 },
	);
	assert.deepEqual(
		sanitizeRule({ ...base, episodes: { type: "range", start: 5, end: 2 } })
			.episodes,
		{ type: "range", start: 5, end: 5 },
	);
	assert.deepEqual(
		sanitizeRule({ ...base, episodes: { type: "specific", values: [] } })
			.episodes,
		{ type: "specific", values: [] },
	);
	assert.deepEqual(
		sanitizeRule({ ...base, episodes: { type: "specific", values: [0, -1] } })
			.episodes,
		{ type: "specific", values: [] },
	);
	assert.deepEqual(
		sanitizeRule({ ...base, episodes: { type: "specific", values: [3, 0, 7] } })
			.episodes,
		{ type: "specific", values: [3, 7] },
	);
});

test("sanitizing keeps seasons inside the engine's integer range", () => {
	const base = emptyRule();
	assert.equal(sanitizeRule({ ...base, seasons: null }).seasons, null);
	assert.equal(sanitizeRule({ ...base, seasons: [] }).seasons, null);
	assert.deepEqual(
		sanitizeRule({ ...base, seasons: [2, 0, -1, 4] }).seasons,
		[2, 4],
	);
	assert.deepEqual(
		sanitizeRule({
			...base,
			seasons: [1, 9_007_199_254_740_991, 4_294_967_296],
		}).seasons,
		[1, 2147483647, 2147483647],
	);
	assert.deepEqual(
		sanitizeRule({
			...base,
			seasons: parseSeasons("3, 99999999999999"),
		}).seasons,
		[3, 2147483647],
	);
});

test("dry-run results count matched samples", () => {
	const row = (matched: boolean) => ({
		item_id: "i",
		feed_id: "f",
		title: "t",
		matched,
		score: 0,
		reason: "",
	});
	assert.equal(countMatched([]), 0);
	assert.equal(countMatched([row(true), row(false), row(true)]), 2);
});
