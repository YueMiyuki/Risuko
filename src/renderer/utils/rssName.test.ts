import assert from "node:assert/strict";
import { test } from "node:test";
import { rssOutputName } from "./rssName.ts";

test("an ASCII pair keeps the previous result", () => {
	assert.equal(
		rssOutputName("My Feed", "Episode 1: Pilot", ".mkv"),
		"My_Feed_Episode_1_Pilot.mkv",
	);
});

test("CJK titles keep their characters", () => {
	assert.equal(
		rssOutputName("某某字幕組", "第一話 再會", ".mkv"),
		"某某字幕組_第一話_再會.mkv",
	);
});

test("accented letters are kept", () => {
	assert.equal(
		rssOutputName("Café", "Café Société", ".mp4"),
		"Café_Café_Société.mp4",
	);
});

test("path separators and reserved characters are removed", () => {
	assert.equal(
		rssOutputName("a/b\\c", 'd<e>f:g"h|i?j*k', ".mp4"),
		"abc_defghijk.mp4",
	);
});

test("control characters are removed and whitespace becomes an underscore", () => {
	assert.equal(rssOutputName("F", "x\0y\nz\u007f\u0085w", ""), "F_xy_zw");
});

test("an item title of 100 emoji is cut to 80 code points without a lone surrogate", () => {
	const name = rssOutputName("F", "😀".repeat(100), ".mkv");
	assert.equal(name, `F_${"😀".repeat(80)}.mkv`);
	assert.equal(
		(name as string & { isWellFormed(): boolean }).isWellFormed(),
		true,
	);
	assert.equal(Array.from(name.slice(2, -4)).length, 80);
});

test("different CJK items of one feed get different names", () => {
	const first = rssOutputName("字幕組", "第一話", ".mkv");
	const second = rssOutputName("字幕組", "第二話", ".mkv");
	assert.notEqual(first, second);
});
