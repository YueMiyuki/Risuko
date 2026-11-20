import assert from "node:assert/strict";
import { test } from "node:test";
import { isMirrorUrl, parseMirrorList } from "./mirrorLinks.ts";

test("one link per line or space-separated, duplicates dropped", () => {
	assert.deepEqual(
		parseMirrorList(
			"https://a.test/f.iso\n  http://b.test/f.iso https://c.test/f.iso\n\nHTTPS://A.TEST/f.iso\n",
		),
		{
			uris: [
				"https://a.test/f.iso",
				"http://b.test/f.iso",
				"https://c.test/f.iso",
			],
			rejected: [],
		},
	);
	assert.deepEqual(parseMirrorList(""), { uris: [], rejected: [] });
});

test("anything that is not a plain http file link is rejected", () => {
	assert.deepEqual(
		parseMirrorList(
			[
				"https://a.test/f.iso",
				"ftp://b.test/f.iso",
				"magnet:?xt=urn:btih:abc",
				"https://c.test/live.m3u8?token=1",
				"https://d.test/post.NZB",
				"https://www.youtube.test/watch?v=1",
				"not-a-link",
			].join("\n"),
			(uri) => uri.includes("youtube"),
		),
		{
			uris: ["https://a.test/f.iso"],
			rejected: [
				"ftp://b.test/f.iso",
				"magnet:?xt=urn:btih:abc",
				"https://c.test/live.m3u8?token=1",
				"https://d.test/post.NZB",
				"https://www.youtube.test/watch?v=1",
				"not-a-link",
			],
		},
	);
});

test("only http and https links count as mirrors", () => {
	assert.equal(isMirrorUrl("https://a.test/f"), true);
	assert.equal(isMirrorUrl(" http://a.test "), true);
	assert.equal(isMirrorUrl("ftp://a.test/f"), false);
	assert.equal(isMirrorUrl("a.test/f"), false);
	assert.equal(isMirrorUrl(""), false);
});
