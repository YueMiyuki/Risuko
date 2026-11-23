import assert from "node:assert/strict";
import { test } from "node:test";
import { liveTaskOptions, liveUserOptions } from "./liveTaskOptions.ts";

test("only settings meant for running downloads are kept", () => {
	assert.deepEqual(
		liveTaskOptions({
			dir: "/new/downloads",
			"user-agent": "Changed/1.0",
			header: ["X-Test: 1"],
			split: "32",
			out: "renamed.bin",
			referer: "https://example.test/",
			"max-download-limit": "512K",
			"seed-time": "30",
		}),
		{ "max-download-limit": "512K" },
	);
});

test("options without any live setting give an empty object", () => {
	assert.deepEqual(
		liveTaskOptions({
			dir: "/new/downloads",
			"user-agent": "Changed/1.0",
			"load-cookies": "/cookies.txt",
		}),
		{},
	);
});

test("the keep seeding switch is the only user setting sent to the running engine", () => {
	assert.deepEqual(
		liveUserOptions({
			"keep-seeding": true,
			locale: "en-US",
			"engine-overrides": {},
			"bt-create-subfolder": false,
		}),
		{ "keep-seeding": true },
	);
	assert.deepEqual(liveUserOptions({ locale: "en-US" }), {});
});
