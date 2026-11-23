import assert from "node:assert/strict";
import { test } from "node:test";
import {
	DEFAULT_SYNC_SERVER,
	normalizeSyncServer,
	sameSyncServer,
} from "./syncServer.ts";

test("an empty address falls back to the default server", () => {
	assert.equal(normalizeSyncServer(""), DEFAULT_SYNC_SERVER);
	assert.equal(normalizeSyncServer("   "), DEFAULT_SYNC_SERVER);
	assert.equal(normalizeSyncServer(undefined), DEFAULT_SYNC_SERVER);
	assert.equal(normalizeSyncServer(null), DEFAULT_SYNC_SERVER);
});

test("empty, undefined and the default address are the same server", () => {
	assert.ok(sameSyncServer("", undefined));
	assert.ok(sameSyncServer(undefined, DEFAULT_SYNC_SERVER));
	assert.ok(sameSyncServer("", DEFAULT_SYNC_SERVER));
	assert.ok(sameSyncServer(null, ""));
});

test("a trailing slash and letter case do not make a different server", () => {
	assert.ok(
		sameSyncServer("https://sync.example.test/", "https://sync.example.test"),
	);
	assert.ok(
		sameSyncServer("https://Sync.Example.TEST", "https://sync.example.test"),
	);
	assert.ok(
		sameSyncServer(
			"  https://sync.example.test//  ",
			"https://sync.example.test",
		),
	);
});

test("a different host is a different server", () => {
	assert.ok(
		!sameSyncServer("https://sync.example.test", "https://other.example.test"),
	);
	assert.ok(!sameSyncServer("https://sync.example.test", ""));
	assert.ok(!sameSyncServer("", "https://sync.example.test"));
});

test("http and https on the same host are different servers", () => {
	assert.ok(
		!sameSyncServer("http://sync.example.test", "https://sync.example.test"),
	);
});
