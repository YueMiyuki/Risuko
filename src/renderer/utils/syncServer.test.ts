import assert from "node:assert/strict";
import { test } from "node:test";
import {
	DEFAULT_SYNC_SERVER,
	normalizeSyncServer,
	sameSyncServer,
	sendsTokenUnencrypted,
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

test("letter case in the path or query makes a different server", () => {
	assert.ok(
		!sameSyncServer(
			"https://sync.example.test/Team",
			"https://sync.example.test/team",
		),
	);
	assert.ok(
		!sameSyncServer(
			"https://sync.example.test/api?key=A",
			"https://sync.example.test/api?key=a",
		),
	);
	assert.ok(
		sameSyncServer(
			"HTTPS://Sync.Example.TEST/Team/",
			"https://sync.example.test/Team",
		),
	);
	assert.ok(
		sameSyncServer(
			"https://sync.example.test:443",
			"https://sync.example.test",
		),
	);
});

test("an address that is not a URL only equals itself", () => {
	assert.ok(sameSyncServer("not a url", "not a url"));
	assert.ok(!sameSyncServer("not a url", "Not A Url"));
});

test("plain http is flagged unless it stays on this machine", () => {
	assert.ok(sendsTokenUnencrypted("http://sync.example.test"));
	assert.ok(sendsTokenUnencrypted("http://192.168.1.20:8787"));
	assert.ok(!sendsTokenUnencrypted("https://sync.example.test"));
	assert.ok(!sendsTokenUnencrypted(""));
	assert.ok(!sendsTokenUnencrypted("not a url"));
	for (const local of [
		"http://localhost:8787",
		"http://dev.localhost",
		"http://127.0.0.1:8787",
		"http://[::1]:8787",
	]) {
		assert.ok(!sendsTokenUnencrypted(local), local);
	}
});
