import assert from "node:assert/strict";
import { test } from "node:test";
import {
	changedProfileIds,
	conflictKeys,
	dirtyProfileIds,
	isAuthFailure,
	mergeProfiles,
	mergeSyncConflict,
	nextPushDelayMs,
	planCategorySync,
	profileDirtyKey,
	pushRetryDelayMs,
	remainingDirtyKeys,
	sameSyncData,
	trackDirtyKey,
} from "./syncConflict.ts";

test("only the keys changed on this device override the newer remote copy", () => {
	const merged = mergeSyncConflict(
		{ dir: "/mine", split: 8, "max-concurrent": 3 },
		{ dir: "/theirs", split: 16, "new-key": true },
		["split"],
	);
	assert.deepEqual(merged, { dir: "/theirs", split: 8, "new-key": true });
});

test("without a record of local changes every local key wins and remote-only keys stay", () => {
	const merged = mergeSyncConflict(
		{ dir: "/mine" },
		{ dir: "/theirs", x: 1 },
		null,
	);
	assert.deepEqual(merged, { dir: "/mine", x: 1 });
});

test("a changed key that no longer exists locally keeps the remote value", () => {
	assert.deepEqual(mergeSyncConflict({}, { dir: "/theirs" }, ["dir"]), {
		dir: "/theirs",
	});
});

test("changed keys are only recorded once a category has been synced", () => {
	const dirty: Record<string, string[]> = { basic: [] };
	assert.equal(trackDirtyKey(dirty, "basic", "dir"), true);
	assert.equal(trackDirtyKey(dirty, "basic", "dir"), false);
	assert.equal(trackDirtyKey(dirty, "never-synced", "dir"), false);
	assert.deepEqual(dirty, { basic: ["dir"] });
});

test("both sides changed is merged instead of one side overwriting the other", () => {
	const local = { hasData: true, timestamp: 200, rev: 4 };
	const remote = { hasData: true, timestamp: 150, rev: 5 };
	assert.equal(planCategorySync(local, remote, ["dir"]), "merge");
	assert.equal(planCategorySync(local, remote, []), "pull");
	assert.equal(planCategorySync(local, { ...remote, rev: 4 }, ["dir"]), "push");
	assert.equal(planCategorySync(local, { ...remote, rev: 4 }, []), "skip");
});

test("without recorded changes the newer timestamp decides and a changed remote is merged", () => {
	const remote = { hasData: true, timestamp: 150, rev: 5 };
	assert.equal(
		planCategorySync({ hasData: true, timestamp: 200, rev: 5 }, remote, null),
		"push",
	);
	assert.equal(
		planCategorySync({ hasData: true, timestamp: 200, rev: 0 }, remote, null),
		"merge",
	);
	assert.equal(
		planCategorySync({ hasData: true, timestamp: 100, rev: 0 }, remote, null),
		"pull",
	);
});

test("a server without revisions falls back to timestamps", () => {
	const remote = { hasData: true, timestamp: 150 };
	assert.equal(
		planCategorySync({ hasData: true, timestamp: 200 }, remote, null),
		"push",
	);
	assert.equal(
		planCategorySync({ hasData: true, timestamp: 100 }, remote, null),
		"pull",
	);
	assert.equal(
		planCategorySync({ hasData: true, timestamp: 150 }, remote, null),
		"skip",
	);
});

test("missing sides push, pull or skip", () => {
	const none = { hasData: false };
	const some = { hasData: true, timestamp: 100, rev: 1 };
	assert.equal(planCategorySync(none, none, null), "skip");
	assert.equal(planCategorySync(none, some, null), "pull");
	assert.equal(planCategorySync(some, none, null), "push");
	assert.equal(planCategorySync({ hasData: true }, some, null), "pull");
});

test("synced data is compared without regard to key order", () => {
	assert.equal(
		sameSyncData(
			{ version: 1, monthly: { a: [1, 2], b: null } },
			{ monthly: { b: null, a: [1, 2] }, version: 1 },
		),
		true,
	);
	assert.equal(sameSyncData({ a: [1, 2] }, { a: [2, 1] }), false);
	assert.equal(sameSyncData({ a: 1 }, { a: 1, b: 2 }), false);
	assert.equal(sameSyncData({ a: 1, b: undefined }, { a: 1 }), true);
});

test("a new profile and an edited profile are reported as changed", () => {
	const previous = [{ id: "a", host: "news.a", updatedAt: 1 }];
	const next = [
		{ id: "a", host: "news.edited", updatedAt: 2 },
		{ id: "b", host: "news.b", updatedAt: 2 },
	];
	assert.deepEqual(changedProfileIds(previous, next), ["a", "b"]);
});

test("an unchanged profile is not reported and profiles without an id are ignored", () => {
	const previous = [{ id: "a", host: "news.a", enabled: true }];
	const next = [
		{ enabled: true, host: "news.a", id: "a" },
		{ host: "no-id" },
		{ id: 7, host: "numeric-id" },
		"not-a-profile",
	];
	assert.deepEqual(changedProfileIds(previous, next), []);
	assert.deepEqual(changedProfileIds(undefined, [{ id: "a" }]), ["a"]);
	assert.deepEqual(changedProfileIds(previous, null), []);
});

test("a deleted profile kept as a tombstone is reported as changed", () => {
	const previous = [{ id: "a", host: "news.a", updatedAt: 1 }];
	const next = [{ id: "a", host: "news.a", updatedAt: 1, deletedAt: 5 }];
	assert.deepEqual(changedProfileIds(previous, next), ["a"]);
});

test("edited profile ids are read from the dirty keys", () => {
	assert.equal(dirtyProfileIds(null), null);
	assert.deepEqual(
		dirtyProfileIds(["usenet-profiles", "usenet-profiles#a"]),
		new Set(["a"]),
	);
	assert.deepEqual(
		dirtyProfileIds(["usenet-profiles", profileDirtyKey("a"), "x"]),
		new Set(["a"]),
	);
	assert.deepEqual(dirtyProfileIds([profileDirtyKey("b#c")]), new Set(["b#c"]));
	assert.deepEqual(dirtyProfileIds(["x"]), new Set());
	assert.deepEqual(dirtyProfileIds([]), new Set());
});

test("a profile list edited by an older version falls back to the clock rule", () => {
	assert.equal(dirtyProfileIds(["usenet-profiles"]), null);
	assert.equal(
		dirtyProfileIds(["usenet-cleanup-mode", "usenet-profiles"]),
		null,
	);
});

test("a profile on one side only is kept, local order first", () => {
	const merged = mergeProfiles(
		[
			{ id: "b", host: "local.b" },
			{ id: "a", host: "local.a" },
		],
		[
			{ id: "c", host: "remote.c" },
			{ id: "a", host: "remote.a" },
			{ id: "d", host: "remote.d" },
		],
		new Set(),
	);
	assert.deepEqual(merged, [
		{ id: "b", host: "local.b" },
		{ id: "a", host: "remote.a" },
		{ id: "c", host: "remote.c" },
		{ id: "d", host: "remote.d" },
	]);
});

test("before the first sync the later updatedAt wins and the local copy wins a tie", () => {
	const merged = mergeProfiles(
		[
			{ id: "a", host: "local.a", updatedAt: 5 },
			{ id: "b", host: "local.b", updatedAt: 9 },
			{ id: "c", host: "local.c", updatedAt: 3 },
			{ id: "d", host: "local.d" },
		],
		[
			{ id: "a", host: "remote.a", updatedAt: 6 },
			{ id: "b", host: "remote.b", updatedAt: 1 },
			{ id: "c", host: "remote.c", updatedAt: 3 },
			{ id: "d", host: "remote.d", updatedAt: "late" },
		],
		null,
	);
	assert.deepEqual(merged, [
		{ id: "a", host: "remote.a", updatedAt: 6 },
		{ id: "b", host: "local.b", updatedAt: 9 },
		{ id: "c", host: "local.c", updatedAt: 3 },
		{ id: "d", host: "local.d" },
	]);
});

test("a profile edited on this device keeps the local copy whatever the clocks say", () => {
	const merged = mergeProfiles(
		[{ id: "a", host: "local.a", updatedAt: 1 }],
		[{ id: "a", host: "remote.a", updatedAt: 9_999_999_999_999 }],
		new Set(["a"]),
	);
	assert.deepEqual(merged, [{ id: "a", host: "local.a", updatedAt: 1 }]);
});

test("a profile not edited on this device takes the remote copy whatever the clocks say", () => {
	const merged = mergeProfiles(
		[{ id: "a", host: "local.a", updatedAt: 9_999_999_999_999 }],
		[{ id: "a", host: "remote.a", updatedAt: 1 }],
		new Set(["b"]),
	);
	assert.deepEqual(merged, [{ id: "a", host: "remote.a", updatedAt: 1 }]);
});

test("profile entries that are not objects or have no string id are dropped", () => {
	const merged = mergeProfiles(
		[null, "x", [{ id: "arr" }], { host: "no-id" }, { id: 3 }, { id: "a" }],
		"not-a-list",
		null,
	);
	assert.deepEqual(merged, [{ id: "a" }]);
	assert.deepEqual(mergeProfiles(undefined, [{ id: "r" }, 5], new Set()), [
		{ id: "r" },
	]);
});

test("conflicts outside usenet override the remote copy with the changed keys", () => {
	assert.deepEqual(conflictKeys("basic", { dir: "/a", split: 8 }, ["split"]), [
		"split",
	]);
	assert.deepEqual(conflictKeys("basic", { dir: "/a", split: 8 }, null), [
		"dir",
		"split",
	]);
});

test("usenet conflicts leave profiles to the per-profile merge", () => {
	const local = {
		"usenet-profiles": [{ id: "a" }],
		"usenet-cleanup-mode": "keep",
	};
	assert.deepEqual(
		conflictKeys("usenet", local, [
			"usenet-profiles",
			profileDirtyKey("a"),
			"usenet-cleanup-mode",
		]),
		["usenet-cleanup-mode"],
	);
	assert.deepEqual(conflictKeys("usenet", local, null), [
		"usenet-cleanup-mode",
	]);
});

test("an upload clears only the keys it pushed", () => {
	assert.deepEqual(
		remainingDirtyKeys(["dir", "split", profileDirtyKey("a")], {
			pushedKeys: ["dir", profileDirtyKey("a")],
		}),
		["split"],
	);
	assert.deepEqual(
		remainingDirtyKeys(["dir"], { pushedKeys: ["dir"], pulledKeys: [] }),
		[],
	);
});

test("a download clears only the keys it replaced", () => {
	assert.deepEqual(
		remainingDirtyKeys(["dir", "split", "max-concurrent"], {
			pulledKeys: ["dir", "max-concurrent", "other"],
		}),
		["split"],
	);
});

test("a download keeps edited profiles and the profile list key queued", () => {
	assert.deepEqual(
		remainingDirtyKeys(
			["usenet-profiles", profileDirtyKey("a"), "usenet-cleanup-mode"],
			{ pulledKeys: ["usenet-profiles", "usenet-cleanup-mode"] },
		),
		["usenet-profiles", profileDirtyKey("a")],
	);
	assert.deepEqual(
		remainingDirtyKeys(["usenet-profiles", "usenet-cleanup-mode"], {
			pulledKeys: ["usenet-profiles", "usenet-cleanup-mode"],
		}),
		[],
	);
});

test("a sync step without a key list empties the dirty list", () => {
	assert.deepEqual(remainingDirtyKeys(["dir", "split"], {}), []);
	assert.deepEqual(
		remainingDirtyKeys(["dir"], { pushedKeys: null, pulledKeys: null }),
		[],
	);
	assert.deepEqual(remainingDirtyKeys([], { pushedKeys: null }), []);
});

test("failed uploads are retried after a doubling delay capped at ten minutes", () => {
	assert.equal(pushRetryDelayMs(1), 30_000);
	assert.equal(pushRetryDelayMs(2), 60_000);
	assert.equal(pushRetryDelayMs(6), 600_000);
	assert.equal(pushRetryDelayMs(50), 600_000);
	assert.equal(pushRetryDelayMs(0), 30_000);
});

test("only a rejected sign-in counts as an auth failure", () => {
	assert.equal(isAuthFailure(401), true);
	assert.equal(isAuthFailure(403), true);
	assert.equal(isAuthFailure(400), false);
	assert.equal(isAuthFailure(409), false);
	assert.equal(isAuthFailure(500), false);
	assert.equal(isAuthFailure("401"), false);
	assert.equal(isAuthFailure(undefined), false);
});

test("a failed batch waits the retry delay unless a fresh change is queued", () => {
	assert.equal(nextPushDelayMs(["a"], [], 1), 30_000);
	assert.equal(nextPushDelayMs(["a"], ["a"], 2), 60_000);
	assert.equal(nextPushDelayMs(["a"], ["b"], 3), 500);
	assert.equal(nextPushDelayMs(["a", "b"], ["b", "c"], 6), 500);
});
