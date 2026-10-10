import assert from "node:assert/strict";
import { test } from "node:test";
import { reuseUnchangedRows, TASK_LIST_KEYS } from "./taskRows.ts";

test("list keys request files but not detail-only payloads", () => {
	assert.ok(TASK_LIST_KEYS.includes("gid"));
	assert.ok(!TASK_LIST_KEYS.includes("usenet"));
	assert.ok(!TASK_LIST_KEYS.includes("chunkProgress"));
});

test("reuses unchanged rows by gid and swaps changed ones", () => {
	const a = { gid: "a", completedLength: "1", files: [{ path: "x" }] };
	const b = { gid: "b", completedLength: "5", files: [] };
	const nextA = { gid: "a", completedLength: "1", files: [{ path: "x" }] };
	const nextB = { gid: "b", completedLength: "6", files: [] };
	const out = reuseUnchangedRows([a, b], [nextB, nextA]);
	assert.equal(out[0], nextB);
	assert.equal(out[1], a);
});

test("new gids and empty history pass through", () => {
	const row = { gid: "n" };
	assert.deepEqual(reuseUnchangedRows([], [row]), [row]);
	const out = reuseUnchangedRows([{ gid: "o" }], [row]);
	assert.equal(out[0], row);
});
