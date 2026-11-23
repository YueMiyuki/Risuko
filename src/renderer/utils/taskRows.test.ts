import assert from "node:assert/strict";
import { test } from "node:test";
import {
	canRefreshLight,
	fillRowFiles,
	LIGHT_REFRESH_FULL_EVERY,
	mergeRowFiles,
	reuseUnchangedRows,
	TASK_LIST_KEYS,
	TASK_LIST_LIGHT_KEYS,
} from "./taskRows.ts";

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

type Row = { gid: string; status: string; files?: unknown[] };

test("light keys are the list keys without files", () => {
	assert.ok(!TASK_LIST_LIGHT_KEYS.includes("files"));
	assert.deepEqual(
		TASK_LIST_LIGHT_KEYS,
		TASK_LIST_KEYS.filter((key) => key !== "files"),
	);
});

test("rows already held take their files from the previous list", () => {
	const filesA = [{ path: "a" }];
	const filesB = [{ path: "b" }];
	const previous: Row[] = [
		{ gid: "a", status: "paused", files: filesA },
		{ gid: "b", status: "complete", files: filesB },
	];
	const { rows, missing } = mergeRowFiles(previous, [
		{ gid: "a", status: "paused" },
		{ gid: "b", status: "complete" },
	]);
	assert.deepEqual(missing, []);
	assert.equal(rows[0].files, filesA);
	assert.equal(rows[1].files, filesB);
});

test("a new gid is missing and comes back without files", () => {
	const { rows, missing } = mergeRowFiles<Row>(
		[{ gid: "a", status: "paused", files: [] }],
		[
			{ gid: "a", status: "paused" },
			{ gid: "n", status: "paused" },
		],
	);
	assert.deepEqual(missing, ["n"]);
	assert.ok(!("files" in rows[1]));
});

test("a status change makes the row missing", () => {
	const { rows, missing } = mergeRowFiles<Row>(
		[{ gid: "a", status: "waiting", files: [{ path: "a" }] }],
		[{ gid: "a", status: "complete" }],
	);
	assert.deepEqual(missing, ["a"]);
	assert.equal(rows[0].files, undefined);
});

test("a previous row without a files array makes the row missing", () => {
	const { missing } = mergeRowFiles<Row>(
		[{ gid: "a", status: "paused" }],
		[{ gid: "a", status: "paused" }],
	);
	assert.deepEqual(missing, ["a"]);
});

test("merged rows keep the order of the light rows", () => {
	const previous: Row[] = [
		{ gid: "a", status: "paused", files: [] },
		{ gid: "b", status: "paused", files: [] },
		{ gid: "c", status: "paused", files: [] },
	];
	const { rows, missing } = mergeRowFiles(previous, [
		{ gid: "c", status: "paused" },
		{ gid: "n", status: "paused" },
		{ gid: "a", status: "paused" },
	]);
	assert.deepEqual(
		rows.map((row) => row.gid),
		["c", "n", "a"],
	);
	assert.deepEqual(missing, ["n"]);
});

test("a row the predicate matches is missing even when its status is unchanged", () => {
	const filesA = [{ path: "a" }];
	const filesB = [{ path: "b" }];
	const { rows, missing } = mergeRowFiles<Row>(
		[
			{ gid: "a", status: "active", files: filesA },
			{ gid: "b", status: "complete", files: filesB },
		],
		[
			{ gid: "a", status: "active" },
			{ gid: "b", status: "complete" },
		],
		(row) => row.status === "active",
	);
	assert.deepEqual(missing, ["a"]);
	assert.equal(rows[0].files, undefined);
	assert.equal(rows[1].files, filesB);
});

test("without a predicate an active row with an unchanged status keeps its files", () => {
	const filesA = [{ path: "a" }];
	const { rows, missing } = mergeRowFiles<Row>(
		[{ gid: "a", status: "active", files: filesA }],
		[{ gid: "a", status: "active" }],
	);
	assert.deepEqual(missing, []);
	assert.equal(rows[0].files, filesA);
});

test("filled rows take the fetched files array", () => {
	const known = [{ path: "a" }];
	const fetched = [{ path: "n" }];
	const out = fillRowFiles<Row>(
		[
			{ gid: "a", status: "paused", files: known },
			{ gid: "n", status: "paused" },
		],
		[{ gid: "n", files: fetched }],
	);
	assert.ok(out);
	assert.equal(out[0].files, known);
	assert.equal(out[1].files, fetched);
});

test("a row left without a files array fails the fill", () => {
	const rows: Row[] = [
		{ gid: "a", status: "paused" },
		{ gid: "b", status: "paused" },
	];
	assert.equal(fillRowFiles(rows, [{ gid: "a", files: [] }]), null);
	assert.equal(
		fillRowFiles(rows, [{ gid: "a", files: [] }, { gid: "b" }]),
		null,
	);
	assert.equal(fillRowFiles(rows, [null, undefined]), null);
});

test("a light refresh is only for the all, waiting, scheduled, stopped and completed lists", () => {
	for (const type of ["all", "waiting", "scheduled", "stopped", "completed"]) {
		assert.equal(
			canRefreshLight({ type, lastType: type, lightRefreshes: 0 }),
			true,
			type,
		);
	}
	assert.equal(
		canRefreshLight({ type: "active", lastType: "active", lightRefreshes: 0 }),
		false,
	);
});

test("a light refresh needs the last applied fetch to be for the same list", () => {
	assert.equal(
		canRefreshLight({
			type: "waiting",
			lastType: "completed",
			lightRefreshes: 0,
		}),
		false,
	);
	assert.equal(
		canRefreshLight({
			type: "stopped",
			lastType: "completed",
			lightRefreshes: 0,
		}),
		false,
	);
	assert.equal(
		canRefreshLight({ type: "waiting", lastType: null, lightRefreshes: 0 }),
		false,
	);
});

test("a full fetch is due once the light refreshes reach the limit", () => {
	assert.equal(
		canRefreshLight({
			type: "stopped",
			lastType: "stopped",
			lightRefreshes: LIGHT_REFRESH_FULL_EVERY - 1,
		}),
		true,
	);
	assert.equal(
		canRefreshLight({
			type: "stopped",
			lastType: "stopped",
			lightRefreshes: LIGHT_REFRESH_FULL_EVERY,
		}),
		false,
	);
});
