import assert from "node:assert/strict";
import { test } from "node:test";
import type { TaskMirror } from "@shared/types/task";
import { buildMirrorRows } from "./taskMirrors.ts";

const mirror = (uri: string, extra: Partial<TaskMirror> = {}): TaskMirror => ({
	uri,
	completedLength: "0",
	downloadSpeed: "0",
	connections: "0",
	status: "idle",
	...extra,
});

test("a single source has no mirror rows", () => {
	assert.deepEqual(buildMirrorRows(undefined), []);
	assert.deepEqual(buildMirrorRows([mirror("https://a.test/f.iso")]), []);
});

test("rows carry each mirror's numbers and are labelled by host", () => {
	const rows = buildMirrorRows([
		mirror("https://a.test/pub/f.iso", {
			completedLength: "1048576",
			downloadSpeed: "2048",
			connections: "5",
			status: "active",
		}),
		mirror("http://b.test:8080/f.iso", { status: "failing" }),
	]);
	assert.deepEqual(
		rows.map(({ label, completed, speed, connections, status }) => ({
			label,
			completed,
			speed,
			connections,
			status,
		})),
		[
			{
				label: "a.test",
				completed: 1048576,
				speed: 2048,
				connections: 5,
				status: "active",
			},
			{
				label: "b.test:8080",
				completed: 0,
				speed: 0,
				connections: 0,
				status: "failing",
			},
		],
	);
});

test("mirrors on one host are told apart by path", () => {
	const rows = buildMirrorRows([
		mirror("https://a.test/one/f.iso"),
		mirror("https://a.test/two/f.iso"),
		mirror("not a url"),
	]);
	assert.deepEqual(
		rows.map((row) => row.label),
		["a.test/one/f.iso", "a.test/two/f.iso", "not a url"],
	);
});

test("mirrors on one host and path are told apart by query", () => {
	const rows = buildMirrorRows([
		mirror("https://a.test/f.iso?sig=1"),
		mirror("https://a.test/f.iso?sig=2"),
		mirror("https://a.test/g.iso?sig=3"),
		mirror("https://b.test/f.iso?sig=4"),
	]);
	assert.deepEqual(
		rows.map((row) => row.label),
		["a.test/f.iso?sig=1", "a.test/f.iso?sig=2", "a.test/g.iso", "b.test"],
	);
});
