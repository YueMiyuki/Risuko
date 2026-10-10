import assert from "node:assert/strict";
import { test } from "node:test";
import { bucketByTime } from "./downsample.ts";

const minute = (value: number) => value * 60;

test("short series keep one bucket per item", () => {
	const items = [0, 1, 2].map(minute);
	const buckets = bucketByTime(items, 10, 60, (value) => value);
	assert.deepEqual(
		buckets.map((bucket) => bucket.time),
		items,
	);
	assert.deepEqual(
		buckets.map((bucket) => bucket.items),
		items.map((value) => [value]),
	);
});

test("long series are bounded, ordered and keep every item", () => {
	const items = Array.from({ length: 5000 }, (_, i) => minute(i));
	const buckets = bucketByTime(items, 1000, 60, (value) => value);
	assert.equal(buckets.length, 1000);
	assert.deepEqual(
		buckets.flatMap((bucket) => bucket.items),
		items,
	);
	const times = buckets.map((bucket) => bucket.time);
	assert.deepEqual(
		times,
		[...times].sort((a, b) => a - b),
	);
});

test("buckets follow time rather than item position", () => {
	const dense = Array.from({ length: 2000 }, (_, i) => minute(i));
	const sparse = [...dense, minute(100000)];
	const buckets = bucketByTime(sparse, 1000, 60, (value) => value);
	const filled = buckets.filter((bucket) => bucket.items.length > 0);
	assert.equal(filled.length, 21);
	assert.equal(filled[0].items.length, 100);
	assert.deepEqual(filled[20].items, [minute(100000)]);
});

test("dense buckets sit at the midpoint of their samples", () => {
	const items = Array.from({ length: 40 }, (_, i) => minute(i));
	const buckets = bucketByTime(items, 10, 60, (value) => value);
	for (const bucket of buckets) {
		const first = bucket.items[0];
		const last = bucket.items[bucket.items.length - 1];
		assert.equal(bucket.time, (first + last) / 2);
	}
	assert.ok(buckets[0].time > items[0]);
});

test("idle gaps are bracketed by empty buckets", () => {
	const items = [0, 1, 500, 501].map(minute);
	const buckets = bucketByTime(items, 1000, 60, (value) => value);
	assert.deepEqual(
		buckets.map((bucket) => [bucket.time, bucket.items.length]),
		[
			[minute(0), 1],
			[minute(1), 1],
			[minute(2), 0],
			[minute(499), 0],
			[minute(500), 1],
			[minute(501), 1],
		],
	);
});

test("empty input yields no buckets", () => {
	assert.deepEqual(
		bucketByTime([], 10, 60, (value: number) => value),
		[],
	);
});
