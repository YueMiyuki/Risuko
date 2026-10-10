import assert from "node:assert/strict";
import { test } from "node:test";
import { downsampleSeries } from "./downsample.ts";

test("short series pass through untouched", () => {
	const items = [1, 2, 3];
	assert.equal(
		downsampleSeries(items, 10, (v) => v),
		items,
	);
});

test("long series are bounded, ordered and keep peaks", () => {
	const items = Array.from({ length: 5000 }, (_, i) => (i === 1234 ? 1e9 : i));
	const out = downsampleSeries(items, 1000, (v) => v);
	assert.equal(out.length, 1000);
	assert.ok(out.includes(1e9));
	const positions = out.map((v) => items.indexOf(v));
	assert.deepEqual(
		positions,
		[...positions].sort((a, b) => a - b),
	);
});
