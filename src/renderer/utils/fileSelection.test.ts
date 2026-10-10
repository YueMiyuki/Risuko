import assert from "node:assert/strict";
import { test } from "node:test";
import { selectionState, sortedIndexList } from "./fileSelection.ts";

test("selection state comes from counts", () => {
	assert.equal(selectionState(0, new Set()), "none");
	assert.equal(selectionState(5, new Set()), "none");
	assert.equal(selectionState(5, new Set([1, 3])), "partial");
	assert.equal(selectionState(3, new Set([1, 2, 3])), "all");
});

test("index list is numerically sorted", () => {
	assert.equal(sortedIndexList(new Set([10, 2, 33])), "2,10,33");
	assert.equal(sortedIndexList(new Set()), "");
});
